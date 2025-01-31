# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzcompose system for Docker compositions.

For an overview of what mzcompose is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""

import argparse
import copy
import functools
import importlib
import importlib.abc
import importlib.util
import inspect
import ipaddress
import json
import os
import random
import re
import shlex
import subprocess
import sys
import time
from contextlib import contextmanager
from inspect import getmembers, isfunction
from pathlib import Path
from tempfile import TemporaryFile
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Match,
    Optional,
    Sequence,
    Type,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import pg8000
import pymysql
import sqlparse
import yaml

from materialize import mzbuild, spawn, ui
from materialize.ui import UIError

T = TypeVar("T")
say = ui.speaker("C> ")

_BASHLIKE_ALT_VAR_PATTERN = re.compile(
    r"""\$\{
        (?P<var>[^:}]+):\+
        (?P<alt_var>[^}]+)
        \}""",
    re.VERBOSE,
)

_BASHLIKE_ENV_VAR_PATTERN = re.compile(
    r"""\$\{
        (?P<var>[^:}]+)
        (?P<default>:-[^}]+)?
        \}""",
    re.VERBOSE,
)


DEFAULT_CONFLUENT_PLATFORM_VERSION = "5.5.4"
DEFAULT_DEBEZIUM_VERSION = "1.6"
LINT_DEBEZIUM_VERSIONS = ["1.4", "1.5", "1.6"]

DEFAULT_MZ_VOLUMES = ["mzdata:/share/mzdata", "tmp:/share/tmp"]


class UnknownCompositionError(UIError):
    """The specified composition was unknown."""

    def __init__(self, name: str):
        super().__init__(f"unknown composition {name!r}")


class LintError:
    def __init__(self, file: Path, message: str):
        self.file = file
        self.message = message

    def __str__(self) -> str:
        return f"{os.path.relpath(self.file)}: {self.message}"

    def __lt__(self, other: "LintError") -> bool:
        return (self.file, self.message) < (other.file, other.message)


def lint_composition(path: Path, composition: Any, errors: List[LintError]) -> None:
    if "services" not in composition:
        return

    for (name, service) in composition["services"].items():
        if service.get("mzbuild") == "materialized":
            lint_materialized_service(path, name, service, errors)
        elif "mzbuild" not in service and "image" in service:
            lint_image_name(path, service["image"], errors)

        if isinstance(service.get("environment"), dict):
            errors.append(
                LintError(
                    path, f"environment for service {name} uses dict instead of list"
                )
            )


def lint_image_name(path: Path, spec: str, errors: List[LintError]) -> None:
    match = re.search(r"((?P<repo>[^/]+)/)?(?P<image>[^:]+)(:(?P<tag>.*))?", spec)
    if not match:
        errors.append(LintError(path, f"malformatted image specification: {spec}"))
        return
    (repo, image, tag) = (match.group("repo"), match.group("image"), match.group("tag"))

    if not tag:
        errors.append(LintError(path, f"image {spec} missing tag"))
    elif tag == "latest":
        errors.append(LintError(path, f'image {spec} depends on floating "latest" tag'))

    if repo == "confluentinc" and image.startswith("cp-"):
        # An '$XXX' environment variable may have been used to specify the version
        if "$" not in tag and tag != DEFAULT_CONFLUENT_PLATFORM_VERSION:
            errors.append(
                LintError(
                    path,
                    f"image {spec} depends on wrong version of Confluent Platform "
                    f"(want {DEFAULT_CONFLUENT_PLATFORM_VERSION})",
                )
            )

    if repo == "debezium":
        if "$" not in tag and tag not in LINT_DEBEZIUM_VERSIONS:
            errors.append(
                LintError(
                    path,
                    f"image {spec} depends on wrong version of Debezium "
                    f"(want {LINT_DEBEZIUM_VERSIONS})",
                )
            )

    if not repo and image == "zookeeper":
        errors.append(
            LintError(
                path, f"replace {spec} with official confluentinc/cp-zookeeper image"
            )
        )

    if repo == "wurstmeister" and image == "kafka":
        errors.append(
            LintError(path, f"replace {spec} with official confluentinc/cp-kafka image")
        )


def lint_materialized_service(
    path: Path, name: str, service: Any, errors: List[LintError]
) -> None:
    # command may be a string that is passed to the shell, or a list of
    # arguments.
    command = service.get("command", "")
    if isinstance(command, str):
        command = command.split()  # split on whitespace to extract individual arguments
    if "--disable-telemetry" not in command:
        errors.append(
            LintError(
                path,
                "materialized service command does not include --disable-telemetry",
            )
        )
    env = service.get("environment", [])
    if "MZ_DEV=1" not in env:
        errors.append(
            LintError(
                path,
                f"materialized service '{name}' does not specify MZ_DEV=1 in its environment: {env}",
            )
        )


class Composition:
    """A parsed mzcompose.yml with a loaded mzworkflows.py file."""

    def __init__(
        self, repo: mzbuild.Repository, name: str, preserve_ports: bool = False
    ):
        self.name = name
        self.repo = repo
        self.images: List[mzbuild.Image] = []
        self.python_funcs: Dict[str, Callable[[Composition], None]] = {}

        default_tag = os.getenv(f"MZBUILD_TAG", None)

        if name in self.repo.compositions:
            self.path = self.repo.compositions[name]
        else:
            raise UnknownCompositionError(name)

        # load the mzcompose.yml file, if one exists
        mzcompose_yml = self.path / "mzcompose.yml"
        if mzcompose_yml.exists():
            with open(mzcompose_yml) as f:
                compose = yaml.safe_load(f) or {}
        else:
            compose = {}

        if "version" not in compose:
            compose["version"] = "3.7"

        if "services" not in compose:
            compose["services"] = {}

        # Stash away sub workflows so that we can load them with the correct environment variables
        self.yaml_workflows = compose.pop("mzworkflows", {})

        # Load the mzworkflows.py file, if one exists
        mzworkflows_py = self.path / "mzworkflows.py"
        if mzworkflows_py.exists():
            spec = importlib.util.spec_from_file_location("mzworkflows", mzworkflows_py)
            assert spec
            module = importlib.util.module_from_spec(spec)
            assert isinstance(spec.loader, importlib.abc.Loader)
            spec.loader.exec_module(module)
            for name, fn in getmembers(module, isfunction):
                if name.startswith("workflow_"):
                    # The name of the workflow is the name of the function
                    # with the "workflow_" prefix stripped and any underscores
                    # replaced with dashes.
                    name = name[len("workflow_") :].replace("_", "-")
                    self.python_funcs[name] = fn

            for python_service in getattr(module, "services", []):
                compose["services"][python_service.name] = python_service.config

        # Resolve all services that reference an `mzbuild` image to a specific
        # `image` reference.
        for name, config in compose["services"].items():
            compose["services"][name] = _substitute_env_vars(
                config, {k: v for k, v in os.environ.items()}
            )
            if "mzbuild" in config:
                image_name = config["mzbuild"]

                if image_name not in self.repo.images:
                    raise UIError(f"mzcompose: unknown image {image_name}")

                image = self.repo.images[image_name]
                override_tag = os.getenv(
                    f"MZBUILD_{image.env_var_name()}_TAG", default_tag
                )
                if override_tag is not None:
                    config["image"] = image.docker_name(override_tag)
                    print(
                        f"mzcompose: warning: overriding {image_name} image to tag {override_tag}",
                        file=sys.stderr,
                    )
                    del config["mzbuild"]
                else:
                    self.images.append(image)

                if "propagate_uid_gid" in config:
                    config["user"] = f"{os.getuid()}:{os.getgid()}"
                    del config["propagate_uid_gid"]

            ports = config.setdefault("ports", [])
            for i, port in enumerate(ports):
                if ":" in str(port):
                    raise UIError(
                        "programming error: disallowed host port in service {name!r}"
                    )
                if preserve_ports:
                    # If preserving ports, bind the container port to the same
                    # host port.
                    ports[i] = f"{port}:{port}"

            if self.repo.rd.coverage:
                # Emit coverage information to a file in a directory that is
                # bind-mounted to the "coverage" directory on the host. We
                # inject the configuration to all services for simplicity, but
                # this only have an effect if the service runs instrumented Rust
                # binaries.
                config.setdefault("environment", []).append(
                    f"LLVM_PROFILE_FILE=/coverage/{name}-%m.profraw"
                )
                config.setdefault("volumes", []).append("./coverage:/coverage")

        # Add default volumes
        compose.setdefault("volumes", {}).update(
            {
                "mzdata": None,
                "tmp": None,
                "secrets": None,
            }
        )

        deps = self.repo.resolve_dependencies(self.images)
        for config in compose["services"].values():
            if "mzbuild" in config:
                config["image"] = deps[config["mzbuild"]].spec()
                del config["mzbuild"]

        self.compose = compose

        # Emit the munged configuration to a temporary file so that we can later
        # pass it to Docker Compose.
        self.file = TemporaryFile()
        os.set_inheritable(self.file.fileno(), True)
        self._write_compose()

    def _write_compose(self) -> None:
        self.file.seek(0)
        self.file.truncate()
        yaml.dump(self.compose, self.file, encoding="utf-8")  # type: ignore
        self.file.flush()

    def get_env(self, workflow_name: str, parent_env: Dict[str, str]) -> Dict[str, str]:
        """Return the desired environment for a workflow."""

        if workflow_name in self.yaml_workflows:
            raw_env = self.yaml_workflows[workflow_name].get("env")
        else:
            raw_env = {}

        if not isinstance(raw_env, dict) and raw_env is not None:
            raise UIError(
                f"Workflow {workflow_name} has wrong type for env: "
                f"expected mapping, got {type(raw_env).__name__}: {raw_env}",
            )
        # ensure that integers (e.g. ports) are treated as env vars
        if isinstance(raw_env, dict):
            raw_env = {k: str(v) for k, v in raw_env.items()}

        # Substitute environment variables from the parent environment, allowing for the child
        # environment to inherit variables from the parent
        child_env = _substitute_env_vars(raw_env, parent_env)

        # Merge the child and parent environments, with the child environment having the tie
        # breaker. This allows for the child to decide if it wants to inherit (from the step
        # above) or override (from this step).
        env = dict(**parent_env)
        if child_env:
            env.update(**child_env)
        return env

    def get_workflow(
        self, workflow_name: str, parent_env: Dict[str, str]
    ) -> "Workflow":
        """Return sub-workflow, with env vars substituted using the supplied environment."""
        if not self.yaml_workflows and not self.python_funcs:
            raise KeyError(f"No workflows defined for composition {self.name}")
        if (
            workflow_name not in self.yaml_workflows
            and workflow_name not in self.python_funcs
        ):
            raise KeyError(f"No workflow called {workflow_name} in {self.name}")

        # Build this workflow, performing environment substitution as necessary
        workflow_env = self.get_env(workflow_name, parent_env)

        # Return a PythonWorkflow if an appropriately-named Python function exists
        if workflow_name in self.python_funcs:
            return PythonWorkflow(
                name=workflow_name,
                func=self.python_funcs[workflow_name],
                env=workflow_env,
                composition=self,
            )

        # Otherwise, look for a YAML sub-workflow
        yaml_workflow = _substitute_env_vars(
            self.yaml_workflows[workflow_name], workflow_env
        )
        built_steps = []
        for raw_step in yaml_workflow["steps"]:
            # A step could be reused over several workflows, so operate on a copy
            raw_step = raw_step.copy()

            step_name = raw_step.pop("step")
            step_ty = Steps.named(step_name)
            munged = {k.replace("-", "_"): v for k, v in raw_step.items()}
            try:
                step = step_ty(**munged)
            except TypeError as e:
                a = " ".join([f"{k}={v}" for k, v in munged.items()])
                raise UIError(f"Unable to construct {step_name} with args {a}: {e}")
            built_steps.append(step)

        return Workflow(workflow_name, built_steps, env=workflow_env, composition=self)

    @classmethod
    def lint(cls, repo: mzbuild.Repository, name: str) -> List[LintError]:
        """Checks a composition for common errors."""
        if not name in repo.compositions:
            raise UnknownCompositionError(name)

        errs: List[LintError] = []

        path = repo.compositions[name] / "mzcompose.yml"

        if path.exists():
            with open(path) as f:
                composition = yaml.safe_load(f) or {}

            lint_composition(path, composition, errs)
        return errs

    def run(
        self,
        args: List[str],
        env: Optional[Dict[str, str]] = None,
        capture: bool = False,
        capture_combined: bool = False,
        check: bool = True,
    ) -> "subprocess.CompletedProcess[str]":
        """Invokes docker-compose on the composition.

        Arguments to specify the files in the composition and the project
        directory are added automatically.

        Args:
            args: Additional arguments to pass to docker-compose.
            env: Additional environment variables to set for the child process.
                These are merged with the current environment.
            capture: Whether to capture the child's stdout and stderr, or
                whether to emit directly to the current stdout/stderr streams.
            capture_combined: capture stdout and stderr, and direct all output
                to the stdout property on the returned object
            check: Whether to raise an error if the child process exits with
                a failing exit code.
        """
        self.file.seek(0)
        if env is not None:
            env = dict(os.environ, **env)

        stdout = 1
        stderr = 2
        if capture:
            stdout = stderr = subprocess.PIPE
        if capture_combined:
            stdout = subprocess.PIPE
            stderr = subprocess.STDOUT
        try:
            return subprocess.run(
                [
                    "docker-compose",
                    f"-f/dev/fd/{self.file.fileno()}",
                    "--project-directory",
                    self.path,
                    *args,
                ],
                env=env,
                close_fds=False,
                check=check,
                stdout=stdout,
                stderr=stderr,
                encoding="utf-8",
            )
        except subprocess.CalledProcessError as e:
            raise UIError(f"running docker-compose failed (exit status {e.returncode})")

    def find_host_ports(self, service: str) -> List[str]:
        """Find all ports open on the host for a given service"""
        # Parsing the output of `docker-compose ps` directly is fraught, as the
        # output depends on terminal width (!). Using the `-q` flag is safe,
        # however, and we can pipe the container IDs into `docker inspect`,
        # which supports machine-readable output.
        if service not in self.compose["services"]:
            raise UIError(f"unknown service {service!r}")
        ports = []
        for info in self.inspect_service_containers(service):
            for (name, port_entry) in info["NetworkSettings"]["Ports"].items():
                for p in port_entry or []:
                    # When IPv6 is enabled, Docker will bind each port twice. Consider
                    # only IPv4 address to avoid spurious warnings about duplicate
                    # ports.
                    if p["HostPort"] not in ports and isinstance(
                        ipaddress.ip_address(p["HostIp"]), ipaddress.IPv4Address
                    ):
                        ports.append(p["HostPort"])
        return ports

    def inspect_service_containers(
        self, service: str, include_stopped: bool = False
    ) -> Iterable[Dict[str, Any]]:
        """
        Return the JSON from `docker inspect` for each container in the given compose service

        There is no explicit documentation of the structure of the returned
        fields, but you can see them in the docker core repo:
        https://github.com/moby/moby/blob/91dc595e9648318/api/types/types.go#L345-L379
        """
        cmd = ["ps", "-q"]
        if include_stopped:
            cmd.append("-a")
        containers = self.run(cmd, capture=True).stdout.splitlines()
        if not containers:
            return
        metadata = spawn.capture(["docker", "inspect", "-f", "{{json .}}", *containers])
        for line in metadata.splitlines():
            info = json.loads(line)
            labels = info["Config"].get("Labels")
            if (
                labels is not None
                and labels.get("com.docker.compose.service") == service
                and labels.get("com.docker.compose.project") == self.name
            ):
                yield info

    def service_logs(self, service_name: str, tail: int = 20) -> str:
        proc = self.run(
            [
                "logs",
                "--tail",
                str(tail),
                service_name,
            ],
            check=True,
            capture_combined=True,
        )
        return proc.stdout

    def get_container_id(self, service: str, running: bool = False) -> str:
        """Given a service name, tries to find a unique matching container id
        If running is True, only return running containers.
        """
        try:
            if running:
                cmd = f"docker ps".split()
            else:
                cmd = f"docker ps -a".split()
            list_containers = spawn.capture(cmd, unicode=True)

            pattern = re.compile(f"^(?P<c_id>[^ ]+).*{service}")
            matches = []
            for line in list_containers.splitlines():
                m = pattern.search(line)
                if m:
                    matches.append(m.group("c_id"))
            if len(matches) != 1:
                raise UIError(
                    f"failed to get a unique container id for service {service}, found: {matches}"
                )

            return matches[0]
        except subprocess.CalledProcessError as e:
            raise UIError(f"failed to get container id for {service}: {e}")

    def docker_inspect(self, format: str, container_id: str) -> str:
        try:
            cmd = f"docker inspect -f '{format}' {container_id}".split()
            output = spawn.capture(cmd, unicode=True, stderr_too=True).splitlines()[0]
        except subprocess.CalledProcessError as e:
            ui.log_in_automation(
                "docker inspect ({}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                    container_id, ui.shell_quote(cmd), e, e.stdout, e.stderr
                )
            )
            raise UIError(f"failed to inspect Docker container: {e}")
        else:
            return output

    def docker_container_is_running(self, container_id: str) -> bool:
        return self.docker_inspect("{{.State.Running}}", container_id) == "'true'"


def _substitute_env_vars(val: T, env: Dict[str, str]) -> T:
    """Substitute docker-compose style env vars in a dict

    This is necessary for mzconduct, since its parameters are not handled by docker-compose
    """
    if isinstance(val, str):
        val = cast(
            T, _BASHLIKE_ENV_VAR_PATTERN.sub(functools.partial(_subst, env), val)
        )
        val = cast(
            T,
            _BASHLIKE_ALT_VAR_PATTERN.sub(
                functools.partial(_alt_subst, env), cast(str, val)
            ),
        )
    elif isinstance(val, dict):
        for k, v in val.items():
            val[k] = _substitute_env_vars(v, env)
    elif isinstance(val, list):
        for i, v in enumerate(val):
            val[i] = _substitute_env_vars(v, env)
    return val


def _subst(env: Dict[str, str], match: Match) -> str:
    var = match.group("var")
    if var is None:
        raise UIError(f"Unable to parse environment variable {match.group(0)}")
    # https://github.com/python/typeshed/issues/3902
    default = cast(Optional[str], match.group("default"))

    env_val = env.get(var)
    if env_val is None and default is None:
        say(f"WARNING: unknown env var {var!r}")
        return cast(str, match.group(0))
    elif env_val is None and default is not None:
        # strip the leading ":-"
        env_val = default[2:]
    assert env_val is not None, "should be replaced correctly"
    return env_val


def _alt_subst(env: Dict[str, str], match: Match) -> str:
    var = match.group("var")
    if var is None:
        raise UIError(f"Unable to parse environment variable {match.group(0)}")
    # https://github.com/python/typeshed/issues/3902
    altvar = cast(Optional[str], match.group("alt_var"))
    assert altvar is not None, "alt var not captured by regex"

    env_val = env.get(var)
    if env_val is None:
        return ""
    return altvar


class Workflows:
    """All Known Workflows inside a Composition"""

    def __init__(self, workflows: Dict[str, "Workflow"]) -> None:
        self._inner = workflows

    def __getitem__(self, workflow: str) -> "Workflow":
        return self._inner[workflow]

    def all_workflows(self) -> Collection["Workflow"]:
        return self._inner.values()

    def names(self) -> Collection[str]:
        return self._inner.keys()


class Workflow:
    """
    A workflow is a collection of WorkflowSteps and some context

    It is possible to specify additional compose files for specific workflows, and all
    their child workflows will have access to services defined in those files.
    """

    def __init__(
        self,
        name: str,
        steps: List["WorkflowStep"],
        env: Dict[str, str],
        composition: Composition,
    ) -> None:
        self.name = name
        self.env = env
        self.composition = composition
        self._steps = steps

    def overview(self) -> str:
        return "{} [{}]".format(self.name, " ".join([s.name for s in self._steps]))

    def __repr__(self) -> str:
        return "Workflow<{}>".format(self.overview())

    def run(self, args: List[str]) -> None:
        for step in self._steps:
            step.run(self)

    @contextmanager
    def with_services(self, services: List["PythonService"]) -> Iterator[None]:
        """Temporarily update the composition with the specified services.

        The services must already exist in the composition. They restored to
        their old definitions when the `with` block ends. Note that the service
        definition is written in its entirety; i.e., the configuration is not
        deep merged but replaced wholesale.

        Lest you are tempted to change this function to allow dynamically
        injecting new services: do not do this! These services will not be
        visible to other commands, like `mzcompose run`, `mzcompose logs`, or
        `mzcompose down`, which makes debugging or inspecting the composition
        challenging.
        """
        # Remember the old composition.
        old_compose = copy.deepcopy(self.composition.compose)

        # Update the composition with the new service definitions.
        for service in services:
            if service.name not in self.composition.compose["services"]:
                raise RuntimeError(
                    "programming error in call to Workflow.with_services: "
                    f"{service.name!r} does not exist"
                )
            self.composition.compose["services"][service.name] = service.config
        self.composition._write_compose()

        try:
            # Run the next composition.
            yield
        finally:
            # Restore the old composition.
            self.composition.compose = old_compose
            self.composition._write_compose()

    def run_compose(
        self, args: List[str], capture: bool = False
    ) -> subprocess.CompletedProcess:
        return self.composition.run(args, self.env, capture=capture)

    def run_sql(self, sql: str) -> None:
        """Run a batch of SQL statements against the materialized service."""
        ports = self.composition.find_host_ports("materialized")
        conn = pg8000.connect(host="localhost", user="materialize", port=int(ports[0]))
        conn.autocommit = True
        cursor = conn.cursor()
        for statement in sqlparse.split(sql):
            cursor.execute(statement)

    def start_and_wait_for_tcp(self, services: List[str]) -> None:
        """Sequentially start the named services, waiting for eaach to become
        available via TCP before moving on to the next."""
        # TODO(benesch): once the workflow API is a proper Python API,
        # remove the `type: ignore` comments below.
        for service in services:
            self.start_services(services=[service])  # type: ignore
            for port in self.composition.compose["services"][service].get("ports", []):
                self.wait_for_tcp(host=service, port=port)  # type: ignore


class PythonServiceConfig(TypedDict, total=False):
    mzbuild: str
    image: str
    hostname: str
    command: str
    ports: Sequence[Union[int, str]]
    environment: List[str]
    depends_on: List[str]
    entrypoint: List[str]
    volumes: List[str]
    networks: Dict[str, Dict[str, List[str]]]
    deploy: Dict[str, Dict[str, Dict[str, str]]]
    propagate_uid_gid: bool
    init: bool


class PythonService:
    """
    A PythonService is a service that has been specified in the 'services' variable of mzworkflows.py
    """

    def __init__(self, name: str, config: PythonServiceConfig) -> None:
        self.name = name
        self.config = config


class Materialized(PythonService):
    def __init__(
        self,
        name: str = "materialized",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        port: Union[int, str] = 6875,
        workers: Optional[int] = None,
        memory: Optional[str] = None,
        data_directory: str = "/share/mzdata",
        timestamp_frequency: str = "100ms",
        options: Optional[Union[str, List[str]]] = "",
        environment: Optional[List[str]] = None,
        environment_extra: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        volumes_extra: Optional[List[str]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> None:
        if environment is None:
            environment = [
                "MZ_LOG_FILTER",
                "MZ_SOFT_ASSERTIONS=1",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "MZ_METRICS_SCRAPING_INTERVAL=1s",
            ]

        # Make sure MZ_DEV=1 is always present
        if "MZ_DEV=1" not in environment:
            environment.append("MZ_DEV=1")

        if environment_extra:
            environment.extend(environment_extra)

        if volumes is None:
            volumes = [*DEFAULT_MZ_VOLUMES]
        if volumes_extra:
            volumes.extend(volumes_extra)

        guest_port = port
        if isinstance(port, str) and ":" in port:
            guest_port = port.split(":")[1]

        command_list = [
            f"--data-directory={data_directory}",
            f"--listen-addr 0.0.0.0:{guest_port}",
            "--disable-telemetry",
            "--experimental",
            f"--timestamp-frequency {timestamp_frequency}",
            f"--introspection-frequency {timestamp_frequency}",
            "--retain-prometheus-metrics 1s",
        ]

        if options:
            if isinstance(options, str):
                command_list.append(options)
            else:
                command_list.extend(options)

        if workers:
            command_list.append(f"--workers {workers}")

        config: PythonServiceConfig = (
            {"image": image} if image else {"mzbuild": "materialized"}
        )

        if hostname:
            config["hostname"] = hostname

        # Depending on the docker-compose version, this may either work or be ignored with a warning
        # Unfortunately no portable way of setting the memory limit is known
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "depends_on": depends_on or [],
                "command": " ".join(command_list),
                "ports": [port],
                "environment": environment,
                "volumes": volumes,
            }
        )

        super().__init__(name=name, config=config)


class Coordd(PythonService):
    def __init__(
        self,
        name: str = "coordd",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        port: int = 6875,
        memory: Optional[str] = None,
        data_directory: str = "/share/mzdata",
        options: str = "",
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        mzbuild: str = "coordd",
    ) -> None:
        if environment is None:
            environment = []

        # Make sure MZ_DEV=1 is always present
        if "MZ_DEV=1" not in environment:
            environment.append("MZ_DEV=1")

        if volumes is None:
            volumes = DEFAULT_MZ_VOLUMES

        command = (
            f"--data-directory={data_directory} {options} --listen-addr 0.0.0.0:{port}"
        )

        config: PythonServiceConfig = (
            {"image": image} if image else {"mzbuild": mzbuild}
        )

        if hostname:
            config["hostname"] = hostname

        # Depending on the docker-compose version, this may either work or be ignored with a warning
        # Unfortunately no portable way of setting the memory limit is known
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "command": command,
                "ports": [port],
                "environment": environment,
                "volumes": volumes,
            }
        )

        super().__init__(name=name, config=config)


class Dataflowd(PythonService):
    def __init__(
        self,
        name: str = "dataflowd",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        ports: List[int] = [6876],
        memory: Optional[str] = None,
        options: str = "",
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
    ) -> None:
        command = f"{options}"

        if environment is None:
            environment = [
                "MZ_LOG_FILTER",
                "MZ_SOFT_ASSERTIONS=1",
            ]

        if volumes is None:
            # We currently give dataflowd access to /tmp so that it can load CSV files
            # but this requirement is expected to go away in the future.
            volumes = DEFAULT_MZ_VOLUMES

        config: PythonServiceConfig = (
            {"image": image} if image else {"mzbuild": "dataflowd"}
        )

        if hostname:
            config["hostname"] = hostname

        # Depending on the docker-compose version, this may either work or be ignored with a warning
        # Unfortunately no portable way of setting the memory limit is known
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "command": command,
                "ports": ports,
                "environment": environment,
                "volumes": volumes,
            }
        )

        super().__init__(name=name, config=config)


class Zookeeper(PythonService):
    def __init__(
        self,
        name: str = "zookeeper",
        image: str = "confluentinc/cp-zookeeper",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 2181,
        environment: List[str] = ["ZOOKEEPER_CLIENT_PORT=2181"],
    ) -> None:
        super().__init__(
            name="zookeeper",
            config={
                "image": f"{image}:{tag}",
                "ports": [port],
                "environment": environment,
            },
        )


class Kafka(PythonService):
    def __init__(
        self,
        name: str = "kafka",
        image: str = "confluentinc/cp-kafka",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 9092,
        auto_create_topics: bool = False,
        broker_id: int = 1,
        offsets_topic_replication_factor: int = 1,
        environment: List[str] = [
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            "KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false",
            "KAFKA_MIN_INSYNC_REPLICAS=1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "KAFKA_MESSAGE_MAX_BYTES=15728640",
            "KAFKA_REPLICA_FETCH_MAX_BYTES=15728640",
        ],
        depends_on: List[str] = ["zookeeper"],
    ) -> None:
        environment = [
            *environment,
            f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{name}:9092",
            f"KAFKA_BROKER_ID={broker_id}",
        ]
        config: PythonServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": [port],
            "environment": [
                *environment,
                f"KAFKA_AUTO_CREATE_TOPICS_ENABLE={auto_create_topics}",
                f"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR={offsets_topic_replication_factor}",
            ],
            "depends_on": depends_on,
        }
        super().__init__(name=name, config=config)


class Redpanda(PythonService):
    def __init__(
        self,
        name: str = "redpanda",
        version: str = "v21.11.1",
        image: Optional[str] = None,
        aliases: Optional[List[str]] = None,
        ports: Optional[List[int]] = None,
    ) -> None:
        if image is None:
            image = f"vectorized/redpanda:{version}"

        if ports is None:
            ports = [9092, 8081]

        # The Redpanda container provides both a Kafka and a Schema Registry replacement
        if aliases is None:
            aliases = ["kafka", "schema-registry"]

        # Most of these options are simply required when using Redpanda in Docker.
        # See: https://vectorized.io/docs/quick-start-docker/#Single-command-for-a-1-node-cluster
        # The `enable_transactions` and `enable_idempotence` feature flags enable
        # features Materialize requires that are present by default in Apache Kafka
        # but not in Redpanda.

        command_list = [
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp=1",
            "--memory=1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            '--set "redpanda.enable_transactions=true"',
            '--set "redpanda.enable_idempotence=true"',
            '--set "redpanda.auto_create_topics_enabled=false"',
            f"--advertise-kafka-addr kafka:{ports[0]}",
        ]

        config: PythonServiceConfig = {
            "image": image,
            "ports": ports,
            "command": " ".join(command_list),
            "networks": {"default": {"aliases": aliases}},
        }

        super().__init__(name=name, config=config)


class SchemaRegistry(PythonService):
    def __init__(
        self,
        name: str = "schema-registry",
        image: str = "confluentinc/cp-schema-registry",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 8081,
        kafka_servers: List[str] = ["kafka"],
        environment: List[str] = [
            # NOTE(guswynn): under docker, kafka *can* be really slow, which means
            # the default of 500ms won't work, so we give it PLENTY of time
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
        ],
        depends_on: Optional[List[str]] = None,
    ) -> None:
        bootstrap_servers = ",".join(
            f"PLAINTEXT://{kafka}:9092" for kafka in kafka_servers
        )
        environment = [
            *environment,
            f"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS={bootstrap_servers}",
        ]
        super().__init__(
            name=name,
            config={
                "image": f"{image}:{tag}",
                "ports": [port],
                "environment": environment,
                "depends_on": depends_on or [*kafka_servers, "zookeeper"],
            },
        )


class Postgres(PythonService):
    def __init__(
        self,
        name: str = "postgres",
        mzbuild: str = "postgres",
        port: int = 5432,
        command: str = "postgres -c wal_level=logical -c max_wal_senders=20 -c max_replication_slots=20",
        environment: List[str] = ["POSTGRESDB=postgres", "POSTGRES_PASSWORD=postgres"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "command": command,
                "ports": [port],
                "environment": environment,
            },
        )


class SqlServer(PythonService):
    def __init__(
        self,
        sa_password: str,  # At least 8 characters including uppercase, lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        name: str = "sql-server",
        image: str = "mcr.microsoft.com/mssql/server",
        environment: List[str] = [
            "ACCEPT_EULA=Y",
            "MSSQL_PID=Developer",
            "MSSQL_AGENT_ENABLED=True",
        ],
    ) -> None:
        environment.append(f"SA_PASSWORD={sa_password}")
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [1433],
                "environment": environment,
            },
        )
        self.sa_password = sa_password


class Debezium(PythonService):
    def __init__(
        self,
        name: str = "debezium",
        image: str = f"debezium/connect:{DEFAULT_DEBEZIUM_VERSION}",
        port: int = 8083,
        environment: List[str] = [
            "BOOTSTRAP_SERVERS=kafka:9092",
            "CONFIG_STORAGE_TOPIC=connect_configs",
            "OFFSET_STORAGE_TOPIC=connect_offsets",
            "STATUS_STORAGE_TOPIC=connect_statuses",
            # We don't support JSON, so ensure that connect uses AVRO to encode messages and CSR to
            # record the schema
            "KEY_CONVERTER=io.confluent.connect.avro.AvroConverter",
            "VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter",
            "CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
            "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        ],
        depends_on: List[str] = ["kafka", "schema-registry"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "environment": environment,
                "depends_on": depends_on,
            },
        )


class Toxiproxy(PythonService):
    def __init__(
        self,
        name: str = "toxiproxy",
        image: str = "shopify/toxiproxy:2.1.4",
        port: int = 8474,
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
            },
        )


class Squid(PythonService):
    """
    An HTTP forward proxy, used in some workflows to test whether Materialize can correctly route
    traffic via the proxy.
    """

    def __init__(
        self,
        name: str = "squid",
        image: str = "sameersbn/squid:3.5.27-2",
        port: int = 3128,
        volumes: List[str] = ["./squid.conf:/etc/squid/squid.conf"],
    ) -> None:
        super().__init__(
            name=name,
            config={"image": image, "ports": [port], "volumes": volumes},
        )


class Localstack(PythonService):
    def __init__(
        self,
        name: str = "localstack",
        image: str = f"localstack/localstack:0.13.1",
        port: int = 4566,
        environment: List[str] = ["HOSTNAME_EXTERNAL=localstack"],
        volumes: List[str] = ["/var/run/docker.sock:/var/run/docker.sock"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "environment": environment,
                "volumes": volumes,
            },
        )


class Testdrive(PythonService):
    def __init__(
        self,
        name: str = "testdrive-svc",
        mzbuild: str = "testdrive",
        materialized_url: str = "postgres://materialize@materialized:6875",
        no_reset: bool = False,
        default_timeout: str = "30s",
        seed: Optional[int] = None,
        consistent_seed: bool = False,
        validate_catalog: bool = True,
        entrypoint: Optional[List[str]] = None,
        shell_eval: Optional[bool] = False,
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        volumes_extra: Optional[List[str]] = None,
        volume_workdir: str = ".:/workdir",
    ) -> None:
        if environment is None:
            environment = [
                "TD_TEST",
                "TMPDIR=/share/tmp",
                "MZ_LOG_FILTER",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "SA_PASSWORD",
                "TOXIPROXY_BYTES_ALLOWED",
                "UPGRADE_FROM_VERSION",
            ]

        if volumes is None:
            volumes = [*DEFAULT_MZ_VOLUMES]
        if volumes_extra:
            volumes.extend(volumes_extra)
        volumes.append(volume_workdir)

        if entrypoint is None:
            entrypoint = [
                "testdrive",
                "--kafka-addr=kafka:9092",
                "--schema-registry-url=http://schema-registry:8081",
                f"--materialized-url={materialized_url}",
            ]

        if validate_catalog:
            entrypoint.append("--validate-catalog=/share/mzdata/catalog")

        if no_reset:
            entrypoint.append("--no-reset")

        entrypoint.append(f"--default-timeout {default_timeout}")

        if seed and consistent_seed:
            raise RuntimeError("Can't pass `seed` and `consistent_seed` at same time")
        elif consistent_seed:
            entrypoint.append(f"--seed {random.getrandbits(32)}")
        elif seed:
            entrypoint.append(f"--seed {seed}")

        if shell_eval:
            # Evaluate the arguments as a shell command
            # This allows bashisms to be used to prepare the list of tests to run
            entrypoint.append("${TD_TEST:-`$$*`}")
        else:
            entrypoint.append("${TD_TEST:-$$*}")

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "entrypoint": [
                    "bash",
                    "-O",
                    "extglob",
                    "-c",
                    " ".join(entrypoint),
                    "bash",
                ],
                "environment": environment,
                "volumes": volumes,
                "propagate_uid_gid": True,
                "init": True,
            },
        )


class TestCerts(PythonService):
    def __init__(
        self,
        name: str = "test-certs",
    ) -> None:
        super().__init__(
            name="test-certs",
            config={
                "mzbuild": "test-certs",
                "volumes": ["secrets:/secrets"],
            },
        )


class SqlLogicTest(PythonService):
    def __init__(
        self,
        name: str = "sqllogictest-svc",
        mzbuild: str = "sqllogictest",
        environment: List[str] = [
            "RUST_BACKTRACE=full",
            "PGUSER=postgres",
            "PGHOST=postgres",
            "PGPASSWORD=postgres",
            "MZ_SOFT_ASSERTIONS=1",
        ],
        volumes: List[str] = ["../..:/workdir"],
        depends_on: List[str] = ["postgres"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "environment": environment,
                "volumes": volumes,
                "depends_on": depends_on,
                "propagate_uid_gid": True,
                "init": True,
            },
        )


class PrometheusSQLExporter(PythonService):
    def __init__(self) -> None:
        super().__init__(
            name="prometheus-sql-exporter",
            config={
                "mzbuild": "ci-mz-sql-exporter",
                "ports": ["9400"],
            },
        )


class Kgen(PythonService):
    def __init__(
        self,
        name: str = "kgen",
        mzbuild: str = "kgen",
        depends_on: List[str] = ["kafka"],
    ) -> None:
        entrypoint = [
            "kgen",
            "--bootstrap-server=kafka:9092",
        ]

        if "schema-registry" in depends_on:
            entrypoint.append("--schema-registry-url=http://schema-registry:8081")

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "depends_on": depends_on,
                "entrypoint": entrypoint,
            },
        )


class PythonWorkflow(Workflow):
    """
    A PythonWorkflow is a workflow that has been specified as a Python function in a mzworkflows.py file
    """

    def __init__(
        self,
        name: str,
        func: Callable,
        env: Dict[str, str],
        composition: Composition,
    ) -> None:
        self.name = name
        self.func = func
        self.env = env
        self.composition = composition
        self.takes_args = len(inspect.signature(func).parameters) > 1

    def overview(self) -> str:
        return "{} [{}]".format(self.name, self.func)

    def __repr__(self) -> str:
        return "Workflow<{}>".format(self.overview())

    def run(self, args: List[str]) -> None:
        print("Running Python function {}".format(self.name))
        old_env = dict(os.environ)
        os.environ.clear()
        os.environ.update(self.env)

        try:
            if self.takes_args:
                self.func(self, args)
            else:
                # If the workflow doesn't have an `args` parameter, construct
                # an empty parser to reject bogus arguments and to handle the
                # trivial help message.
                parser = WorkflowArgumentParser(self)
                parser.parse_args(args)
                self.func(self)
        finally:
            os.environ.clear()
            os.environ.update(old_env)


class WorkflowArgumentParser(argparse.ArgumentParser):
    """An argument parser that takes its name and description from a `Workflow`."""

    def __init__(self, w: PythonWorkflow):
        super().__init__(
            prog=f"mzcompose run {w.name}", description=inspect.getdoc(w.func)
        )


class Steps:
    """A registry of named `WorkflowStep`_"""

    _steps: Dict[str, Type["WorkflowStep"]] = {}

    @classmethod
    def named(cls, name: str) -> Type["WorkflowStep"]:
        try:
            return cls._steps[name]
        except KeyError:
            raise UIError(f"unknown step {name!r}")

    @classmethod
    def register(
        cls, name: str
    ) -> Callable[[Type["WorkflowStep"]], Type["WorkflowStep"]]:
        if name in cls._steps:
            raise ValueError(f"Double registration of step name: {name}")

        def reg(to_register: Type["WorkflowStep"]) -> Type["WorkflowStep"]:
            cls._steps[name] = to_register
            to_register.name = name

            # Allow the step to also be called as a Workflow.step_name() classmethod
            def run_step(workflow: Workflow, **kwargs: Any) -> Optional[str]:
                step: WorkflowStep = to_register(**kwargs)
                return step.run(workflow)

            func_name = name.replace("-", "_")
            if func_name == "run":
                # Temporary workaround for the fact that `Workflow.run` already
                # exists.
                func_name = "run_service"
            if not hasattr(Workflow, func_name):
                setattr(Workflow, func_name, run_step)
            else:
                raise UIError(
                    f"Unable to register method Workflow.{func_name} as one already exists."
                )

            return to_register

        return reg

    @classmethod
    def print_known_steps(cls) -> None:
        """Print all steps registered with `register`_"""
        for name in sorted(cls._steps):
            print(name)


class WorkflowStep:
    """Peform a single action in a workflow"""

    # populated by Steps.register
    name: str
    """The name used to refer to this step in a workflow file"""

    def __init__(self, **kwargs: Any) -> None:
        pass

    def run(self, workflow: Workflow) -> Optional[str]:
        """Perform the action specified by this step"""


@Steps.register("print-env")
class PrintEnvStep(WorkflowStep):
    """Prints the `env` `Dict` for this workflow."""

    def run(self, workflow: Workflow) -> None:
        print("Workflow has environment of", workflow.env)


@Steps.register("sleep")
class Sleep(WorkflowStep):
    """Waits for the defined duration of time."""

    def __init__(self, duration: Union[int, str]) -> None:
        self._duration = int(duration)

    def run(self, workflow: Workflow) -> None:
        print(f"Sleeping {self._duration} seconds")
        time.sleep(self._duration)


@Steps.register("start-services")
class StartServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
    """

    def __init__(self, *, services: Optional[List[str]] = None) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise UIError(f"services should be a list, got: {self._services}")

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(["up", "-d", *self._services])
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise UIError(f"services didn't come up cleanly: {services}")


@Steps.register("kill-services")
class KillServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
      signal: signal to send to the container (e.g. SIGINT)
    """

    def __init__(
        self, *, services: Optional[List[str]] = None, signal: Optional[str] = None
    ) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise UIError(f"services should be a list, got: {self._services}")
        self._signal = signal

    def run(self, workflow: Workflow) -> None:
        compose_cmd = ["kill"]
        if self._signal:
            compose_cmd.extend(["-s", self._signal])
        compose_cmd.extend(self._services)

        try:
            workflow.run_compose(compose_cmd)
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise UIError(f"services didn't die cleanly: {services}")


@Steps.register("restart-services")
class RestartServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
    """

    def __init__(self, *, services: Optional[List[str]] = None) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise UIError(f"services should be a list, got: {self._services}")

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(["restart", *self._services])
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise UIError(f"services didn't restart cleanly: {services}")


@Steps.register("remove-services")
class RemoveServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
      destroy_volumes: Boolean to indicate if the volumes should be removed as well
    """

    def __init__(
        self,
        *,
        services: Optional[List[str]] = None,
        destroy_volumes: bool = False,
    ) -> None:
        self._services = services if services is not None else []
        self._destroy_volumes = destroy_volumes
        if not isinstance(self._services, list):
            raise UIError(f"services should be a list, got: {self._services}")

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(
                [
                    "rm",
                    "-f",
                    "-s",
                    *(["-v"] if self._destroy_volumes else []),
                    *self._services,
                ],
            )
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise UIError(f"services didn't restart cleanly: {services}")


@Steps.register("remove-volumes")
class RemoveVolumesStep(WorkflowStep):
    """
    Params:
      volumes: List of volume names
    """

    def __init__(self, *, volumes: List[str]) -> None:
        self._volumes = volumes
        if not isinstance(self._volumes, list):
            raise UIError(f"volumes should be a list, got: {self._volumes}")

    def run(self, workflow: Workflow) -> None:
        volumes = (f"{workflow.composition.name}_{v}" for v in self._volumes)
        spawn.runv(["docker", "volume", "rm", *volumes])


@Steps.register("wait-for-postgres")
class WaitForPgStep(WorkflowStep):
    """
    Args:
        dbname: the name of the database to wait for
        host: the host postgres is listening on
        port: the port postgres is listening on
        timeout_secs: How long to wait for postgres to be up before failing (Default: 30)
        query: The query to execute to ensure that it is running (Default: "Select 1")
        user: The chosen user (this is only relevant for postgres)
        service: The service that postgres is running as (Default: postgres)
    """

    def __init__(
        self,
        *,
        dbname: str = "postgres",
        port: Optional[int] = None,
        host: str = "localhost",
        timeout_secs: int = 120,
        query: str = "SELECT 1",
        user: str = "postgres",
        password: str = "postgres",
        expected: Union[Iterable[Any], Literal["any"]] = [[1]],
        print_result: bool = False,
        service: str = "postgres",
    ) -> None:
        self._dbname = dbname
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._timeout_secs = timeout_secs
        self._query = query
        self._expected = expected
        self._print_result = print_result
        self._service = service

    def run(self, workflow: Workflow) -> None:
        if self._port is None:
            ports = workflow.composition.find_host_ports(self._service)
            if len(ports) != 1:
                logs = workflow.composition.service_logs(self._service)
                if ports:
                    msg = (
                        f"Unable to unambiguously determine port for {self._service},"
                        f"found ports: {','.join(ports)}\nService logs:\n{logs}"
                    )
                else:
                    msg = f"No ports found for {self._service}\nService logs:\n{logs}"
                raise UIError(msg)
            port = int(ports[0])
        else:
            port = self._port
        try:
            wait_for_pg(
                dbname=self._dbname,
                host=self._host,
                port=port,
                timeout_secs=self._timeout_secs,
                query=self._query,
                user=self._user,
                password=self._password,
                expected=self._expected,
                print_result=self._print_result,
            )
        except UIError as e:
            logs = workflow.composition.service_logs(self._service)
            raise UIError(f"{e}:\nService logs:\n{logs}")


@Steps.register("wait-for-mz")
class WaitForMzStep(WaitForPgStep):
    """Same thing as wait-for-postgres, but with materialized defaults"""

    def __init__(
        self,
        *,
        user: str = "materialize",
        dbname: str = "materialize",
        host: str = "localhost",
        port: Optional[int] = None,
        timeout_secs: int = 60,
        query: str = "SELECT 1",
        expected: Union[Iterable[Any], Literal["any"]] = [[1]],
        print_result: bool = False,
        service: str = "materialized",
    ) -> None:
        super().__init__(
            user=user,
            dbname=dbname,
            host=host,
            port=port,
            timeout_secs=timeout_secs,
            query=query,
            expected=expected,
            print_result=print_result,
            service=service,
        )


@Steps.register("wait-for-mysql")
class WaitForMysqlStep(WorkflowStep):
    """
    Params:
        host: The host mysql is running on
        port: The port mysql is listening on (Default: discover host port)
        user: The user to connect as (Default: mysqluser)
        password: The password to use (Default: mysqlpw)
        service: The name mysql is running as (Default: mysql)
    """

    def __init__(
        self,
        *,
        user: str = "root",
        password: str = "rootpw",
        host: str = "localhost",
        port: Optional[int] = None,
        timeout_secs: int = 60,
        service: str = "mysql",
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs
        self._service = service

    def run(self, workflow: Workflow) -> None:
        if self._port is None:
            ports = workflow.composition.find_host_ports(self._service)
            if len(ports) != 1:
                raise UIError(
                    f"Could not unambiguously determine port for {self._service} "
                    f"found: {','.join(ports)}"
                )
            port = int(ports[0])
        else:
            port = self._port
        wait_for_mysql(
            user=self._user,
            passwd=self._password,
            host=self._host,
            port=port,
            timeout_secs=self._timeout_secs,
        )


@Steps.register("run-mysql")
class RunMysql(WorkflowStep):
    """
    Params:
        host: The host mysql is running on
        port: The port mysql is listening on (Default: discover host port)
        user: The user to connect as (Default: root)
        password: The password to use (Default: rootpw)
        service: The name mysql is running as (Default: mysql)
        query: The query to execute
    """

    def __init__(
        self,
        *,
        user: str = "root",
        password: str = "rootpw",
        host: str = "localhost",
        port: Optional[int] = None,
        service: str = "mysql",
        query: str,
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._service = service
        self._query = query

    def run(self, workflow: Workflow) -> None:
        if self._port is None:
            ports = workflow.composition.find_host_ports(self._service)
            if len(ports) != 1:
                raise UIError(
                    f"Could not unambiguously determine port for {self._service} "
                    f"found: {','.join(ports)}"
                )
            port = int(ports[0])
        else:
            port = self._port
        conn = pymysql.connect(
            user=self._user,
            passwd=self._password,
            host=self._host,
            port=port,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
            autocommit=True,
        )
        with conn.cursor() as cur:
            cur.execute(self._query)


class WaitDependency(TypedDict):
    """For wait-for-tcp, specify additional items to check"""

    host: str
    port: int
    hint: Optional[str]


@Steps.register("wait-for-tcp")
class WaitForTcpStep(WorkflowStep):
    """Wait for a tcp port to be open inside a container

    Params:
        host: The host that is available inside the docker network
        port: the port to connect to
        timeout_secs: How long to wait (default: 30)

        dependencies: A list of {host, port, hint} objects that must
            continue to be up while checking this one. Immediately fail
            the wait if these go down.
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int,
        timeout_secs: int = 240,
        dependencies: Optional[List[WaitDependency]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs
        self._dependencies = dependencies or []

    def run(self, workflow: Workflow) -> None:
        ui.progress(f"waiting for {self._host}:{self._port}", "C")
        for remaining in ui.timeout_loop(self._timeout_secs):
            cmd = f"docker run --rm -t --network {workflow.composition.name}_default ubuntu:focal-20210723".split()

            try:
                _check_tcp(cmd[:], self._host, self._port, self._timeout_secs)
            except subprocess.CalledProcessError:
                ui.progress(" {}".format(int(remaining)))
            else:
                ui.progress(" success!", finish=True)
                return

            for dep in self._dependencies:
                host, port = dep["host"], dep["port"]
                try:
                    _check_tcp(
                        cmd[:], host, port, self._timeout_secs, kind="dependency "
                    )
                except subprocess.CalledProcessError:
                    message = f"Dependency is down {host}:{port}"
                    try:
                        dep_logs = workflow.composition.service_logs(host)
                    except Exception as e:
                        dep_logs = f"unable to determine logs: {e}"
                    if "hint" in dep:
                        message += f"\n    hint: {dep['hint']}"
                    message += "\nDependency service logs:\n"
                    message += dep_logs
                    ui.progress(" error!", finish=True)
                    raise UIError(message)

        ui.progress(" error!", finish=True)
        try:
            logs = workflow.composition.service_logs(self._host)
        except Exception as e:
            logs = f"unable to determine logs: {e}"

        raise UIError(
            f"Unable to connect to {self._host}:{self._port}\nService logs:\n{logs}"
        )


def _check_tcp(
    cmd: List[str], host: str, port: int, timeout_secs: int, kind: str = ""
) -> List[str]:
    cmd.extend(
        [
            "timeout",
            str(timeout_secs),
            "bash",
            "-c",
            f"cat < /dev/null > /dev/tcp/{host}/{port}",
        ]
    )
    try:
        spawn.capture(cmd, unicode=True, stderr_too=True)
    except subprocess.CalledProcessError as e:
        ui.log_in_automation(
            "wait-for-tcp ({}{}:{}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                kind, host, port, ui.shell_quote(cmd), e, e.stdout, e.stderr
            )
        )
        raise
    return cmd


@Steps.register("drop-kafka-topics")
class DropKafkaTopicsStep(WorkflowStep):
    def __init__(self, *, kafka_container: str, topic_pattern: str) -> None:
        self._container = kafka_container
        self._topic_pattern = topic_pattern

    def run(self, workflow: Workflow) -> None:
        say(f"dropping kafka topics {self._topic_pattern} from {self._container}")
        try:
            spawn.runv(
                [
                    "docker",
                    "exec",
                    "-t",
                    self._container,
                    "kafka-topics",
                    "--delete",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--topic",
                    self._topic_pattern,
                ],
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            # generally this is fine, it just means that the topics already don't exist
            ui.log_in_automation(f"DEBUG: error purging topics: {e}: {e.output}")


@Steps.register("workflow")
class WorkflowWorkflowStep(WorkflowStep):
    def __init__(self, workflow: str) -> None:
        self._workflow = workflow

    def run(self, workflow: Workflow) -> None:
        try:
            # Run the specified workflow with the context of the parent workflow
            child_workflow = workflow.composition.get_workflow(
                self._workflow, workflow.env
            )
            print(f"Running workflow {child_workflow.name} ...")
            child_workflow.run([])
        except KeyError:
            raise UIError(f"unknown workflow {workflow.composition.name!r}")


@Steps.register("run")
class RunStep(WorkflowStep):
    """
    Run a service using `mzcompose run`

    Running a service behaves slightly differently than making it come up, importantly it
    is not an _error_ if it ends at all.

    Args:

      - service: (required) the name of the service, from the mzcompose file
      - entrypoint: Overwrite the entrypoint with this
      - command: the command to run. These are the arguments to the entrypoint
      - capture: Capture and return output (default: False)
      - daemon: run as a daemon (default: False)
      - service_ports: expose and use service ports. (Default: True)
      - force_service_name: ensure that this container has exactly the name of
        its service. Only one container can exist with a given name at the same
        time, so this should only be used when a start_services step cannot be used --e.g.
        because it is not desired for it to be restarted on completion, or
        because it needs to be passed command-line arguments.
    """

    def __init__(
        self,
        *,
        service: str,
        command: Optional[Union[str, list]] = None,
        env: Dict[str, str] = {},
        capture: bool = False,
        daemon: bool = False,
        entrypoint: Optional[str] = None,
        service_ports: bool = True,
        force_service_name: bool = False,
    ) -> None:
        cmd = []
        if daemon:
            cmd.append("-d")
        if entrypoint:
            cmd.append(f"--entrypoint={entrypoint}")
        cmd.append(service)
        if isinstance(command, str):
            cmd.extend(shlex.split(command))
        elif isinstance(command, list):
            cmd.extend(command)
        self._service = service
        self._force_service_name = force_service_name
        self._service_ports = service_ports
        self._command = cmd
        self._capture = capture
        self._env = env

    def run(self, workflow: Workflow) -> Any:
        try:
            return workflow.run_compose(
                capture=self._capture,
                args=[
                    "run",
                    *(["--service-ports"] if self._service_ports else []),
                    *(["--name", self._service] if self._force_service_name else []),
                    *(f"-e{k}={v}" for k, v in self._env.items()),
                    *self._command,
                ],
            ).stdout
        except subprocess.CalledProcessError:
            raise UIError("giving up: {}".format(ui.shell_quote(self._command)))


@Steps.register("exec")
class ExecStep(WorkflowStep):
    """
    Run 'docker-compose exec' using `mzcompose run`

    Args:

      - service: (required) the name of the service
      - command: (required) the command to run
    """

    def __init__(self, *, service: str, command: Union[str, list]) -> None:
        self._service = service
        cmd_list = ["exec", self._service]
        if isinstance(command, str):
            cmd_list.extend(shlex.split(command))
        elif isinstance(command, list):
            cmd_list.extend(command)

        self._service = service
        self._command = cmd_list

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(self._command)
        except subprocess.CalledProcessError:
            raise UIError("giving up: {}".format(ui.shell_quote(self._command)))


@Steps.register("ensure-stays-up")
class EnsureStaysUpStep(WorkflowStep):
    def __init__(self, *, container: str, seconds: int) -> None:
        self._container = container
        self._uptime_secs = seconds

    def run(self, workflow: Workflow) -> None:
        ui.progress(f"Ensuring {self._container} stays up ", "C")
        for i in range(self._uptime_secs, 0, -1):
            time.sleep(1)
            containers = [
                s["Name"]
                for s in workflow.composition.inspect_service_containers(
                    self._container, include_stopped=True
                )
            ]
            if not containers:
                try:
                    logs = workflow.composition.service_logs(self._container)
                except subprocess.CalledProcessError as e:
                    logs = (
                        f"Unable to determine service logs, docker output:\n{e.output}"
                    )
                raise UIError(
                    f"container {self._container} stopped running!\nService logs:\n{logs}"
                )
            ui.progress(f" {i}")
        print()


@Steps.register("down")
class DownStep(WorkflowStep):
    def __init__(self, *, destroy_volumes: bool = False) -> None:
        """Bring the cluster down"""
        self._destroy_volumes = destroy_volumes

    def run(self, workflow: Workflow) -> None:
        say("bringing the cluster down")
        workflow.run_compose(["down", *(["-v"] if self._destroy_volumes else [])])


@Steps.register("wait")
class WaitStep(WorkflowStep):
    def __init__(
        self, *, service: str, expected_return_code: int, print_logs: bool = False
    ) -> None:
        """Wait for the container with name service to exit"""
        self._expected_return_code = expected_return_code
        self._service = service
        self._print_logs = print_logs

    def run(self, workflow: Workflow) -> None:
        say(f"Waiting for the service {self._service} to exit")
        ps_proc = workflow.run_compose(["ps", "-q", self._service], capture=True)
        container_ids = [c for c in ps_proc.stdout.strip().split("\n")]
        if len(container_ids) > 1:
            raise UIError(
                f"Expected to get a single container for {self._service}; got: {container_ids}"
            )
        elif not container_ids:
            raise UIError(f"No containers returned for service {self._service}")

        container_id = container_ids[0]
        wait_cmd = ["docker", "wait", container_id]
        wait_proc = spawn.runv(wait_cmd, capture_output=True)
        return_codes = [
            int(c) for c in wait_proc.stdout.decode("utf-8").strip().split("\n")
        ]
        if len(return_codes) != 1:
            raise UIError(
                f"Expected single exit code for {container_id}; got: {return_codes}"
            )

        return_code = return_codes[0]
        if return_code != self._expected_return_code:
            raise UIError(
                f"Expected exit code {self._expected_return_code} for {container_id}; got: {return_code}"
            )

        if self._print_logs:
            spawn.runv(["docker", "logs", container_id])


def wait_for_pg(
    timeout_secs: int,
    query: str,
    dbname: str,
    port: int,
    host: str,
    user: str,
    password: str,
    print_result: bool,
    expected: Union[Iterable[Any], Literal["any"]],
) -> None:
    """Wait for a pg-compatible database (includes materialized)"""
    args = f"dbname={dbname} host={host} port={port} user={user} password={password}"
    ui.progress(f"waiting for {args} to handle {query!r}", "C")
    error = None
    for remaining in ui.timeout_loop(timeout_secs):
        try:
            conn = pg8000.connect(
                database=dbname,
                host=host,
                port=port,
                user=user,
                password=password,
                timeout=1,
            )
            # The default (autocommit = false) wraps everything in a transaction.
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(query)
            if expected == "any" and cur.rowcount == -1:
                ui.progress("success!", finish=True)
                return
            result = list(cur.fetchall())
            if expected == "any" or result == expected:
                if print_result:
                    say(f"query result: {result}")
                else:
                    ui.progress("success!", finish=True)
                return
            else:
                say(
                    f"host={host} port={port} did not return rows matching {expected} got: {result}"
                )
        except Exception as e:
            ui.progress(" " + str(int(remaining)))
            error = e
    ui.progress(finish=True)
    raise UIError(f"never got correct result for {args}: {error}")


def wait_for_mysql(
    timeout_secs: int, user: str, passwd: str, host: str, port: int
) -> None:
    args = f"mysql user={user} host={host} port={port}"
    ui.progress(f"waiting for {args}", "C")
    error = None
    for _ in ui.timeout_loop(timeout_secs):
        try:
            conn = pymysql.connect(user=user, passwd=passwd, host=host, port=port)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
            if result == (1,):
                print(f"success!")
                return
            else:
                print(f"weird, {args} did not return 1: {result}")
        except Exception as e:
            ui.progress(".")
            error = e
    ui.progress(finish=True)

    raise UIError(f"Never got correct result for {args}: {error}")
