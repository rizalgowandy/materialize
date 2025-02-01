# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum

from materialize.zippy.framework import Capabilities, Capability


class ReplicaSizeType(Enum):
    Nodes = 1
    Workers = 2
    Both = 3


class ReplicaExists(Capability):
    """A replica exists in the Mz instance."""

    name: str
    size_type: ReplicaSizeType
    size: str

    def __init__(self, name: str) -> None:
        self.name = name


def source_capable_clusters(capabilities: Capabilities) -> list[str]:
    if len(capabilities.get(ReplicaExists)) > 0:
        # Default cluster may have multiple replicas, can not be used for sources
        return ["storage"]
    else:
        return ["storage", "quickstart"]
