// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Command-line tool for interacting with Materialize Cloud.

use mzcloud::apis::{
    configuration::Configuration,
    deployments_api::{
        deployments_certs_retrieve, deployments_create, deployments_destroy, deployments_list,
        deployments_logs_retrieve, deployments_retrieve, deployments_update,
    },
    mz_versions_api::mz_versions_list,
    schema_api::schema_retrieve,
};
use mzcloud::models::{deployment_request::DeploymentRequest, deployment_size::DeploymentSize};

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Command {
    /// Create a new Materialize deployment.
    Create {
        /// Version of materialized to deploy. Defaults to latest available version.
        #[structopt(short, long)]
        mz_version: Option<String>,

        /// Size of the deployment.
        #[structopt(short, long, parse(try_from_str = parse_size))]
        size: Option<DeploymentSize>,
    },

    /// Describe a Materialize deployment.
    Describe {
        /// ID of the deployment.
        id: String,
    },

    /// Change the version or size of a Materialize deployment.
    Update {
        /// ID of the deployment.
        id: String,

        /// Version of materialized to upgrade to.
        mz_version: String,

        /// Size of the deployment. Defaults to current size.
        #[structopt(short, long, parse(try_from_str = parse_size))]
        size: Option<DeploymentSize>,
    },

    /// Destroy a Materialize deployment.
    Destroy {
        /// ID of the deployment.
        id: String,
    },

    /// List existing Materialize deployments.
    List,

    /// Download the certificates bundle for a Materialize deployment.
    Certs {
        /// ID of the deployment.
        id: String,
        /// Path to save the certs bundle to.
        #[structopt(short, long, default_value = "mzcloud-certs.zip")]
        output_file: String,
    },
    /// Download the logs from a Materialize deployment.
    Logs {
        /// ID of the deployment.
        id: String,
    },

    /// List all possible materialize versions.
    MzVersions,

    /// Get the OpenApi v3 schema for Materialize Cloud.
    Schema,
}

#[derive(Debug, StructOpt)]
struct Opts {
    /// Bearer token for authentication.
    #[structopt(short, long, env = "MZCLOUD_TOKEN", hide_env_values = true)]
    token: String,

    /// Action to take.
    #[structopt(subcommand)]
    command: Command,
}

fn parse_size(s: &str) -> Result<DeploymentSize, String> {
    match s {
        "XS" => Ok(DeploymentSize::XS),
        "S" => Ok(DeploymentSize::S),
        "M" => Ok(DeploymentSize::M),
        "L" => Ok(DeploymentSize::L),
        "XL" => Ok(DeploymentSize::XL),
        _ => Err("Invalid size.".to_owned()),
    }
}

async fn mz_version_or_latest(
    config: &Configuration,
    mz_version: Option<String>,
) -> anyhow::Result<String> {
    Ok(match mz_version {
        Some(v) => v,
        None => mz_versions_list(&config)
            .await?
            .last()
            .expect("No materialize versions supported by Materialize Cloud server.")
            .to_owned(),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::process::exit(match run().await {
        Ok(_) => 0,
        Err(err) => {
            eprintln!("error: {:#?}", err);
            1
        }
    })
}

async fn run() -> anyhow::Result<()> {
    let opts = Opts::from_args();
    let config = Configuration {
        base_path: "http://localhost:8000".to_owned(),
        user_agent: Some("mzcloud-cli/0.1.0/rust".to_owned()),
        client: reqwest::Client::new(),
        basic_auth: None,
        oauth_access_token: None,
        bearer_access_token: Some(opts.token),
        api_key: None,
    };
    match opts.command {
        Command::Create { size, mz_version } => {
            let mz_version = mz_version_or_latest(&config, mz_version).await?;
            let deployment =
                deployments_create(&config, DeploymentRequest { size, mz_version }).await?;
            println!("{}", serde_json::to_string_pretty(&deployment)?);
        }
        Command::Describe { id } => {
            let deployment = deployments_retrieve(&config, &id).await?;
            println!("{}", serde_json::to_string_pretty(&deployment)?);
        }
        Command::Update {
            id,
            size,
            mz_version,
        } => {
            let deployment =
                deployments_update(&config, &id, DeploymentRequest { size, mz_version }).await?;
            println!("{}", serde_json::to_string_pretty(&deployment)?);
        }
        Command::Destroy { id } => {
            deployments_destroy(&config, &id).await?;
        }
        Command::List => {
            let deployments = deployments_list(&config).await?;
            println!("{}", serde_json::to_string_pretty(&deployments)?);
        }
        Command::Certs { id, output_file } => {
            let bytes = deployments_certs_retrieve(&config, &id).await?;
            std::fs::write(&output_file, &bytes)?;
            println!("Certificate bundle saved to {}", &output_file);
        }
        Command::Logs { id } => {
            let logs = deployments_logs_retrieve(&config, &id).await?;
            print!("{}", logs);
        }
        Command::MzVersions => {
            let versions = mz_versions_list(&config).await?;
            println!("{}", serde_json::to_string_pretty(&versions)?);
        }
        Command::Schema => {
            let schema = schema_retrieve(&config, Some("json")).await?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    };

    Ok(())
}
