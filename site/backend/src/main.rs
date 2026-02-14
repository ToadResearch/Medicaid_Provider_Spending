mod build;
mod cli;
mod download;
mod geo;
mod hcpcs;
mod index;
mod npi;
mod server;
mod storage;

use anyhow::Context;
use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = cli::Args::parse();

    match args.cmd {
        cli::Command::Build(cmd) => build::run(cmd).await.context("build failed"),
        cli::Command::Serve(cmd) => server::run(cmd).await.context("serve failed"),
    }
}
