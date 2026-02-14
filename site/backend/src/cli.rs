use clap::{Parser, Subcommand};

const DEFAULT_DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data");

#[derive(Parser, Debug)]
#[command(name = "site-backend")]
#[command(about = "Spending Explorer backend (DuckDB + Tantivy)", long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Download inputs (if missing), build DuckDB rollups, build Tantivy indices.
    Build(BuildArgs),
    /// Serve the HTTP API (requires a completed build).
    Serve(ServeArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct BuildArgs {
    /// Backend data directory (downloads, DuckDB DB, Tantivy indices).
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    pub data_dir: String,

    /// Hugging Face dataset repo id (e.g., mkieffer/Medicaid-Provider-Spending).
    #[arg(long, default_value = "mkieffer/Medicaid-Provider-Spending")]
    pub hf_repo: String,

    /// Hugging Face revision (branch/tag/commit).
    #[arg(long, default_value = "main")]
    pub hf_revision: String,

    /// Do not download missing inputs; error instead.
    #[arg(long)]
    pub offline: bool,

    /// Re-download inputs even if they already exist.
    #[arg(long)]
    pub force_download: bool,

    /// Use an already-downloaded ZIP centroid file (GeoNames tab-separated format).
    #[arg(long)]
    pub zip_centroids_file: Option<String>,

    /// Rebuild DuckDB tables and Tantivy indices even if they already exist.
    #[arg(long)]
    pub rebuild: bool,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ServeArgs {
    /// Backend data directory (DuckDB DB and Tantivy indices).
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    pub data_dir: String,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 8787)]
    pub port: u16,
}
