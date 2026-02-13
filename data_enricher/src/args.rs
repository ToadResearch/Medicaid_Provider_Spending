use clap::Parser;

use crate::constants::{DEFAULT_DATASET_URL, DEFAULT_HCPCS_API_BASE_URL, DEFAULT_NPI_API_BASE_URL};

#[derive(Debug, Parser)]
#[command(name = "data_enricher")]
#[command(about = "Build resumable NPI/HCPCS mappings and enrich Medicaid provider spending data")]
pub struct Args {
    /// Local dataset path (.csv or .parquet). If omitted, it defaults to data/<url-file>.
    #[arg(long)]
    pub input_path: Option<std::path::PathBuf>,

    /// Source URL used when input_path does not exist locally.
    #[arg(long, default_value = DEFAULT_DATASET_URL)]
    pub input_url: String,

    /// Enriched output path (.csv or .parquet).
    #[arg(long)]
    pub output_path: Option<std::path::PathBuf>,

    /// NPI -> provider mapping CSV output path.
    #[arg(long, alias = "npi-mapping-csv")]
    pub mapping_csv: Option<std::path::PathBuf>,

    /// SQLite cache database path for NPI resumable lookup state.
    #[arg(long, alias = "npi-cache-db")]
    pub cache_db: Option<std::path::PathBuf>,

    /// HCPCS mapping CSV output path.
    #[arg(long)]
    pub hcpcs_mapping_csv: Option<std::path::PathBuf>,

    /// SQLite cache database path for HCPCS resumable lookup state.
    #[arg(long)]
    pub hcpcs_cache_db: Option<std::path::PathBuf>,

    /// Build mapping files only, skip enrichment.
    #[arg(long, default_value_t = false)]
    pub build_map_only: bool,

    /// Rebuild mapping files from cache+API even if mapping CSV already exists.
    #[arg(long, default_value_t = false)]
    pub rebuild_map: bool,

    /// Reset mapping state by deleting mapping CSVs and cache DBs first.
    #[arg(long, default_value_t = false)]
    pub reset_map: bool,

    /// Max concurrent in-flight API requests.
    #[arg(long, default_value_t = 2)]
    pub concurrency: usize,

    /// Global request start rate for API calls.
    ///
    /// NPPES reference:
    /// https://npiregistry.cms.hhs.gov/
    /// https://npiregistry.cms.hhs.gov/api-page
    ///
    /// HCPCS API (Clinical Tables) rate-limit guidance:
    /// https://clinicaltables.nlm.nih.gov/faq.html
    #[arg(long, default_value_t = 2)]
    pub requests_per_second: u32,

    /// Max retry attempts for transient API failures.
    #[arg(long, default_value_t = 5)]
    pub max_retries: u32,

    /// Optional cap for new uncached lookups in this run.
    #[arg(long)]
    pub max_new_lookups: Option<usize>,

    /// Skip API requests and only use existing cache entries.
    #[arg(long, default_value_t = false)]
    pub skip_api: bool,

    /// NPI API base URL.
    #[arg(long, default_value = DEFAULT_NPI_API_BASE_URL)]
    pub api_base_url: String,

    /// NPI API version query parameter.
    #[arg(long, default_value = "2.1")]
    pub api_version: String,

    /// HCPCS API base URL.
    #[arg(long, default_value = DEFAULT_HCPCS_API_BASE_URL)]
    pub hcpcs_api_base_url: String,

    /// Optional Hugging Face token. Upload only happens if upload flags are set.
    #[arg(long)]
    pub hf_token: Option<String>,

    /// Optional Hugging Face repo id. Upload only happens if upload flags are set.
    #[arg(long)]
    pub hf_repo_id: Option<String>,

    /// Hugging Face repo type.
    #[arg(long, default_value = "dataset")]
    pub hf_repo_type: String,

    /// Upload NPI mapping CSV to Hugging Face (requires hf_token + hf_repo_id).
    #[arg(long, default_value_t = false)]
    pub hf_upload_mapping: bool,

    /// Upload HCPCS mapping CSV to Hugging Face (requires hf_token + hf_repo_id).
    #[arg(long, default_value_t = false)]
    pub hf_upload_hcpcs_mapping: bool,

    /// Upload enriched dataset to Hugging Face (requires hf_token + hf_repo_id).
    #[arg(long, default_value_t = false)]
    pub hf_upload_enriched: bool,

    /// Destination path for NPI mapping file in Hugging Face repo.
    #[arg(long)]
    pub hf_mapping_path_in_repo: Option<String>,

    /// Destination path for HCPCS mapping file in Hugging Face repo.
    #[arg(long)]
    pub hf_hcpcs_mapping_path_in_repo: Option<String>,

    /// Destination path for enriched file in Hugging Face repo.
    #[arg(long)]
    pub hf_enriched_path_in_repo: Option<String>,
}
