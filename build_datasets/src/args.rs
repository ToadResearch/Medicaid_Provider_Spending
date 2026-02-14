use clap::Parser;

use crate::constants::{DEFAULT_DATASET_URL, DEFAULT_HCPCS_API_BASE_URL, DEFAULT_NPI_API_BASE_URL};

#[derive(Debug, Parser)]
#[command(name = "build_datasets")]
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

    /// Output CSV path for unresolved identifiers report (NPI + HCPCS).
    #[arg(long)]
    pub unresolved_report_csv: Option<std::path::PathBuf>,

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

    /// NPI API reference dataset output path (.parquet).
    #[arg(long)]
    pub npi_api_reference_parquet: Option<std::path::PathBuf>,

    /// HCPCS API reference dataset output path (.parquet).
    #[arg(long)]
    pub hcpcs_api_reference_parquet: Option<std::path::PathBuf>,

    /// Optional local CPT/HCPCS fallback CSV used when HCPCS API is missing codes.
    ///
    /// Expected columns: hcpcs_code, short_desc, long_desc (date/flag columns optional).
    #[arg(long)]
    pub hcpcs_fallback_csv: Option<std::path::PathBuf>,

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

    /// Additional retry rounds for identifiers that still fail after per-request retries.
    ///
    /// Example: with 2 rounds, the pipeline does initial pass + up to 2 follow-up passes
    /// for request failures, with cooldown between rounds.
    #[arg(long, default_value_t = 2)]
    pub failure_retry_rounds: u32,

    /// Cooldown in seconds before each follow-up failure-retry round.
    #[arg(long, default_value_t = 30)]
    pub failure_retry_delay_seconds: u64,

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

    /// Number of HCPCS codes to query per batched HCPCS API request.
    ///
    /// The HCPCS API allows count up to 500 per request; batch size controls
    /// how many explicit code terms are combined in a single OR query.
    #[arg(long, default_value_t = 100)]
    pub hcpcs_batch_size: usize,

    /// Directory containing extracted monthly NPPES CSV bundles.
    ///
    /// Expected files are produced by `download.sh` under:
    /// data/raw/nppes/monthly/
    #[arg(long)]
    pub nppes_monthly_dir: Option<std::path::PathBuf>,

    /// Directory containing extracted weekly NPPES CSV bundles.
    ///
    /// Expected files are produced by `download.sh` under:
    /// data/raw/nppes/weekly/
    #[arg(long)]
    pub nppes_weekly_dir: Option<std::path::PathBuf>,

    /// Skip local NPPES bulk-file loading and use cache/API only.
    #[arg(long, default_value_t = false)]
    pub skip_nppes_bulk: bool,

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

    /// Upload NPI API reference parquet to Hugging Face (requires hf_token + hf_repo_id).
    #[arg(long, default_value_t = false)]
    pub hf_upload_npi_reference: bool,

    /// Upload HCPCS API reference parquet to Hugging Face (requires hf_token + hf_repo_id).
    #[arg(long, default_value_t = false)]
    pub hf_upload_hcpcs_reference: bool,

    /// Upload enriched dataset to Hugging Face (requires hf_token + hf_repo_id).
    #[arg(long, default_value_t = false)]
    pub hf_upload_enriched: bool,

    /// Destination path for NPI mapping file in Hugging Face repo.
    #[arg(long)]
    pub hf_mapping_path_in_repo: Option<String>,

    /// Destination path for HCPCS mapping file in Hugging Face repo.
    #[arg(long)]
    pub hf_hcpcs_mapping_path_in_repo: Option<String>,

    /// Destination path for NPI API reference parquet in Hugging Face repo.
    #[arg(long)]
    pub hf_npi_reference_path_in_repo: Option<String>,

    /// Destination path for HCPCS API reference parquet in Hugging Face repo.
    #[arg(long)]
    pub hf_hcpcs_reference_path_in_repo: Option<String>,

    /// Destination path for enriched file in Hugging Face repo.
    #[arg(long)]
    pub hf_enriched_path_in_repo: Option<String>,
}
