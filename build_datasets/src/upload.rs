use anyhow::{Context, Result, bail};
use std::{path::Path, process::Command};

use crate::args::Args;

pub fn maybe_upload_outputs(
    args: &Args,
    npi_mapping_csv: &Path,
    hcpcs_mapping_csv: &Path,
    npi_api_responses_parquet: &Path,
    hcpcs_api_responses_parquet: &Path,
) -> Result<()> {
    if !args.hf_upload_mapping
        && !args.hf_upload_hcpcs_mapping
        && !args.hf_upload_npi
        && !args.hf_upload_hcpcs
    {
        return Ok(());
    }

    let token = args
        .hf_token
        .as_deref()
        .context("HF upload requested but --hf-token was not provided")?;
    let repo_id = args
        .hf_repo_id
        .as_deref()
        .context("HF upload requested but --hf-repo-id was not provided")?;

    if args.hf_upload_mapping {
        if !npi_mapping_csv.exists() {
            bail!(
                "NPI mapping upload requested but file does not exist: {}",
                npi_mapping_csv.display()
            );
        }
        let path_in_repo = args
            .hf_mapping_path_in_repo
            .clone()
            .unwrap_or(file_name_for_repo(npi_mapping_csv)?);
        upload_file_to_hf(
            npi_mapping_csv,
            &path_in_repo,
            repo_id,
            &args.hf_repo_type,
            token,
        )?;
    }

    if args.hf_upload_hcpcs_mapping {
        if !hcpcs_mapping_csv.exists() {
            bail!(
                "HCPCS mapping upload requested but file does not exist: {}",
                hcpcs_mapping_csv.display()
            );
        }
        let path_in_repo = args
            .hf_hcpcs_mapping_path_in_repo
            .clone()
            .unwrap_or(file_name_for_repo(hcpcs_mapping_csv)?);
        upload_file_to_hf(
            hcpcs_mapping_csv,
            &path_in_repo,
            repo_id,
            &args.hf_repo_type,
            token,
        )?;
    }

    if args.hf_upload_npi {
        if !npi_api_responses_parquet.exists() {
            bail!(
                "NPI resolved identifier parquet upload requested but file does not exist: {}",
                npi_api_responses_parquet.display()
            );
        }
        let path_in_repo = args
            .hf_npi_path_in_repo
            .clone()
            .unwrap_or(file_name_for_repo(npi_api_responses_parquet)?);
        upload_file_to_hf(
            npi_api_responses_parquet,
            &path_in_repo,
            repo_id,
            &args.hf_repo_type,
            token,
        )?;
    }

    if args.hf_upload_hcpcs {
        if !hcpcs_api_responses_parquet.exists() {
            bail!(
                "HCPCS resolved identifier parquet upload requested but file does not exist: {}",
                hcpcs_api_responses_parquet.display()
            );
        }
        let path_in_repo = args
            .hf_hcpcs_path_in_repo
            .clone()
            .unwrap_or(file_name_for_repo(hcpcs_api_responses_parquet)?);
        upload_file_to_hf(
            hcpcs_api_responses_parquet,
            &path_in_repo,
            repo_id,
            &args.hf_repo_type,
            token,
        )?;
    }

    Ok(())
}

fn file_name_for_repo(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|x| x.to_str())
        .map(|x| x.to_string())
        .context("Could not derive filename for repo path")
}

fn upload_file_to_hf(
    local_file: &Path,
    path_in_repo: &str,
    repo_id: &str,
    repo_type: &str,
    token: &str,
) -> Result<()> {
    println!(
        "Uploading {} -> hf://{}/{} ({})",
        local_file.display(),
        repo_id,
        path_in_repo,
        repo_type
    );

    let python = r#"
import os
import sys

try:
    from huggingface_hub import HfApi
except ImportError:
    print("huggingface_hub is required. Install with: pip install huggingface_hub", file=sys.stderr)
    sys.exit(1)

api = HfApi(token=os.environ["HF_TOKEN"])
api.upload_file(
    path_or_fileobj=os.environ["HF_LOCAL_FILE"],
    path_in_repo=os.environ["HF_PATH_IN_REPO"],
    repo_id=os.environ["HF_REPO_ID"],
    repo_type=os.environ["HF_REPO_TYPE"],
)
print("Upload complete.")
"#;

    let status = Command::new("python3")
        .arg("-c")
        .arg(python)
        .env("HF_TOKEN", token)
        .env("HF_LOCAL_FILE", local_file.to_string_lossy().to_string())
        .env("HF_PATH_IN_REPO", path_in_repo)
        .env("HF_REPO_ID", repo_id)
        .env("HF_REPO_TYPE", repo_type)
        .status()
        .context("Failed starting python3 for Hugging Face upload")?;

    if !status.success() {
        bail!("Hugging Face upload failed for {}", local_file.display());
    }
    Ok(())
}
