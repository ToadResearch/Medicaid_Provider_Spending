mod args;
mod common;
mod constants;
mod enrich;
mod hcpcs;
mod npi;
mod upload;

use anyhow::{Context, Result};
use clap::Parser;
use reqwest::Client;
use std::fs;

use args::Args;
use common::{
    default_enriched_output_path, delete_if_exists, download_file, file_name_from_url, project_root,
};
use constants::{
    HCPCS_API_DOC_URL, HCPCS_API_FAQ_URL, NPPES_API_DOC_URL, NPPES_RATE_LIMIT_NOTICE_URL,
};
use enrich::enrich_dataset;
use hcpcs::build_hcpcs_mapping;
use npi::build_npi_mapping;
use upload::maybe_upload_outputs;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("NPI API references:");
    println!("  - {}", NPPES_RATE_LIMIT_NOTICE_URL);
    println!("  - {}", NPPES_API_DOC_URL);
    println!("HCPCS API references:");
    println!("  - {}", HCPCS_API_DOC_URL);
    println!("  - {}", HCPCS_API_FAQ_URL);

    let project_dir = project_root();
    let data_dir = project_dir.join("data");
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("Failed creating data directory {}", data_dir.display()))?;

    let default_input_path = data_dir.join(file_name_from_url(&args.input_url)?);
    let input_path = args.input_path.clone().unwrap_or(default_input_path);

    let npi_mapping_csv = args
        .mapping_csv
        .clone()
        .unwrap_or_else(|| data_dir.join("npi_provider_mapping.csv"));
    let npi_cache_db = args
        .cache_db
        .clone()
        .unwrap_or_else(|| data_dir.join("npi_provider_cache.sqlite"));

    let hcpcs_mapping_csv = args
        .hcpcs_mapping_csv
        .clone()
        .unwrap_or_else(|| data_dir.join("hcpcs_code_mapping.csv"));
    let hcpcs_cache_db = args
        .hcpcs_cache_db
        .clone()
        .unwrap_or_else(|| data_dir.join("hcpcs_code_cache.sqlite"));

    let output_path = args
        .output_path
        .clone()
        .unwrap_or_else(|| default_enriched_output_path(&input_path, &data_dir));

    let client = Client::builder()
        .user_agent("medicaid-provider-spending-enricher/0.3")
        .build()
        .context("Failed creating HTTP client")?;

    if args.reset_map {
        delete_if_exists(&npi_mapping_csv)?;
        delete_if_exists(&npi_cache_db)?;
        delete_if_exists(&hcpcs_mapping_csv)?;
        delete_if_exists(&hcpcs_cache_db)?;
        println!("Reset mapping state (deleted NPI + HCPCS mappings and cache DBs).");
    }

    if !input_path.exists() {
        println!(
            "Input file missing at {}. Downloading from {}",
            input_path.display(),
            args.input_url
        );
        download_file(&client, &args.input_url, &input_path).await?;
    } else {
        println!("Using input file {}", input_path.display());
    }

    let should_build_npi_map = args.reset_map || args.rebuild_map || !npi_mapping_csv.exists();
    let should_build_hcpcs_map = args.reset_map || args.rebuild_map || !hcpcs_mapping_csv.exists();

    if should_build_npi_map {
        build_npi_mapping(&args, &client, &input_path, &npi_cache_db, &npi_mapping_csv).await?;
    } else {
        println!(
            "NPI mapping exists at {}. Skipping NPI build (pass --rebuild-map or --reset-map to rebuild).",
            npi_mapping_csv.display()
        );
    }

    if should_build_hcpcs_map {
        build_hcpcs_mapping(
            &args,
            &client,
            &input_path,
            &hcpcs_cache_db,
            &hcpcs_mapping_csv,
        )
        .await?;
    } else {
        println!(
            "HCPCS mapping exists at {}. Skipping HCPCS build (pass --rebuild-map or --reset-map to rebuild).",
            hcpcs_mapping_csv.display()
        );
    }

    if !args.build_map_only {
        enrich_dataset(
            &input_path,
            &output_path,
            &npi_mapping_csv,
            &hcpcs_mapping_csv,
        )?;
        println!("Wrote enriched dataset {}", output_path.display());
    } else {
        println!("--build-map-only set; skipping enrichment.");
    }

    maybe_upload_outputs(&args, &npi_mapping_csv, &hcpcs_mapping_csv, &output_path)?;
    Ok(())
}
