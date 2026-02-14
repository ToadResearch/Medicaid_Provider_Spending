mod args;
mod common;
mod constants;
mod hcpcs;
mod npi;
mod triage;
mod upload;

use anyhow::{Context, Result};
use clap::Parser;
use csv::Writer;
use indicatif::MultiProgress;
use reqwest::Client;
use std::{
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use args::Args;
use common::{
    delete_if_exists, download_file, file_name_from_url, install_ctrlc_handler, new_api_run_id,
    project_root,
};
use hcpcs::{
    backfill_hcpcs_api_responses_from_legacy_parquet, build_hcpcs_mapping,
    collect_unresolved_hcpcs, export_hcpcs_api_responses_parquet, is_hcpcs_dataset_complete,
};
use npi::{
    backfill_npi_api_responses_from_legacy_parquet, build_npi_mapping, collect_unresolved_npis,
    export_npi_api_responses_parquet, is_npi_dataset_complete,
};
use triage::write_unresolved_identifier_triage;
use upload::maybe_upload_outputs;

fn write_unresolved_identifiers_report(
    input_path: &Path,
    npi_cache_db: &Path,
    hcpcs_cache_db: &Path,
    output_csv: &Path,
) -> Result<()> {
    if let Some(parent) = output_csv.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed creating {}", parent.display()))?;
    }

    let npis = collect_unresolved_npis(input_path, npi_cache_db)?;
    let hcpcs = collect_unresolved_hcpcs(input_path, hcpcs_cache_db)?;

    let file_name = output_csv
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("unresolved_identifiers.csv");
    let tmp_path = output_csv.with_file_name(format!("{file_name}.tmp"));

    let mut writer = Writer::from_path(&tmp_path)
        .with_context(|| format!("Failed creating unresolved report {}", tmp_path.display()))?;
    writer
        .write_record([
            "identifier_type",
            "identifier",
            "status",
            "error_message",
            "fetched_at_unix",
        ])
        .context("Failed writing unresolved report header")?;

    for item in npis {
        let fetched_at = item
            .fetched_at_unix
            .map(|v| v.to_string())
            .unwrap_or_default();
        writer
            .write_record([
                "npi",
                item.npi.as_str(),
                item.status.as_str(),
                item.error_message.as_deref().unwrap_or(""),
                fetched_at.as_str(),
            ])
            .context("Failed writing unresolved NPI record")?;
    }

    for item in hcpcs {
        let fetched_at = item
            .fetched_at_unix
            .map(|v| v.to_string())
            .unwrap_or_default();
        writer
            .write_record([
                "hcpcs",
                item.hcpcs_code.as_str(),
                item.status.as_str(),
                item.error_message.as_deref().unwrap_or(""),
                fetched_at.as_str(),
            ])
            .context("Failed writing unresolved HCPCS record")?;
    }

    writer
        .flush()
        .context("Failed flushing unresolved report writer")?;
    fs::rename(&tmp_path, output_csv).with_context(|| {
        format!(
            "Failed moving unresolved report {} to {}",
            tmp_path.display(),
            output_csv.display()
        )
    })?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let project_dir = project_root();
    let data_dir = project_dir.join("data");
    let raw_dir = data_dir.join("raw");
    let raw_medicaid_dir = raw_dir.join("medicaid");
    let raw_nppes_dir = raw_dir.join("nppes");
    let nppes_monthly_dir = args
        .nppes_monthly_dir
        .clone()
        .unwrap_or_else(|| raw_nppes_dir.join("monthly"));
    let nppes_weekly_dir = args
        .nppes_weekly_dir
        .clone()
        .unwrap_or_else(|| raw_nppes_dir.join("weekly"));
    let mappings_dir = data_dir.join("mappings");
    let cache_dir = data_dir.join("cache");
    let output_dir = data_dir.join("output");

    fs::create_dir_all(&raw_medicaid_dir)
        .with_context(|| format!("Failed creating {}", raw_medicaid_dir.display()))?;
    fs::create_dir_all(&nppes_monthly_dir)
        .with_context(|| format!("Failed creating {}", nppes_monthly_dir.display()))?;
    fs::create_dir_all(&nppes_weekly_dir)
        .with_context(|| format!("Failed creating {}", nppes_weekly_dir.display()))?;
    fs::create_dir_all(mappings_dir.join("npi"))
        .with_context(|| format!("Failed creating {}", mappings_dir.join("npi").display()))?;
    fs::create_dir_all(mappings_dir.join("hcpcs"))
        .with_context(|| format!("Failed creating {}", mappings_dir.join("hcpcs").display()))?;
    fs::create_dir_all(cache_dir.join("npi"))
        .with_context(|| format!("Failed creating {}", cache_dir.join("npi").display()))?;
    fs::create_dir_all(cache_dir.join("hcpcs"))
        .with_context(|| format!("Failed creating {}", cache_dir.join("hcpcs").display()))?;
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("Failed creating {}", output_dir.display()))?;

    let default_input_path = raw_medicaid_dir.join(file_name_from_url(&args.input_url)?);
    let input_path = args.input_path.clone().unwrap_or(default_input_path);

    let npi_mapping_csv = args
        .mapping_csv
        .clone()
        .unwrap_or_else(|| mappings_dir.join("npi").join("npi_provider_mapping.csv"));
    let npi_cache_db = args
        .cache_db
        .clone()
        .unwrap_or_else(|| cache_dir.join("npi").join("npi_provider_cache.sqlite"));

    let hcpcs_mapping_csv = args
        .hcpcs_mapping_csv
        .clone()
        .unwrap_or_else(|| mappings_dir.join("hcpcs").join("hcpcs_code_mapping.csv"));
    let hcpcs_cache_db = args
        .hcpcs_cache_db
        .clone()
        .unwrap_or_else(|| cache_dir.join("hcpcs").join("hcpcs_code_cache.sqlite"));
    let npi_api_responses_parquet = args
        .npi_api_responses_parquet
        .clone()
        .unwrap_or_else(|| output_dir.join("npi.parquet"));
    let hcpcs_api_responses_parquet = args
        .hcpcs_api_responses_parquet
        .clone()
        .unwrap_or_else(|| output_dir.join("hcpcs.parquet"));
    let hcpcs_fallback_csv = args
        .hcpcs_fallback_csv
        .clone()
        .unwrap_or_else(|| raw_dir.join("cpt").join("cpt_hcpcs_fallback.csv"));

    let unresolved_report_csv = args
        .unresolved_report_csv
        .clone()
        .unwrap_or_else(|| data_dir.join("unresolved_identifiers.csv"));
    let api_run_id = new_api_run_id();

    let client = Client::builder()
        .user_agent("medicaid-provider-spending-mappings/0.4")
        .build()
        .context("Failed creating HTTP client")?;

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    install_ctrlc_handler(Arc::clone(&shutdown_requested));

    if args.reset_map {
        delete_if_exists(&npi_mapping_csv)?;
        delete_if_exists(&npi_cache_db)?;
        delete_if_exists(&hcpcs_mapping_csv)?;
        delete_if_exists(&hcpcs_cache_db)?;
        delete_if_exists(&npi_api_responses_parquet)?;
        delete_if_exists(&hcpcs_api_responses_parquet)?;
        // Backwards-compat cleanup: older runs wrote under data/reference/**.
        let legacy_reference_dir = data_dir.join("reference");
        delete_if_exists(
            &legacy_reference_dir
                .join("npi")
                .join("npi_api_reference.parquet"),
        )?;
        delete_if_exists(
            &legacy_reference_dir
                .join("hcpcs")
                .join("hcpcs_api_reference.parquet"),
        )?;
        delete_if_exists(&unresolved_report_csv)?;
        println!(
            "Reset mapping state (deleted NPI + HCPCS mappings, cache DBs, and API response datasets)."
        );
    }

    if !args.reset_map {
        // Migration path: older runs wrote append-only API request logs under data/reference/**.
        // If present, import them into the new cache-backed, deduped API response tables so that
        // `data/output/{npi,hcpcs}.parquet` can be generated without re-querying the APIs.
        let legacy_reference_dir = data_dir.join("reference");
        let legacy_npi_parquet = legacy_reference_dir
            .join("npi")
            .join("npi_api_reference.parquet");
        if legacy_npi_parquet.exists() {
            match backfill_npi_api_responses_from_legacy_parquet(&npi_cache_db, &legacy_npi_parquet)
            {
                Ok(imported) if imported > 0 => println!(
                    "Imported {} NPI API response rows from legacy parquet {}",
                    imported,
                    legacy_npi_parquet.display()
                ),
                Ok(_) => {}
                Err(err) => println!(
                    "Warning: failed importing legacy NPI API response parquet {}: {err}",
                    legacy_npi_parquet.display()
                ),
            }
        }

        let legacy_hcpcs_parquet = legacy_reference_dir
            .join("hcpcs")
            .join("hcpcs_api_reference.parquet");
        if legacy_hcpcs_parquet.exists() {
            match backfill_hcpcs_api_responses_from_legacy_parquet(
                &hcpcs_cache_db,
                &legacy_hcpcs_parquet,
            ) {
                Ok(imported) if imported > 0 => println!(
                    "Imported {} HCPCS API response rows from legacy parquet {}",
                    imported,
                    legacy_hcpcs_parquet.display()
                ),
                Ok(_) => {}
                Err(err) => println!(
                    "Warning: failed importing legacy HCPCS API response parquet {}: {err}",
                    legacy_hcpcs_parquet.display()
                ),
            }
        }
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

    let npi_dataset_done = if args.reset_map || args.rebuild_map {
        false
    } else {
        is_npi_dataset_complete(&input_path, &npi_cache_db, &npi_mapping_csv)?
    };
    let hcpcs_dataset_done = if args.reset_map || args.rebuild_map {
        false
    } else {
        is_hcpcs_dataset_complete(
            &input_path,
            &hcpcs_cache_db,
            &hcpcs_mapping_csv,
            &hcpcs_fallback_csv,
        )?
    };
    let should_build_npi_map = args.reset_map || args.rebuild_map || !npi_dataset_done;
    let should_build_hcpcs_map = args.reset_map || args.rebuild_map || !hcpcs_dataset_done;

    let mut interrupted = false;
    match (should_build_npi_map, should_build_hcpcs_map) {
        (true, true) => {
            println!("Building NPI and HCPCS mappings in parallel...");
            let progress_hub = Arc::new(MultiProgress::new());
            let (npi_interrupted, hcpcs_interrupted) = tokio::try_join!(
                build_npi_mapping(
                    &args,
                    &client,
                    &input_path,
                    &npi_cache_db,
                    &npi_mapping_csv,
                    &npi_api_responses_parquet,
                    &api_run_id,
                    Some(Arc::clone(&progress_hub)),
                    Arc::clone(&shutdown_requested),
                    &nppes_monthly_dir,
                    &nppes_weekly_dir,
                ),
                build_hcpcs_mapping(
                    &args,
                    &client,
                    &input_path,
                    &hcpcs_cache_db,
                    &hcpcs_mapping_csv,
                    &hcpcs_api_responses_parquet,
                    &hcpcs_fallback_csv,
                    &api_run_id,
                    Some(Arc::clone(&progress_hub)),
                    Arc::clone(&shutdown_requested),
                ),
            )?;
            interrupted = npi_interrupted || hcpcs_interrupted;
        }
        (true, false) => {
            interrupted = build_npi_mapping(
                &args,
                &client,
                &input_path,
                &npi_cache_db,
                &npi_mapping_csv,
                &npi_api_responses_parquet,
                &api_run_id,
                None,
                Arc::clone(&shutdown_requested),
                &nppes_monthly_dir,
                &nppes_weekly_dir,
            )
            .await?;
            println!(
                "HCPCS dataset already built (mapping: {}, api responses: {}). Skipping HCPCS build (cache coverage is complete, including local fallback where applicable; pass --rebuild-map or --reset-map to rebuild).",
                hcpcs_mapping_csv.display(),
                hcpcs_api_responses_parquet.display()
            );
        }
        (false, true) => {
            println!(
                "NPI dataset already built (mapping: {}, api responses: {}). Skipping NPI build (pass --rebuild-map or --reset-map to rebuild).",
                npi_mapping_csv.display(),
                npi_api_responses_parquet.display()
            );
            interrupted = build_hcpcs_mapping(
                &args,
                &client,
                &input_path,
                &hcpcs_cache_db,
                &hcpcs_mapping_csv,
                &hcpcs_api_responses_parquet,
                &hcpcs_fallback_csv,
                &api_run_id,
                None,
                Arc::clone(&shutdown_requested),
            )
            .await?;
        }
        (false, false) => {
            println!(
                "NPI dataset already built (mapping: {}, api responses: {}). Skipping NPI build (pass --rebuild-map or --reset-map to rebuild).",
                npi_mapping_csv.display(),
                npi_api_responses_parquet.display()
            );
            println!(
                "HCPCS dataset already built (mapping: {}, api responses: {}). Skipping HCPCS build (cache coverage is complete, including local fallback where applicable; pass --rebuild-map or --reset-map to rebuild).",
                hcpcs_mapping_csv.display(),
                hcpcs_api_responses_parquet.display()
            );
        }
    }

    if !should_build_npi_map {
        export_npi_api_responses_parquet(&npi_cache_db, &npi_api_responses_parquet)?;
        println!(
            "Wrote NPI API responses dataset {}",
            npi_api_responses_parquet.display()
        );
    }
    if !should_build_hcpcs_map {
        export_hcpcs_api_responses_parquet(&hcpcs_cache_db, &hcpcs_api_responses_parquet)?;
        println!(
            "Wrote HCPCS API responses dataset {}",
            hcpcs_api_responses_parquet.display()
        );
    }

    if interrupted || shutdown_requested.load(Ordering::SeqCst) {
        write_unresolved_identifiers_report(
            &input_path,
            &npi_cache_db,
            &hcpcs_cache_db,
            &unresolved_report_csv,
        )?;
        println!(
            "Wrote unresolved identifiers report {}",
            unresolved_report_csv.display()
        );
        let triage_dir = output_dir.join("triage");
        match write_unresolved_identifier_triage(&unresolved_report_csv, &triage_dir) {
            Ok(summary) => println!(
                "Wrote unresolved identifier triage outputs {} (hcpcs_rows={} hcpcs_needs_review={} npi_rows={} npi_needs_review={})",
                triage_dir.display(),
                summary.hcpcs_rows,
                summary.hcpcs_needs_review_rows,
                summary.npi_rows,
                summary.npi_needs_review_rows
            ),
            Err(err) => println!(
                "Warning: failed writing unresolved identifier triage outputs {}: {err}",
                triage_dir.display()
            ),
        }
        println!("Graceful shutdown complete. Progress saved; skipping uploads.");
        return Ok(());
    }

    maybe_upload_outputs(
        &args,
        &npi_mapping_csv,
        &hcpcs_mapping_csv,
        &npi_api_responses_parquet,
        &hcpcs_api_responses_parquet,
    )?;

    write_unresolved_identifiers_report(
        &input_path,
        &npi_cache_db,
        &hcpcs_cache_db,
        &unresolved_report_csv,
    )?;
    println!(
        "Wrote unresolved identifiers report {}",
        unresolved_report_csv.display()
    );
    let triage_dir = output_dir.join("triage");
    match write_unresolved_identifier_triage(&unresolved_report_csv, &triage_dir) {
        Ok(summary) => println!(
            "Wrote unresolved identifier triage outputs {} (hcpcs_rows={} hcpcs_needs_review={} npi_rows={} npi_needs_review={})",
            triage_dir.display(),
            summary.hcpcs_rows,
            summary.hcpcs_needs_review_rows,
            summary.npi_rows,
            summary.npi_needs_review_rows
        ),
        Err(err) => println!(
            "Warning: failed writing unresolved identifier triage outputs {}: {err}",
            triage_dir.display()
        ),
    }
    Ok(())
}
