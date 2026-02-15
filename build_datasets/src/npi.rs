use anyhow::{Context, Result};
use csv::Writer;
use duckdb::Connection;
use futures::{StreamExt, stream::FuturesUnordered};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{Client, header::RETRY_AFTER};
use rusqlite::{Connection as SqliteConnection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::IsTerminal,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

use crate::{
    args::Args,
    common::{
        is_retryable_status, now_unix_seconds, parse_retry_after, source_expr, sql_escape_path,
        truncate_for_log, wait_for_rate_slot,
    },
    parquet_writer::StringParquetWriter,
};

struct NpiCache {
    conn: SqliteConnection,
}

#[derive(Debug, Clone)]
pub struct UnresolvedNpiEntry {
    pub npi: String,
    pub status: String,
    pub error_message: Option<String>,
    pub fetched_at_unix: Option<i64>,
}

impl NpiCache {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed creating cache dir {}", parent.display()))?;
        }
        let conn = SqliteConnection::open(path)
            .with_context(|| format!("Failed opening cache DB {}", path.display()))?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS npi_cache (
                npi TEXT PRIMARY KEY,
                provider_name TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                fetched_at_unix INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_npi_cache_status ON npi_cache(status);
            CREATE TABLE IF NOT EXISTS npi_api_responses (
                npi TEXT PRIMARY KEY,
                basic_json TEXT,
                addresses_json TEXT,
                practice_locations_json TEXT,
                taxonomies_json TEXT,
                identifiers_json TEXT,
                other_names_json TEXT,
                endpoints_json TEXT,
                url TEXT,
                error_message TEXT,
                api_run_id TEXT,
                requested_at_utc TEXT,
                request_params_json TEXT,
                results_json TEXT,
                response_json_raw TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_npi_api_responses_requested_at
                ON npi_api_responses(requested_at_utc);
            ",
        )
        .context("Failed initializing NPI cache schema")?;
        Ok(Self { conn })
    }

    fn classify_for_lookup(&self, npis: &[String]) -> Result<(usize, Vec<String>)> {
        let mut stmt = self
            .conn
            .prepare("SELECT status FROM npi_cache WHERE npi = ?1")
            .context("Failed preparing NPI cache lookup statement")?;

        let mut resolved = 0usize;
        let mut missing = Vec::new();

        for npi in npis {
            let status: Option<String> = stmt
                .query_row([npi], |row| row.get(0))
                .optional()
                .with_context(|| format!("Failed NPI cache lookup for {npi}"))?;

            match status.as_deref() {
                Some("ok") | Some("not_found") => resolved += 1,
                Some(_) | None => missing.push(npi.clone()),
            }
        }

        Ok((resolved, missing))
    }

    fn upsert_ok(&self, npi: &str, provider_name: &str) -> Result<()> {
        self.upsert(npi, Some(provider_name), "ok", None)
    }

    fn upsert_not_found(&self, npi: &str) -> Result<()> {
        self.upsert(npi, None, "not_found", None)
    }

    fn upsert_error(&self, npi: &str, message: &str) -> Result<()> {
        self.upsert(npi, None, "error", Some(message))
    }

    fn upsert(
        &self,
        npi: &str,
        provider_name: Option<&str>,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                "
                INSERT INTO npi_cache (npi, provider_name, status, error_message, fetched_at_unix)
                VALUES (?1, ?2, ?3, ?4, strftime('%s', 'now'))
                ON CONFLICT(npi) DO UPDATE SET
                    provider_name = excluded.provider_name,
                    status = excluded.status,
                    error_message = excluded.error_message,
                    fetched_at_unix = excluded.fetched_at_unix
                ",
                params![npi, provider_name, status, error_message],
            )
            .with_context(|| format!("Failed updating NPI cache for {npi}"))?;
        Ok(())
    }

    fn upsert_api_responses(&mut self, rows: &[NpiApiReferenceRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let tx = self
            .conn
            .transaction()
            .context("Failed starting NPI API responses transaction")?;
        let mut stmt = tx
            .prepare(
                "
                INSERT INTO npi_api_responses (
                    npi,
                    basic_json,
                    addresses_json,
                    practice_locations_json,
                    taxonomies_json,
                    identifiers_json,
                    other_names_json,
                    endpoints_json,
                    url,
                    error_message,
                    api_run_id,
                    requested_at_utc,
                    request_params_json,
                    results_json,
                    response_json_raw
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
                )
                ON CONFLICT(npi) DO UPDATE SET
                    basic_json = excluded.basic_json,
                    addresses_json = excluded.addresses_json,
                    practice_locations_json = excluded.practice_locations_json,
                    taxonomies_json = excluded.taxonomies_json,
                    identifiers_json = excluded.identifiers_json,
                    other_names_json = excluded.other_names_json,
                    endpoints_json = excluded.endpoints_json,
                    url = excluded.url,
                    error_message = excluded.error_message,
                    api_run_id = excluded.api_run_id,
                    requested_at_utc = excluded.requested_at_utc,
                    request_params_json = excluded.request_params_json,
                    results_json = excluded.results_json,
                    response_json_raw = excluded.response_json_raw
                WHERE excluded.requested_at_utc > npi_api_responses.requested_at_utc
                   OR npi_api_responses.requested_at_utc IS NULL
                ",
            )
            .context("Failed preparing NPI API responses upsert statement")?;
        for row in rows {
            stmt.execute(params![
                row.npi.as_str(),
                row.basic_json.as_deref(),
                row.addresses_json.as_deref(),
                row.practice_locations_json.as_deref(),
                row.taxonomies_json.as_deref(),
                row.identifiers_json.as_deref(),
                row.other_names_json.as_deref(),
                row.endpoints_json.as_deref(),
                row.request_url.as_str(),
                row.error_message.as_deref(),
                row.api_run_id.as_str(),
                row.requested_at_utc.as_str(),
                row.request_params_json.as_str(),
                row.results_json.as_deref(),
                row.response_json_raw.as_deref(),
            ])
            .with_context(|| format!("Failed upserting NPI API response row for {}", row.npi))?;
        }
        drop(stmt);
        tx.commit()
            .context("Failed committing NPI API responses transaction")?;
        Ok(())
    }

    fn export_mapping_csv(&self, output_path: &Path) -> Result<()> {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed creating NPI mapping parent directory {}",
                    parent.display()
                )
            })?;
        }

        let file_name = output_path
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("npi_provider_mapping.csv");
        let tmp_path = output_path.with_file_name(format!("{file_name}.tmp"));

        let mut writer = Writer::from_path(&tmp_path).with_context(|| {
            format!(
                "Failed creating temp NPI mapping CSV {}",
                tmp_path.display()
            )
        })?;
        writer
            .write_record(["npi", "provider_name", "status", "fetched_at_unix"])
            .context("Failed writing NPI mapping CSV header")?;

        let mut stmt = self
            .conn
            .prepare(
                "
                SELECT npi, COALESCE(provider_name, ''), status, fetched_at_unix
                FROM npi_cache
                WHERE status IN ('ok', 'not_found')
                ORDER BY npi
                ",
            )
            .context("Failed preparing NPI mapping export query")?;
        let mut rows = stmt.query([]).context("Failed querying NPI mapping rows")?;

        while let Some(row) = rows.next().context("Failed iterating NPI mapping rows")? {
            let npi: String = row.get(0).context("Failed reading npi")?;
            let provider_name: String = row.get(1).context("Failed reading provider_name")?;
            let status: String = row.get(2).context("Failed reading status")?;
            let fetched_at_unix: i64 = row.get(3).context("Failed reading fetched_at_unix")?;
            writer
                .write_record([npi, provider_name, status, fetched_at_unix.to_string()])
                .context("Failed writing NPI mapping row")?;
        }
        writer
            .flush()
            .context("Failed flushing NPI mapping CSV writer")?;

        fs::rename(&tmp_path, output_path).with_context(|| {
            format!(
                "Failed moving temp NPI mapping {} to {}",
                tmp_path.display(),
                output_path.display()
            )
        })?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct NpiApiResponse {
    #[serde(default)]
    results: Vec<NpiResult>,
}

#[derive(Debug, Deserialize)]
struct NpiResult {
    basic: Option<NpiBasic>,
}

#[derive(Debug, Deserialize)]
struct NpiBasic {
    organization_name: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
}

#[derive(Debug, Clone)]
struct NpiApiReferenceRow {
    npi: String,
    basic_json: Option<String>,
    addresses_json: Option<String>,
    practice_locations_json: Option<String>,
    taxonomies_json: Option<String>,
    identifiers_json: Option<String>,
    other_names_json: Option<String>,
    endpoints_json: Option<String>,
    request_url: String,
    http_status: Option<i64>,
    error_message: Option<String>,
    api_run_id: String,
    requested_at_utc: String,
    request_params_json: String,
    results_json: Option<String>,
    response_json_raw: Option<String>,
}

enum NpiResolveResult {
    Found {
        provider_name: String,
        reference_row: NpiApiReferenceRow,
    },
    NotFound {
        reference_row: NpiApiReferenceRow,
    },
    Error {
        error_message: String,
        reference_row: NpiApiReferenceRow,
    },
}

pub async fn build_npi_mapping(
    args: &Args,
    client: &Client,
    input_path: &Path,
    cache_db: &Path,
    mapping_csv: &Path,
    api_responses_parquet: &Path,
    api_run_id: &str,
    progress_hub: Option<Arc<MultiProgress>>,
    shutdown_requested: Arc<AtomicBool>,
    nppes_monthly_dir: &Path,
    nppes_weekly_dir: &Path,
) -> Result<bool> {
    println!("Extracting unique NPIs...");
    let unique_npis = extract_unique_npis(input_path)?;
    println!(
        "Discovered {} unique NPIs in source data.",
        unique_npis.len()
    );

    let mut cache = NpiCache::open(cache_db)?;
    let mut exporter =
        NpiResolvedParquetExporter::try_new(api_responses_parquet, &unique_npis, api_run_id)?;
    let (resolved_before_bulk, _) = cache.classify_for_lookup(&unique_npis)?;
    let mut monthly_loaded = 0usize;
    let mut weekly_loaded = 0usize;
    let mut used_monthly_file: Option<PathBuf> = None;
    let mut used_weekly_file: Option<PathBuf> = None;
    let mut bulk_sources: Vec<NppesBulkFiles> = Vec::new();

    if !args.skip_nppes_bulk {
        let monthly_primary = select_latest_nppes_csv(nppes_monthly_dir)?;
        let weekly_primary = select_latest_nppes_csv(nppes_weekly_dir)?;
        if monthly_primary.is_none() && weekly_primary.is_none() {
            println!(
                "No local NPPES bulk files found under {} and {}. Falling back to cache/API.",
                nppes_monthly_dir.display(),
                nppes_weekly_dir.display()
            );
        } else {
            if let Some(monthly) = monthly_primary.clone() {
                used_monthly_file = Some(monthly.clone());
                bulk_sources.push(NppesBulkFiles {
                    label: "monthly",
                    othername_csv: find_nppes_sibling_csv(&monthly, "othername_pfile_")?,
                    pl_csv: find_nppes_sibling_csv(&monthly, "pl_pfile_")?,
                    endpoint_csv: find_nppes_sibling_csv(&monthly, "endpoint_pfile_")?,
                    npidata_csv: monthly,
                });
            }
            if let Some(weekly) = weekly_primary.clone() {
                used_weekly_file = Some(weekly.clone());
                bulk_sources.push(NppesBulkFiles {
                    label: "weekly",
                    othername_csv: find_nppes_sibling_csv(&weekly, "othername_pfile_")?,
                    pl_csv: find_nppes_sibling_csv(&weekly, "pl_pfile_")?,
                    endpoint_csv: find_nppes_sibling_csv(&weekly, "endpoint_pfile_")?,
                    npidata_csv: weekly,
                });
            }

            println!("Loading local NPPES bulk files before API fallback...");
            exporter.load_supplemental_records(&bulk_sources, &shutdown_requested)?;
            if let Some(weekly_source) = bulk_sources.iter().find(|s| s.label == "weekly") {
                weekly_loaded = exporter.write_bulk_from_primary(
                    Some(&cache),
                    weekly_source,
                    &shutdown_requested,
                )?;
            }
            if let Some(monthly_source) = bulk_sources.iter().find(|s| s.label == "monthly") {
                monthly_loaded = exporter.write_bulk_from_primary(
                    Some(&cache),
                    monthly_source,
                    &shutdown_requested,
                )?;
            }
        }
    } else {
        println!("--skip-nppes-bulk set; skipping local NPPES bulk-file preload.");
    }

    let (resolved_count, mut missing_npis) = cache.classify_for_lookup(&unique_npis)?;
    let unresolved_before_limit = missing_npis.len();

    if let Some(limit) = args.max_new_lookups {
        if missing_npis.len() > limit {
            println!(
                "Applying --max-new-lookups={} to NPI lookups (from {}).",
                limit,
                missing_npis.len()
            );
            missing_npis.truncate(limit);
        }
    }
    let planned_api_lookups = if args.skip_api { 0 } else { missing_npis.len() };
    print_npi_download_plan_table(
        unique_npis.len(),
        resolved_before_bulk,
        resolved_count,
        monthly_loaded,
        weekly_loaded,
        unresolved_before_limit,
        planned_api_lookups,
        used_monthly_file.as_deref(),
        used_weekly_file.as_deref(),
    );

    let mut interrupted = shutdown_requested.load(Ordering::SeqCst);
    let mut api_reference_rows: Vec<NpiApiReferenceRow> = Vec::new();
    if interrupted {
        println!("Shutdown requested; skipping new NPI API lookups.");
    } else if args.skip_api {
        println!("--skip-api set; unresolved NPIs remain unresolved.");
    } else if !missing_npis.is_empty() {
        let (api_interrupted, rows) = resolve_missing_npis(
            &cache,
            missing_npis,
            client,
            args,
            api_run_id,
            progress_hub.clone(),
            Arc::clone(&shutdown_requested),
        )
        .await?;
        interrupted |= api_interrupted;
        api_reference_rows = rows;
    }

    cache.upsert_api_responses(&api_reference_rows)?;
    cache.export_mapping_csv(mapping_csv)?;
    println!("Wrote NPI mapping CSV {}", mapping_csv.display());
    exporter.write_remaining_from_api_responses(&cache, &shutdown_requested)?;
    if shutdown_requested.load(Ordering::SeqCst) {
        exporter.abort()?;
        println!(
            "Shutdown requested; aborted NPI resolved identifier parquet export (output not updated)."
        );
    } else {
        exporter.finish()?;
        println!(
            "Wrote NPI resolved identifier dataset {}",
            api_responses_parquet.display()
        );
    }
    Ok(interrupted || shutdown_requested.load(Ordering::SeqCst))
}

pub fn is_npi_dataset_complete(
    input_path: &Path,
    cache_db: &Path,
    mapping_csv: &Path,
) -> Result<bool> {
    if !cache_db.exists() || !mapping_csv.exists() {
        return Ok(false);
    }

    let unique_npis = extract_unique_npis(input_path)?;
    let cache = NpiCache::open(cache_db)?;
    let mut stmt = cache
        .conn
        .prepare(
            "SELECT 1 FROM npi_cache
             WHERE npi = ?1 AND status IN ('ok', 'not_found', 'error')
             LIMIT 1",
        )
        .context("Failed preparing NPI completeness query")?;

    for npi in unique_npis {
        let exists: Option<i64> = stmt
            .query_row([&npi], |row| row.get(0))
            .optional()
            .with_context(|| format!("Failed checking NPI cache status for {npi}"))?;
        if exists.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn export_npi_api_responses_parquet(
    input_path: &Path,
    cache_db: &Path,
    output_path: &Path,
    api_run_id: &str,
    shutdown_requested: &Arc<AtomicBool>,
    nppes_monthly_dir: &Path,
    nppes_weekly_dir: &Path,
    skip_nppes_bulk: bool,
) -> Result<()> {
    println!(
        "Regenerating NPI resolved identifier parquet {} (bulk NPPES + cached API responses)...",
        output_path.display()
    );

    let unique_npis = extract_unique_npis(input_path)?;

    let cache = NpiCache::open(cache_db)?;
    let mut exporter = NpiResolvedParquetExporter::try_new(output_path, &unique_npis, api_run_id)?;
    let mut bulk_sources: Vec<NppesBulkFiles> = Vec::new();

    if !skip_nppes_bulk {
        let monthly_primary = select_latest_nppes_csv(nppes_monthly_dir)?;
        let weekly_primary = select_latest_nppes_csv(nppes_weekly_dir)?;
        if let Some(monthly) = monthly_primary.clone() {
            bulk_sources.push(NppesBulkFiles {
                label: "monthly",
                othername_csv: find_nppes_sibling_csv(&monthly, "othername_pfile_")?,
                pl_csv: find_nppes_sibling_csv(&monthly, "pl_pfile_")?,
                endpoint_csv: find_nppes_sibling_csv(&monthly, "endpoint_pfile_")?,
                npidata_csv: monthly,
            });
        }
        if let Some(weekly) = weekly_primary.clone() {
            bulk_sources.push(NppesBulkFiles {
                label: "weekly",
                othername_csv: find_nppes_sibling_csv(&weekly, "othername_pfile_")?,
                pl_csv: find_nppes_sibling_csv(&weekly, "pl_pfile_")?,
                endpoint_csv: find_nppes_sibling_csv(&weekly, "endpoint_pfile_")?,
                npidata_csv: weekly,
            });
        }

        if bulk_sources.is_empty() {
            println!(
                "No local NPPES bulk files found under {} and {}. Export will use cached API rows only.",
                nppes_monthly_dir.display(),
                nppes_weekly_dir.display()
            );
        } else {
            exporter.load_supplemental_records(&bulk_sources, shutdown_requested)?;
            if let Some(weekly_source) = bulk_sources.iter().find(|s| s.label == "weekly") {
                let _ =
                    exporter.write_bulk_from_primary(None, weekly_source, shutdown_requested)?;
            }
            if let Some(monthly_source) = bulk_sources.iter().find(|s| s.label == "monthly") {
                let _ =
                    exporter.write_bulk_from_primary(None, monthly_source, shutdown_requested)?;
            }
        }
    } else {
        println!("--skip-nppes-bulk set; exporting from cached API rows only.");
    }

    exporter.write_remaining_from_api_responses(&cache, shutdown_requested)?;
    if shutdown_requested.load(Ordering::SeqCst) {
        exporter.abort()?;
        println!("Shutdown requested; aborted NPI parquet export (output not updated).");
    } else {
        exporter.finish()?;
        println!(
            "Wrote NPI resolved identifier dataset {}",
            output_path.display()
        );
    }
    Ok(())
}

pub fn backfill_npi_api_responses_from_legacy_parquet(
    cache_db: &Path,
    legacy_parquet: &Path,
) -> Result<usize> {
    if !legacy_parquet.exists() {
        return Ok(0);
    }

    let mut cache = NpiCache::open(cache_db)?;
    let existing: i64 = cache
        .conn
        .query_row("SELECT COUNT(*) FROM npi_api_responses", [], |row| {
            row.get(0)
        })
        .context("Failed checking NPI API responses row count")?;
    if existing > 0 {
        return Ok(0);
    }

    let conn = Connection::open_in_memory()
        .context("Failed opening DuckDB for NPI legacy parquet import")?;
    let legacy_escaped = sql_escape_path(legacy_parquet);
    let query = format!(
        "
        SELECT * EXCLUDE (rn) FROM (
            SELECT
                npi,
                basic_json,
                addresses_json,
                practice_locations_json,
                taxonomies_json,
                identifiers_json,
                other_names_json,
                endpoints_json,
                request_url,
                error_message,
                api_run_id,
                requested_at_utc,
                request_params_json,
                results_json,
                response_json_raw,
                row_number() OVER (PARTITION BY npi ORDER BY requested_at_utc DESC) AS rn
            FROM read_parquet('{legacy_escaped}')
        ) WHERE rn = 1
        "
    );

    let mut stmt = conn
        .prepare(&query)
        .context("Failed preparing DuckDB query for NPI legacy parquet import")?;
    let mut rows = stmt
        .query([])
        .context("Failed querying DuckDB for NPI legacy parquet import")?;

    let mut imported = Vec::new();
    while let Some(row) = rows
        .next()
        .context("Failed iterating DuckDB rows for NPI legacy parquet import")?
    {
        let npi: String = row.get(0).context("Failed reading npi")?;
        let basic_json: Option<String> = row.get(1).context("Failed reading basic_json")?;
        let addresses_json: Option<String> = row.get(2).context("Failed reading addresses_json")?;
        let practice_locations_json: Option<String> = row
            .get(3)
            .context("Failed reading practice_locations_json")?;
        let taxonomies_json: Option<String> =
            row.get(4).context("Failed reading taxonomies_json")?;
        let identifiers_json: Option<String> =
            row.get(5).context("Failed reading identifiers_json")?;
        let other_names_json: Option<String> =
            row.get(6).context("Failed reading other_names_json")?;
        let endpoints_json: Option<String> = row.get(7).context("Failed reading endpoints_json")?;
        let request_url: Option<String> = row.get(8).context("Failed reading request_url")?;
        let error_message: Option<String> = row.get(9).context("Failed reading error_message")?;
        let api_run_id: Option<String> = row.get(10).context("Failed reading api_run_id")?;
        let requested_at_utc: Option<String> =
            row.get(11).context("Failed reading requested_at_utc")?;
        let request_params_json: Option<String> =
            row.get(12).context("Failed reading request_params_json")?;
        let results_json: Option<String> = row.get(13).context("Failed reading results_json")?;
        let response_json_raw: Option<String> =
            row.get(14).context("Failed reading response_json_raw")?;

        imported.push(NpiApiReferenceRow {
            npi,
            basic_json,
            addresses_json,
            practice_locations_json,
            taxonomies_json,
            identifiers_json,
            other_names_json,
            endpoints_json,
            request_url: request_url.unwrap_or_default(),
            http_status: None,
            error_message,
            api_run_id: api_run_id.unwrap_or_default(),
            requested_at_utc: requested_at_utc.unwrap_or_default(),
            request_params_json: request_params_json.unwrap_or_default(),
            results_json,
            response_json_raw,
        });
    }

    cache.upsert_api_responses(&imported)?;
    Ok(imported.len())
}

pub fn collect_unresolved_npis(
    input_path: &Path,
    cache_db: &Path,
) -> Result<Vec<UnresolvedNpiEntry>> {
    let unique_npis = extract_unique_npis(input_path)?;
    let cache = NpiCache::open(cache_db)?;
    let mut stmt = cache
        .conn
        .prepare("SELECT status, error_message, fetched_at_unix FROM npi_cache WHERE npi = ?1")
        .context("Failed preparing unresolved NPI lookup statement")?;

    let mut unresolved = Vec::new();
    for npi in unique_npis {
        let row: Option<(String, Option<String>, i64)> = stmt
            .query_row([&npi], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .optional()
            .with_context(|| format!("Failed unresolved NPI lookup for {npi}"))?;

        match row {
            Some((status, _error_message, _fetched_at_unix)) if status == "ok" => {}
            Some((status, error_message, fetched_at_unix)) => unresolved.push(UnresolvedNpiEntry {
                npi,
                status,
                error_message: normalize_error_message(error_message),
                fetched_at_unix: Some(fetched_at_unix),
            }),
            None => unresolved.push(UnresolvedNpiEntry {
                npi,
                status: "missing_cache".to_string(),
                error_message: None,
                fetched_at_unix: None,
            }),
        }
    }
    unresolved.sort_by(|a, b| a.npi.cmp(&b.npi));
    Ok(unresolved)
}

fn normalize_error_message(value: Option<String>) -> Option<String> {
    value.and_then(|message| {
        let trimmed = message.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn extract_unique_npis(input_path: &Path) -> Result<Vec<String>> {
    let conn = Connection::open_in_memory().context("Failed opening DuckDB")?;
    let source = source_expr(input_path)?;
    let query = format!(
        "
        WITH src AS (
            SELECT * FROM {source}
        )
        SELECT DISTINCT TRIM(npi) AS npi
        FROM (
            SELECT CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi FROM src
            UNION ALL
            SELECT CAST(SERVICING_PROVIDER_NPI_NUM AS VARCHAR) AS npi FROM src
        ) AS combined
        WHERE npi IS NOT NULL AND TRIM(npi) <> ''
        "
    );

    let mut stmt = conn
        .prepare(&query)
        .context("Failed preparing unique NPI query")?;
    let rows = stmt
        .query_map([], |row| row.get::<usize, String>(0))
        .context("Failed running unique NPI query")?;

    let mut npis = Vec::new();
    for row in rows {
        npis.push(row.context("Failed reading NPI row")?);
    }
    Ok(npis)
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn print_npi_download_plan_table(
    dataset_unique_npis: usize,
    resolved_before_bulk: usize,
    resolved_after_bulk: usize,
    monthly_loaded: usize,
    weekly_loaded: usize,
    unresolved_before_limit: usize,
    planned_api_lookups: usize,
    monthly_file: Option<&Path>,
    weekly_file: Option<&Path>,
) {
    let use_color = std::io::stdout().is_terminal();
    let reset = if use_color { "\x1b[0m" } else { "" };
    let bold = if use_color { "\x1b[1m" } else { "" };
    let cyan = if use_color { "\x1b[36m" } else { "" };
    let green = if use_color { "\x1b[32m" } else { "" };
    let yellow = if use_color { "\x1b[33m" } else { "" };
    let magenta = if use_color { "\x1b[35m" } else { "" };

    let coverage_pct_after_bulk = if dataset_unique_npis == 0 {
        0.0
    } else {
        (resolved_after_bulk as f64 / dataset_unique_npis as f64) * 100.0
    };
    let found_via_bulk = resolved_after_bulk.saturating_sub(resolved_before_bulk);
    let bulk_rows_matched = monthly_loaded + weekly_loaded;
    let monthly_src = monthly_file
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "(none)".to_string());
    let weekly_src = weekly_file
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "(none)".to_string());

    let border = "+--------------------------------------------+--------------------------+";
    let section = "| NPI API PRE-DOWNLOAD SUMMARY               |                          |";

    println!();
    println!("{bold}{cyan}{border}{reset}");
    println!("{bold}{cyan}{section}{reset}");
    println!("{bold}{cyan}{border}{reset}");
    println!(
        "| {:<42} | {:<24} |",
        "Unique NPIs in dataset",
        format_count(dataset_unique_npis)
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Already saved in cache",
        green,
        format_count(resolved_before_bulk),
        reset
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Found via NPPES bulk this run",
        green,
        format_count(found_via_bulk),
        reset
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Total resolved before API",
        green,
        format!(
            "{} ({:.2}%)",
            format_count(resolved_after_bulk),
            coverage_pct_after_bulk
        ),
        reset
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Still unresolved",
        yellow,
        format_count(unresolved_before_limit),
        reset
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Planned API downloads now",
        magenta,
        format_count(planned_api_lookups),
        reset
    );
    println!(
        "| {:<42} | {:<24} |",
        "Bulk rows matched (monthly + weekly)",
        format_count(bulk_rows_matched)
    );
    println!(
        "| {:<42} | {:<24} |",
        "Monthly source matched rows",
        format_count(monthly_loaded)
    );
    println!(
        "| {:<42} | {:<24} |",
        "Weekly source matched rows",
        format_count(weekly_loaded)
    );
    println!("{bold}{cyan}{border}{reset}");
    println!("  monthly source: {}", monthly_src);
    println!("  weekly source:  {}", weekly_src);
    println!();
}

fn json_to_string_opt(value: Option<&Value>) -> Option<String> {
    value
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::to_string(v).ok())
}

fn build_npi_reference_row_from_value(
    response_value: &Value,
    npi: &str,
    request_url: &str,
    http_status: i64,
    api_run_id: &str,
    requested_at_utc: &str,
    request_params_json: &str,
) -> NpiApiReferenceRow {
    let results = response_value.get("results").and_then(Value::as_array);
    let first_result = results.and_then(|values| values.first());

    NpiApiReferenceRow {
        npi: npi.to_string(),
        basic_json: json_to_string_opt(first_result.and_then(|v| v.get("basic"))),
        addresses_json: json_to_string_opt(first_result.and_then(|v| v.get("addresses"))),
        practice_locations_json: json_to_string_opt(
            first_result.and_then(|v| v.get("practiceLocations")),
        ),
        taxonomies_json: json_to_string_opt(first_result.and_then(|v| v.get("taxonomies"))),
        identifiers_json: json_to_string_opt(first_result.and_then(|v| v.get("identifiers"))),
        other_names_json: json_to_string_opt(first_result.and_then(|v| v.get("other_names"))),
        endpoints_json: json_to_string_opt(first_result.and_then(|v| v.get("endpoints"))),
        request_url: request_url.to_string(),
        http_status: Some(http_status),
        error_message: None,
        api_run_id: api_run_id.to_string(),
        requested_at_utc: requested_at_utc.to_string(),
        request_params_json: request_params_json.to_string(),
        results_json: json_to_string_opt(response_value.get("results")),
        response_json_raw: serde_json::to_string(response_value).ok(),
    }
}

// NOTE: The resolved NPI identifier parquet (`data/output/npi.parquet`) is exported as a unified
// dataset (bulk NPPES + cached API responses) via `NpiResolvedParquetExporter`.

fn select_latest_nppes_csv(dir: &Path) -> Result<Option<PathBuf>> {
    let mut candidates = collect_nppes_csvs(dir)?;
    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    });
    Ok(candidates.pop())
}

fn collect_nppes_csvs(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut csv_paths = Vec::new();
    collect_csv_paths_recursive(dir, &mut csv_paths)?;

    let mut candidates = Vec::new();
    for path in csv_paths {
        if is_nppes_primary_csv(&path)? {
            candidates.push(path);
        }
    }
    candidates.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    });
    Ok(candidates)
}

fn collect_csv_paths_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("Failed reading directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("Failed iterating {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_csv_paths_recursive(&path, out)?;
        } else if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("csv"))
            .unwrap_or(false)
        {
            out.push(path);
        }
    }
    Ok(())
}

fn is_nppes_primary_csv(path: &Path) -> Result<bool> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("Failed opening CSV {}", path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("Failed reading headers from {}", path.display()))?;

    let has_npi = headers.iter().any(|h| h.trim() == "NPI");
    let has_entity_type = headers.iter().any(|h| h.trim() == "Entity Type Code");
    let has_org = headers
        .iter()
        .any(|h| h.trim() == "Provider Organization Name (Legal Business Name)");
    let has_first = headers.iter().any(|h| h.trim() == "Provider First Name");
    let has_last = headers
        .iter()
        .any(|h| h.trim() == "Provider Last Name (Legal Name)");

    if !(has_npi && has_entity_type && (has_org || (has_first && has_last))) {
        return Ok(false);
    }

    // Header helper CSV files are not useful for preloading and can be expensive to scan repeatedly.
    let has_data_rows = reader
        .records()
        .next()
        .transpose()
        .with_context(|| format!("Failed reading first row from {}", path.display()))?
        .is_some();
    Ok(has_data_rows)
}

fn header_index(headers: &csv::StringRecord, name: &str) -> Result<usize> {
    headers
        .iter()
        .position(|h| h.trim() == name)
        .with_context(|| format!("CSV missing required header '{name}'"))
}

fn opt_header_index(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim() == name)
}

fn normalize_postal_code(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_digit() {
            out.push(ch);
        }
    }
    out
}

fn normalize_country_code(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "US".to_string()
    } else {
        trimmed.to_string()
    }
}

fn country_name_for_code(code: &str) -> Option<&'static str> {
    if code.eq_ignore_ascii_case("US") {
        Some("United States")
    } else {
        None
    }
}

fn address_type_for_country(code: &str) -> &'static str {
    if code.eq_ignore_ascii_case("US") {
        "DOM"
    } else {
        "FOR"
    }
}

#[derive(Debug, Clone)]
struct OtherNameRecord {
    organization_name: String,
    type_code: String,
}

#[derive(Debug, Clone)]
struct PracticeLocationRecord {
    address_1: String,
    address_2: String,
    city: String,
    state: String,
    postal_code: String,
    country_code: String,
    telephone_number: String,
    telephone_extension: String,
    fax_number: String,
}

#[derive(Debug, Clone)]
struct EndpointRecord {
    endpoint_type: String,
    endpoint_type_description: String,
    endpoint: String,
    affiliation: String,
    endpoint_description: String,
    affiliation_legal_business_name: String,
    use_code: String,
    use_description: String,
    other_use_description: String,
    content_type: String,
    content_description: String,
    other_content_description: String,
    address_1: String,
    address_2: String,
    city: String,
    state: String,
    country_code: String,
    postal_code: String,
}

#[derive(Debug, Clone)]
struct NppesBulkFiles {
    label: &'static str,
    npidata_csv: PathBuf,
    othername_csv: Option<PathBuf>,
    pl_csv: Option<PathBuf>,
    endpoint_csv: Option<PathBuf>,
}

impl NppesBulkFiles {
    fn url_sentinel(&self) -> String {
        let file_name = self
            .npidata_csv
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("npidata.csv");
        format!("nppes_bulk:{}:{}", self.label, file_name)
    }

    fn request_params_json(&self) -> String {
        json!({
            "source": "nppes_bulk",
            "bundle": self.label,
            "npidata_file": self.npidata_csv.display().to_string(),
            "othername_file": self.othername_csv.as_ref().map(|p| p.display().to_string()),
            "pl_file": self.pl_csv.as_ref().map(|p| p.display().to_string()),
            "endpoint_file": self.endpoint_csv.as_ref().map(|p| p.display().to_string()),
        })
        .to_string()
    }
}

fn find_nppes_sibling_csv(primary_csv: &Path, prefix: &str) -> Result<Option<PathBuf>> {
    let Some(dir) = primary_csv.parent() else {
        return Ok(None);
    };

    let mut candidates = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("Failed reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("Failed iterating {}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        if !name.to_ascii_lowercase().ends_with(".csv") {
            continue;
        }
        if name.to_ascii_lowercase().ends_with("_fileheader.csv") {
            continue;
        }
        if name.starts_with(prefix) {
            candidates.push(path);
        }
    }
    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    });
    Ok(candidates.pop())
}

fn load_othername_records<'a>(
    csv_path: &Path,
    target_npis: &HashSet<&'a str>,
    out: &mut HashMap<&'a str, Vec<OtherNameRecord>>,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<usize> {
    println!("Loading NPPES othername file {}", csv_path.display());
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .with_context(|| format!("Failed opening NPPES othername CSV {}", csv_path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("Failed reading headers from {}", csv_path.display()))?
        .clone();

    let npi_idx = header_index(&headers, "NPI")?;
    let name_idx = header_index(&headers, "Provider Other Organization Name")?;
    let type_idx = header_index(&headers, "Provider Other Organization Name Type Code")?;

    let mut processed = 0usize;
    let mut loaded = 0usize;
    for row in reader.records() {
        let row =
            row.with_context(|| format!("Failed reading record in {}", csv_path.display()))?;
        processed += 1;
        if processed % 100_000 == 0 && shutdown_requested.load(Ordering::SeqCst) {
            println!(
                "Shutdown requested while reading {}. Stopping othername load early.",
                csv_path.display()
            );
            break;
        }

        let npi = row.get(npi_idx).unwrap_or("").trim();
        let Some(key_ref) = target_npis.get(npi) else {
            continue;
        };
        let key = *key_ref;
        let organization_name = row.get(name_idx).unwrap_or("").trim();
        if organization_name.is_empty() {
            continue;
        }
        let type_code = row.get(type_idx).unwrap_or("").trim();
        out.entry(key).or_default().push(OtherNameRecord {
            organization_name: organization_name.to_string(),
            type_code: type_code.to_string(),
        });
        loaded += 1;
    }

    println!(
        "Loaded {} othername rows (scanned {}).",
        format_count(loaded),
        format_count(processed)
    );
    Ok(loaded)
}

fn load_practice_location_records<'a>(
    csv_path: &Path,
    target_npis: &HashSet<&'a str>,
    out: &mut HashMap<&'a str, Vec<PracticeLocationRecord>>,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<usize> {
    println!(
        "Loading NPPES secondary practice location file {}",
        csv_path.display()
    );
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .with_context(|| format!("Failed opening NPPES pl CSV {}", csv_path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("Failed reading headers from {}", csv_path.display()))?
        .clone();

    let npi_idx = header_index(&headers, "NPI")?;
    let addr1_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address- Address Line 1",
    )?;
    let addr2_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address-  Address Line 2",
    )?;
    let city_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address - City Name",
    )?;
    let state_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address - State Name",
    )?;
    let postal_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address - Postal Code",
    )?;
    let country_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address - Country Code (If outside U.S.)",
    )?;
    let phone_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address - Telephone Number",
    )?;
    let phone_ext_idx = header_index(
        &headers,
        "Provider Secondary Practice Location Address - Telephone Extension",
    )?;
    let fax_idx = header_index(&headers, "Provider Practice Location Address - Fax Number")?;

    let mut processed = 0usize;
    let mut loaded = 0usize;
    for row in reader.records() {
        let row =
            row.with_context(|| format!("Failed reading record in {}", csv_path.display()))?;
        processed += 1;
        if processed % 100_000 == 0 && shutdown_requested.load(Ordering::SeqCst) {
            println!(
                "Shutdown requested while reading {}. Stopping practice location load early.",
                csv_path.display()
            );
            break;
        }

        let npi = row.get(npi_idx).unwrap_or("").trim();
        let Some(key_ref) = target_npis.get(npi) else {
            continue;
        };
        let key = *key_ref;

        let address_1 = row.get(addr1_idx).unwrap_or("").trim();
        let address_2 = row.get(addr2_idx).unwrap_or("").trim();
        let city = row.get(city_idx).unwrap_or("").trim();
        let state = row.get(state_idx).unwrap_or("").trim();
        let postal_code = normalize_postal_code(row.get(postal_idx).unwrap_or("").trim());
        let country_code = normalize_country_code(row.get(country_idx).unwrap_or("").trim());
        let telephone_number = row.get(phone_idx).unwrap_or("").trim();
        let telephone_extension = row.get(phone_ext_idx).unwrap_or("").trim();
        let fax_number = row.get(fax_idx).unwrap_or("").trim();

        if address_1.is_empty()
            && address_2.is_empty()
            && city.is_empty()
            && state.is_empty()
            && postal_code.is_empty()
            && telephone_number.is_empty()
        {
            continue;
        }

        out.entry(key).or_default().push(PracticeLocationRecord {
            address_1: address_1.to_string(),
            address_2: address_2.to_string(),
            city: city.to_string(),
            state: state.to_string(),
            postal_code,
            country_code,
            telephone_number: telephone_number.to_string(),
            telephone_extension: telephone_extension.to_string(),
            fax_number: fax_number.to_string(),
        });
        loaded += 1;
    }

    println!(
        "Loaded {} secondary practice location rows (scanned {}).",
        format_count(loaded),
        format_count(processed)
    );
    Ok(loaded)
}

fn load_endpoint_records<'a>(
    csv_path: &Path,
    target_npis: &HashSet<&'a str>,
    out: &mut HashMap<&'a str, Vec<EndpointRecord>>,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<usize> {
    println!("Loading NPPES endpoint file {}", csv_path.display());
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .with_context(|| format!("Failed opening NPPES endpoint CSV {}", csv_path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("Failed reading headers from {}", csv_path.display()))?
        .clone();

    let npi_idx = header_index(&headers, "NPI")?;
    let endpoint_type_idx = header_index(&headers, "Endpoint Type")?;
    let endpoint_type_desc_idx = header_index(&headers, "Endpoint Type Description")?;
    let endpoint_idx = header_index(&headers, "Endpoint")?;
    let affiliation_idx = header_index(&headers, "Affiliation")?;
    let endpoint_desc_idx = header_index(&headers, "Endpoint Description")?;
    let affiliation_lbn_idx = header_index(&headers, "Affiliation Legal Business Name")?;
    let use_code_idx = header_index(&headers, "Use Code")?;
    let use_desc_idx = header_index(&headers, "Use Description")?;
    let other_use_desc_idx = header_index(&headers, "Other Use Description")?;
    let content_type_idx = header_index(&headers, "Content Type")?;
    let content_desc_idx = header_index(&headers, "Content Description")?;
    let other_content_desc_idx = header_index(&headers, "Other Content Description")?;
    let addr1_idx = header_index(&headers, "Affiliation Address Line One")?;
    let addr2_idx = header_index(&headers, "Affiliation Address Line Two")?;
    let city_idx = header_index(&headers, "Affiliation Address City")?;
    let state_idx = header_index(&headers, "Affiliation Address State")?;
    let country_idx = header_index(&headers, "Affiliation Address Country")?;
    let postal_idx = header_index(&headers, "Affiliation Address Postal Code")?;

    let mut processed = 0usize;
    let mut loaded = 0usize;
    for row in reader.records() {
        let row =
            row.with_context(|| format!("Failed reading record in {}", csv_path.display()))?;
        processed += 1;
        if processed % 100_000 == 0 && shutdown_requested.load(Ordering::SeqCst) {
            println!(
                "Shutdown requested while reading {}. Stopping endpoint load early.",
                csv_path.display()
            );
            break;
        }

        let npi = row.get(npi_idx).unwrap_or("").trim();
        let Some(key_ref) = target_npis.get(npi) else {
            continue;
        };
        let key = *key_ref;

        let endpoint_type = row.get(endpoint_type_idx).unwrap_or("").trim();
        let endpoint = row.get(endpoint_idx).unwrap_or("").trim();
        if endpoint_type.is_empty() && endpoint.is_empty() {
            continue;
        }

        let country_code = normalize_country_code(row.get(country_idx).unwrap_or("").trim());
        let postal_code = normalize_postal_code(row.get(postal_idx).unwrap_or("").trim());

        out.entry(key).or_default().push(EndpointRecord {
            endpoint_type: endpoint_type.to_string(),
            endpoint_type_description: row
                .get(endpoint_type_desc_idx)
                .unwrap_or("")
                .trim()
                .to_string(),
            endpoint: endpoint.to_string(),
            affiliation: row.get(affiliation_idx).unwrap_or("").trim().to_string(),
            endpoint_description: row.get(endpoint_desc_idx).unwrap_or("").trim().to_string(),
            affiliation_legal_business_name: row
                .get(affiliation_lbn_idx)
                .unwrap_or("")
                .trim()
                .to_string(),
            use_code: row.get(use_code_idx).unwrap_or("").trim().to_string(),
            use_description: row.get(use_desc_idx).unwrap_or("").trim().to_string(),
            other_use_description: row.get(other_use_desc_idx).unwrap_or("").trim().to_string(),
            content_type: row.get(content_type_idx).unwrap_or("").trim().to_string(),
            content_description: row.get(content_desc_idx).unwrap_or("").trim().to_string(),
            other_content_description: row
                .get(other_content_desc_idx)
                .unwrap_or("")
                .trim()
                .to_string(),
            address_1: row.get(addr1_idx).unwrap_or("").trim().to_string(),
            address_2: row.get(addr2_idx).unwrap_or("").trim().to_string(),
            city: row.get(city_idx).unwrap_or("").trim().to_string(),
            state: row.get(state_idx).unwrap_or("").trim().to_string(),
            country_code,
            postal_code,
        });
        loaded += 1;
    }

    println!(
        "Loaded {} endpoint rows (scanned {}).",
        format_count(loaded),
        format_count(processed)
    );
    Ok(loaded)
}

#[derive(Debug, Clone)]
struct TaxonomyIndices {
    code: Option<usize>,
    license: Option<usize>,
    state: Option<usize>,
    primary_switch: Option<usize>,
    group: Option<usize>,
}

#[derive(Debug, Clone)]
struct IdentifierIndices {
    identifier: Option<usize>,
    type_code: Option<usize>,
    state: Option<usize>,
    issuer: Option<usize>,
}

#[derive(Debug, Clone)]
struct NppesPrimaryIndices {
    npi: usize,
    entity_type: Option<usize>,
    org_name: Option<usize>,
    last_name: Option<usize>,
    first_name: Option<usize>,
    middle_name: Option<usize>,
    name_prefix: Option<usize>,
    name_suffix: Option<usize>,
    credential: Option<usize>,
    mailing_address_1: Option<usize>,
    mailing_address_2: Option<usize>,
    mailing_city: Option<usize>,
    mailing_state: Option<usize>,
    mailing_postal: Option<usize>,
    mailing_country: Option<usize>,
    mailing_phone: Option<usize>,
    mailing_fax: Option<usize>,
    location_address_1: Option<usize>,
    location_address_2: Option<usize>,
    location_city: Option<usize>,
    location_state: Option<usize>,
    location_postal: Option<usize>,
    location_country: Option<usize>,
    location_phone: Option<usize>,
    location_fax: Option<usize>,
    enumeration_date: Option<usize>,
    last_update_date: Option<usize>,
    deactivation_date: Option<usize>,
    reactivation_date: Option<usize>,
    sex: Option<usize>,
    authorized_official_last: Option<usize>,
    authorized_official_first: Option<usize>,
    authorized_official_middle: Option<usize>,
    authorized_official_title: Option<usize>,
    authorized_official_phone: Option<usize>,
    authorized_official_prefix: Option<usize>,
    authorized_official_suffix: Option<usize>,
    authorized_official_credential: Option<usize>,
    sole_proprietor: Option<usize>,
    organizational_subpart: Option<usize>,
    certification_date: Option<usize>,
    taxonomies: Vec<TaxonomyIndices>,
    identifiers: Vec<IdentifierIndices>,
}

impl NppesPrimaryIndices {
    fn from_headers(headers: &csv::StringRecord) -> Result<Self> {
        let npi = header_index(headers, "NPI")?;
        let entity_type = opt_header_index(headers, "Entity Type Code");
        let org_name =
            opt_header_index(headers, "Provider Organization Name (Legal Business Name)");
        let last_name = opt_header_index(headers, "Provider Last Name (Legal Name)");
        let first_name = opt_header_index(headers, "Provider First Name");
        let middle_name = opt_header_index(headers, "Provider Middle Name");
        let name_prefix = opt_header_index(headers, "Provider Name Prefix Text");
        let name_suffix = opt_header_index(headers, "Provider Name Suffix Text");
        let credential = opt_header_index(headers, "Provider Credential Text");

        let mailing_address_1 =
            opt_header_index(headers, "Provider First Line Business Mailing Address");
        let mailing_address_2 =
            opt_header_index(headers, "Provider Second Line Business Mailing Address");
        let mailing_city = opt_header_index(headers, "Provider Business Mailing Address City Name");
        let mailing_state =
            opt_header_index(headers, "Provider Business Mailing Address State Name");
        let mailing_postal =
            opt_header_index(headers, "Provider Business Mailing Address Postal Code");
        let mailing_country = opt_header_index(
            headers,
            "Provider Business Mailing Address Country Code (If outside U.S.)",
        );
        let mailing_phone = opt_header_index(
            headers,
            "Provider Business Mailing Address Telephone Number",
        );
        let mailing_fax = opt_header_index(headers, "Provider Business Mailing Address Fax Number");

        let location_address_1 = opt_header_index(
            headers,
            "Provider First Line Business Practice Location Address",
        );
        let location_address_2 = opt_header_index(
            headers,
            "Provider Second Line Business Practice Location Address",
        );
        let location_city = opt_header_index(
            headers,
            "Provider Business Practice Location Address City Name",
        );
        let location_state = opt_header_index(
            headers,
            "Provider Business Practice Location Address State Name",
        );
        let location_postal = opt_header_index(
            headers,
            "Provider Business Practice Location Address Postal Code",
        );
        let location_country = opt_header_index(
            headers,
            "Provider Business Practice Location Address Country Code (If outside U.S.)",
        );
        let location_phone = opt_header_index(
            headers,
            "Provider Business Practice Location Address Telephone Number",
        );
        let location_fax = opt_header_index(
            headers,
            "Provider Business Practice Location Address Fax Number",
        );

        let enumeration_date = opt_header_index(headers, "Provider Enumeration Date");
        let last_update_date = opt_header_index(headers, "Last Update Date");
        let deactivation_date = opt_header_index(headers, "NPI Deactivation Date");
        let reactivation_date = opt_header_index(headers, "NPI Reactivation Date");
        let sex = opt_header_index(headers, "Provider Sex Code");

        let authorized_official_last = opt_header_index(headers, "Authorized Official Last Name");
        let authorized_official_first = opt_header_index(headers, "Authorized Official First Name");
        let authorized_official_middle =
            opt_header_index(headers, "Authorized Official Middle Name");
        let authorized_official_title =
            opt_header_index(headers, "Authorized Official Title or Position");
        let authorized_official_phone =
            opt_header_index(headers, "Authorized Official Telephone Number");
        let authorized_official_prefix =
            opt_header_index(headers, "Authorized Official Name Prefix Text");
        let authorized_official_suffix =
            opt_header_index(headers, "Authorized Official Name Suffix Text");
        let authorized_official_credential =
            opt_header_index(headers, "Authorized Official Credential Text");
        let sole_proprietor = opt_header_index(headers, "Is Sole Proprietor");
        let organizational_subpart = opt_header_index(headers, "Is Organization Subpart");
        let certification_date = opt_header_index(headers, "Certification Date");

        let mut taxonomies = Vec::with_capacity(15);
        for i in 1..=15 {
            taxonomies.push(TaxonomyIndices {
                code: opt_header_index(headers, &format!("Healthcare Provider Taxonomy Code_{i}")),
                license: opt_header_index(headers, &format!("Provider License Number_{i}")),
                state: opt_header_index(
                    headers,
                    &format!("Provider License Number State Code_{i}"),
                ),
                primary_switch: opt_header_index(
                    headers,
                    &format!("Healthcare Provider Primary Taxonomy Switch_{i}"),
                ),
                group: opt_header_index(
                    headers,
                    &format!("Healthcare Provider Taxonomy Group_{i}"),
                ),
            });
        }

        let mut identifiers = Vec::with_capacity(50);
        for i in 1..=50 {
            identifiers.push(IdentifierIndices {
                identifier: opt_header_index(headers, &format!("Other Provider Identifier_{i}")),
                type_code: opt_header_index(
                    headers,
                    &format!("Other Provider Identifier Type Code_{i}"),
                ),
                state: opt_header_index(headers, &format!("Other Provider Identifier State_{i}")),
                issuer: opt_header_index(headers, &format!("Other Provider Identifier Issuer_{i}")),
            });
        }

        Ok(Self {
            npi,
            entity_type,
            org_name,
            last_name,
            first_name,
            middle_name,
            name_prefix,
            name_suffix,
            credential,
            mailing_address_1,
            mailing_address_2,
            mailing_city,
            mailing_state,
            mailing_postal,
            mailing_country,
            mailing_phone,
            mailing_fax,
            location_address_1,
            location_address_2,
            location_city,
            location_state,
            location_postal,
            location_country,
            location_phone,
            location_fax,
            enumeration_date,
            last_update_date,
            deactivation_date,
            reactivation_date,
            sex,
            authorized_official_last,
            authorized_official_first,
            authorized_official_middle,
            authorized_official_title,
            authorized_official_phone,
            authorized_official_prefix,
            authorized_official_suffix,
            authorized_official_credential,
            sole_proprietor,
            organizational_subpart,
            certification_date,
            taxonomies,
            identifiers,
        })
    }
}

fn row_value<'r>(row: &'r csv::StringRecord, idx: Option<usize>) -> &'r str {
    idx.and_then(|i| row.get(i)).unwrap_or("").trim()
}

#[derive(Debug, Clone)]
struct BulkNpiJsonRow {
    basic_json: String,
    addresses_json: String,
    practice_locations_json: String,
    taxonomies_json: String,
    identifiers_json: String,
    other_names_json: String,
    endpoints_json: String,
    results_json: String,
    response_json: String,
}

fn build_bulk_npi_json_row(
    npi: &str,
    row: &csv::StringRecord,
    idx: &NppesPrimaryIndices,
    other_names: &[OtherNameRecord],
    practice_locations: &[PracticeLocationRecord],
    endpoints: &[EndpointRecord],
) -> BulkNpiJsonRow {
    let entity_type_code = row_value(row, idx.entity_type);
    let enumeration_type = if entity_type_code == "1" {
        "NPI-1"
    } else if entity_type_code == "2" {
        "NPI-2"
    } else {
        ""
    };

    let deactivation_date = row_value(row, idx.deactivation_date);
    let reactivation_date = row_value(row, idx.reactivation_date);
    let status = if !deactivation_date.is_empty() && reactivation_date.is_empty() {
        "D"
    } else {
        "A"
    };

    let mut basic = serde_json::Map::new();
    basic.insert("status".to_string(), Value::String(status.to_string()));
    let enumeration_date = row_value(row, idx.enumeration_date);
    if !enumeration_date.is_empty() {
        basic.insert(
            "enumeration_date".to_string(),
            Value::String(enumeration_date.to_string()),
        );
    }
    let last_updated = row_value(row, idx.last_update_date);
    if !last_updated.is_empty() {
        basic.insert(
            "last_updated".to_string(),
            Value::String(last_updated.to_string()),
        );
    }
    let certification_date = row_value(row, idx.certification_date);
    if !certification_date.is_empty() {
        basic.insert(
            "certification_date".to_string(),
            Value::String(certification_date.to_string()),
        );
    }
    let credential = row_value(row, idx.credential);
    if !credential.is_empty() {
        basic.insert(
            "credential".to_string(),
            Value::String(credential.to_string()),
        );
    }

    if enumeration_type == "NPI-1" {
        let first_name = row_value(row, idx.first_name);
        if !first_name.is_empty() {
            basic.insert(
                "first_name".to_string(),
                Value::String(first_name.to_string()),
            );
        }
        let last_name = row_value(row, idx.last_name);
        if !last_name.is_empty() {
            basic.insert(
                "last_name".to_string(),
                Value::String(last_name.to_string()),
            );
        }
        let middle_name = row_value(row, idx.middle_name);
        if !middle_name.is_empty() {
            basic.insert(
                "middle_name".to_string(),
                Value::String(middle_name.to_string()),
            );
        }
        let name_prefix = row_value(row, idx.name_prefix);
        if !name_prefix.is_empty() {
            basic.insert(
                "name_prefix".to_string(),
                Value::String(name_prefix.to_string()),
            );
        }
        let name_suffix = row_value(row, idx.name_suffix);
        if !name_suffix.is_empty() {
            basic.insert(
                "name_suffix".to_string(),
                Value::String(name_suffix.to_string()),
            );
        }
        let sex = row_value(row, idx.sex);
        if !sex.is_empty() {
            basic.insert("sex".to_string(), Value::String(sex.to_string()));
        }
        let sole_proprietor = row_value(row, idx.sole_proprietor);
        if !sole_proprietor.is_empty() {
            basic.insert(
                "sole_proprietor".to_string(),
                Value::String(sole_proprietor.to_string()),
            );
        }
    } else if enumeration_type == "NPI-2" {
        let org_name = row_value(row, idx.org_name);
        if !org_name.is_empty() {
            basic.insert(
                "organization_name".to_string(),
                Value::String(org_name.to_string()),
            );
        }
        let organizational_subpart = row_value(row, idx.organizational_subpart);
        if !organizational_subpart.is_empty() {
            basic.insert(
                "organizational_subpart".to_string(),
                Value::String(organizational_subpart.to_string()),
            );
        }
        let auth_first = row_value(row, idx.authorized_official_first);
        if !auth_first.is_empty() {
            basic.insert(
                "authorized_official_first_name".to_string(),
                Value::String(auth_first.to_string()),
            );
        }
        let auth_last = row_value(row, idx.authorized_official_last);
        if !auth_last.is_empty() {
            basic.insert(
                "authorized_official_last_name".to_string(),
                Value::String(auth_last.to_string()),
            );
        }
        let auth_middle = row_value(row, idx.authorized_official_middle);
        if !auth_middle.is_empty() {
            basic.insert(
                "authorized_official_middle_name".to_string(),
                Value::String(auth_middle.to_string()),
            );
        }
        let auth_prefix = row_value(row, idx.authorized_official_prefix);
        if !auth_prefix.is_empty() {
            basic.insert(
                "authorized_official_name_prefix_text".to_string(),
                Value::String(auth_prefix.to_string()),
            );
        }
        let auth_suffix = row_value(row, idx.authorized_official_suffix);
        if !auth_suffix.is_empty() {
            basic.insert(
                "authorized_official_name_suffix_text".to_string(),
                Value::String(auth_suffix.to_string()),
            );
        }
        let auth_cred = row_value(row, idx.authorized_official_credential);
        if !auth_cred.is_empty() {
            basic.insert(
                "authorized_official_credential_text".to_string(),
                Value::String(auth_cred.to_string()),
            );
        }
        let auth_title = row_value(row, idx.authorized_official_title);
        if !auth_title.is_empty() {
            basic.insert(
                "authorized_official_title_or_position".to_string(),
                Value::String(auth_title.to_string()),
            );
        }
        let auth_phone = row_value(row, idx.authorized_official_phone);
        if !auth_phone.is_empty() {
            basic.insert(
                "authorized_official_telephone_number".to_string(),
                Value::String(auth_phone.to_string()),
            );
        }
    }

    let basic_json =
        serde_json::to_string(&Value::Object(basic)).unwrap_or_else(|_| "{}".to_string());

    let mut addresses = Vec::new();
    for (
        purpose,
        addr1_idx,
        addr2_idx,
        city_idx,
        state_idx,
        postal_idx,
        country_idx,
        phone_idx,
        fax_idx,
    ) in [
        (
            "MAILING",
            idx.mailing_address_1,
            idx.mailing_address_2,
            idx.mailing_city,
            idx.mailing_state,
            idx.mailing_postal,
            idx.mailing_country,
            idx.mailing_phone,
            idx.mailing_fax,
        ),
        (
            "LOCATION",
            idx.location_address_1,
            idx.location_address_2,
            idx.location_city,
            idx.location_state,
            idx.location_postal,
            idx.location_country,
            idx.location_phone,
            idx.location_fax,
        ),
    ] {
        let address_1 = row_value(row, addr1_idx);
        let address_2 = row_value(row, addr2_idx);
        let city = row_value(row, city_idx);
        let state = row_value(row, state_idx);
        let postal_code = normalize_postal_code(row_value(row, postal_idx));
        let country_code = normalize_country_code(row_value(row, country_idx));
        let telephone_number = row_value(row, phone_idx);
        let fax_number = row_value(row, fax_idx);

        if address_1.is_empty()
            && address_2.is_empty()
            && city.is_empty()
            && state.is_empty()
            && postal_code.is_empty()
            && telephone_number.is_empty()
            && fax_number.is_empty()
        {
            continue;
        }

        let mut obj = serde_json::Map::new();
        obj.insert(
            "address_purpose".to_string(),
            Value::String(purpose.to_string()),
        );
        obj.insert(
            "address_type".to_string(),
            Value::String(address_type_for_country(&country_code).to_string()),
        );
        if !address_1.is_empty() {
            obj.insert(
                "address_1".to_string(),
                Value::String(address_1.to_string()),
            );
        }
        if !address_2.is_empty() {
            obj.insert(
                "address_2".to_string(),
                Value::String(address_2.to_string()),
            );
        }
        if !city.is_empty() {
            obj.insert("city".to_string(), Value::String(city.to_string()));
        }
        if !state.is_empty() {
            obj.insert("state".to_string(), Value::String(state.to_string()));
        }
        if !postal_code.is_empty() {
            obj.insert("postal_code".to_string(), Value::String(postal_code));
        }
        obj.insert(
            "country_code".to_string(),
            Value::String(country_code.clone()),
        );
        if let Some(name) = country_name_for_code(&country_code) {
            obj.insert("country_name".to_string(), Value::String(name.to_string()));
        }
        if !telephone_number.is_empty() {
            obj.insert(
                "telephone_number".to_string(),
                Value::String(telephone_number.to_string()),
            );
        }
        if !fax_number.is_empty() {
            obj.insert(
                "fax_number".to_string(),
                Value::String(fax_number.to_string()),
            );
        }
        addresses.push(Value::Object(obj));
    }

    let addresses_json =
        serde_json::to_string(&Value::Array(addresses)).unwrap_or_else(|_| "[]".to_string());

    let mut taxonomies = Vec::new();
    for t in &idx.taxonomies {
        let code = row_value(row, t.code);
        if code.is_empty() {
            continue;
        }
        let primary = row_value(row, t.primary_switch).eq_ignore_ascii_case("Y");
        let license_raw = row_value(row, t.license);
        let state_raw = row_value(row, t.state);
        let group_raw = row_value(row, t.group);

        let mut obj = serde_json::Map::new();
        obj.insert("code".to_string(), Value::String(code.to_string()));
        obj.insert("desc".to_string(), Value::Null);
        obj.insert(
            "license".to_string(),
            if license_raw.is_empty() {
                Value::Null
            } else {
                Value::String(license_raw.to_string())
            },
        );
        obj.insert("primary".to_string(), Value::Bool(primary));
        obj.insert(
            "state".to_string(),
            if state_raw.is_empty() {
                Value::Null
            } else {
                Value::String(state_raw.to_string())
            },
        );
        obj.insert(
            "taxonomy_group".to_string(),
            Value::String(group_raw.to_string()),
        );
        taxonomies.push(Value::Object(obj));
    }
    let taxonomies_json =
        serde_json::to_string(&Value::Array(taxonomies)).unwrap_or_else(|_| "[]".to_string());

    let mut identifiers = Vec::new();
    for i in &idx.identifiers {
        let identifier_value = row_value(row, i.identifier);
        if identifier_value.is_empty() {
            continue;
        }
        let code_raw = row_value(row, i.type_code);
        let state_raw = row_value(row, i.state);
        let issuer_raw = row_value(row, i.issuer);

        let mut obj = serde_json::Map::new();
        obj.insert(
            "identifier".to_string(),
            Value::String(identifier_value.to_string()),
        );
        if !code_raw.is_empty() {
            obj.insert("code".to_string(), Value::String(code_raw.to_string()));
        }
        obj.insert("desc".to_string(), Value::Null);
        if !state_raw.is_empty() {
            obj.insert("state".to_string(), Value::String(state_raw.to_string()));
        }
        obj.insert(
            "issuer".to_string(),
            if issuer_raw.is_empty() {
                Value::Null
            } else {
                Value::String(issuer_raw.to_string())
            },
        );
        identifiers.push(Value::Object(obj));
    }
    let identifiers_json =
        serde_json::to_string(&Value::Array(identifiers)).unwrap_or_else(|_| "[]".to_string());

    let mut other_names_values = Vec::new();
    for other in other_names {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "organization_name".to_string(),
            Value::String(other.organization_name.clone()),
        );
        if !other.type_code.is_empty() {
            obj.insert("code".to_string(), Value::String(other.type_code.clone()));
        }
        obj.insert("desc".to_string(), Value::Null);
        other_names_values.push(Value::Object(obj));
    }
    let other_names_json = serde_json::to_string(&Value::Array(other_names_values))
        .unwrap_or_else(|_| "[]".to_string());

    let mut practice_locations_values = Vec::new();
    for pl in practice_locations {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "address_type".to_string(),
            Value::String(address_type_for_country(&pl.country_code).to_string()),
        );
        if !pl.address_1.is_empty() {
            obj.insert("address_1".to_string(), Value::String(pl.address_1.clone()));
        }
        if !pl.address_2.is_empty() {
            obj.insert("address_2".to_string(), Value::String(pl.address_2.clone()));
        }
        if !pl.city.is_empty() {
            obj.insert("city".to_string(), Value::String(pl.city.clone()));
        }
        if !pl.state.is_empty() {
            obj.insert("state".to_string(), Value::String(pl.state.clone()));
        }
        if !pl.postal_code.is_empty() {
            obj.insert(
                "postal_code".to_string(),
                Value::String(pl.postal_code.clone()),
            );
        }
        obj.insert(
            "country_code".to_string(),
            Value::String(pl.country_code.clone()),
        );
        if let Some(name) = country_name_for_code(&pl.country_code) {
            obj.insert("country_name".to_string(), Value::String(name.to_string()));
        }
        if !pl.telephone_number.is_empty() {
            obj.insert(
                "telephone_number".to_string(),
                Value::String(pl.telephone_number.clone()),
            );
        }
        if !pl.telephone_extension.is_empty() {
            obj.insert(
                "telephone_extension".to_string(),
                Value::String(pl.telephone_extension.clone()),
            );
        }
        if !pl.fax_number.is_empty() {
            obj.insert(
                "fax_number".to_string(),
                Value::String(pl.fax_number.clone()),
            );
        }
        practice_locations_values.push(Value::Object(obj));
    }
    let practice_locations_json = serde_json::to_string(&Value::Array(practice_locations_values))
        .unwrap_or_else(|_| "[]".to_string());

    let mut endpoints_values = Vec::new();
    for ep in endpoints {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "endpointType".to_string(),
            Value::String(ep.endpoint_type.clone()),
        );
        obj.insert(
            "endpointTypeDescription".to_string(),
            Value::String(ep.endpoint_type_description.clone()),
        );
        obj.insert("endpoint".to_string(), Value::String(ep.endpoint.clone()));
        obj.insert(
            "affiliation".to_string(),
            Value::String(ep.affiliation.clone()),
        );
        obj.insert("useCode".to_string(), Value::String(ep.use_code.clone()));
        obj.insert(
            "useDescription".to_string(),
            Value::String(ep.use_description.clone()),
        );
        obj.insert(
            "contentType".to_string(),
            Value::String(ep.content_type.clone()),
        );
        obj.insert(
            "contentTypeDescription".to_string(),
            Value::String(ep.content_description.clone()),
        );
        if !ep.other_use_description.is_empty() {
            obj.insert(
                "otherUseDescription".to_string(),
                Value::String(ep.other_use_description.clone()),
            );
        }
        if !ep.other_content_description.is_empty() {
            obj.insert(
                "otherContentDescription".to_string(),
                Value::String(ep.other_content_description.clone()),
            );
        }
        if !ep.endpoint_description.is_empty() {
            obj.insert(
                "endpointDescription".to_string(),
                Value::String(ep.endpoint_description.clone()),
            );
        }
        if !ep.affiliation_legal_business_name.is_empty() {
            obj.insert(
                "affiliationLegalBusinessName".to_string(),
                Value::String(ep.affiliation_legal_business_name.clone()),
            );
        }
        let address_type = address_type_for_country(&ep.country_code);
        obj.insert(
            "address_type".to_string(),
            Value::String(address_type.to_string()),
        );
        if !ep.address_1.is_empty() {
            obj.insert("address_1".to_string(), Value::String(ep.address_1.clone()));
        }
        if !ep.address_2.is_empty() {
            obj.insert("address_2".to_string(), Value::String(ep.address_2.clone()));
        }
        if !ep.city.is_empty() {
            obj.insert("city".to_string(), Value::String(ep.city.clone()));
        }
        if !ep.state.is_empty() {
            obj.insert("state".to_string(), Value::String(ep.state.clone()));
        }
        if !ep.postal_code.is_empty() {
            obj.insert(
                "postal_code".to_string(),
                Value::String(ep.postal_code.clone()),
            );
        }
        obj.insert(
            "country_code".to_string(),
            Value::String(ep.country_code.clone()),
        );
        if let Some(name) = country_name_for_code(&ep.country_code) {
            obj.insert("country_name".to_string(), Value::String(name.to_string()));
        }
        endpoints_values.push(Value::Object(obj));
    }
    let endpoints_json =
        serde_json::to_string(&Value::Array(endpoints_values)).unwrap_or_else(|_| "[]".to_string());

    let npi_json = serde_json::to_string(npi).unwrap_or_else(|_| format!("\"{npi}\""));
    let enum_json = serde_json::to_string(enumeration_type).unwrap_or_else(|_| "\"\"".to_string());

    let results_json = format!(
        "[{{\"number\":{npi_json},\"enumeration_type\":{enum_json},\"basic\":{basic_json},\"addresses\":{addresses_json},\"practiceLocations\":{practice_locations_json},\"taxonomies\":{taxonomies_json},\"identifiers\":{identifiers_json},\"other_names\":{other_names_json},\"endpoints\":{endpoints_json}}}]"
    );
    let response_json = format!("{{\"result_count\":1,\"results\":{results_json}}}");

    BulkNpiJsonRow {
        basic_json,
        addresses_json,
        practice_locations_json,
        taxonomies_json,
        identifiers_json,
        other_names_json,
        endpoints_json,
        results_json,
        response_json,
    }
}

struct NpiResolvedParquetExporter<'a> {
    unique_npis: &'a [String],
    remaining: HashSet<&'a str>,
    other_names: HashMap<&'a str, Vec<OtherNameRecord>>,
    practice_locations: HashMap<&'a str, Vec<PracticeLocationRecord>>,
    endpoints: HashMap<&'a str, Vec<EndpointRecord>>,
    writer: StringParquetWriter,
    requested_at_utc: String,
    api_run_id: String,
}

impl<'a> NpiResolvedParquetExporter<'a> {
    fn try_new(output_path: &Path, unique_npis: &'a [String], api_run_id: &str) -> Result<Self> {
        let columns = [
            "npi",
            "basic",
            "addresses",
            "practice_locations",
            "taxonomies",
            "identifiers",
            "other_names",
            "endpoints",
            "url",
            "error_message",
            "api_run_id",
            "requested_at_utc",
            "request_params",
            "results",
            "response_json",
        ];
        let writer = StringParquetWriter::try_new(output_path, &columns, 10_000)?;
        let remaining: HashSet<&str> = unique_npis.iter().map(|s| s.as_str()).collect();
        Ok(Self {
            unique_npis,
            remaining,
            other_names: HashMap::new(),
            practice_locations: HashMap::new(),
            endpoints: HashMap::new(),
            writer,
            requested_at_utc: now_unix_seconds().to_string(),
            api_run_id: api_run_id.to_string(),
        })
    }

    fn load_supplemental_records(
        &mut self,
        sources: &[NppesBulkFiles],
        shutdown_requested: &Arc<AtomicBool>,
    ) -> Result<()> {
        let target_npis = &self.remaining;
        for source in sources {
            if let Some(path) = source.othername_csv.as_deref() {
                let _ = load_othername_records(
                    path,
                    target_npis,
                    &mut self.other_names,
                    shutdown_requested,
                )?;
            }
            if let Some(path) = source.pl_csv.as_deref() {
                let _ = load_practice_location_records(
                    path,
                    target_npis,
                    &mut self.practice_locations,
                    shutdown_requested,
                )?;
            }
            if let Some(path) = source.endpoint_csv.as_deref() {
                let _ = load_endpoint_records(
                    path,
                    target_npis,
                    &mut self.endpoints,
                    shutdown_requested,
                )?;
            }
        }
        Ok(())
    }

    fn write_bulk_from_primary(
        &mut self,
        cache: Option<&NpiCache>,
        source: &NppesBulkFiles,
        shutdown_requested: &Arc<AtomicBool>,
    ) -> Result<usize> {
        println!(
            "Exporting bulk NPI rows from NPPES {} primary file {}",
            source.label,
            source.npidata_csv.display()
        );

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&source.npidata_csv)
            .with_context(|| {
                format!("Failed opening NPPES CSV {}", source.npidata_csv.display())
            })?;
        let headers = reader
            .headers()
            .with_context(|| {
                format!(
                    "Failed reading NPPES headers from {}",
                    source.npidata_csv.display()
                )
            })?
            .clone();
        let idx = NppesPrimaryIndices::from_headers(&headers)?;

        let request_params_json = source.request_params_json();
        let url_sentinel = source.url_sentinel();
        let requested_at_utc = self.requested_at_utc.clone();
        let api_run_id = self.api_run_id.clone();

        // Cache preload is optional (export-only runs should not mutate the cache).
        let mut stmt = if let Some(cache) = cache {
            cache
                .conn
                .execute_batch("BEGIN IMMEDIATE TRANSACTION;")
                .context("Failed beginning NPPES bulk transaction")?;
            Some(
                cache.conn
                    .prepare(
                        "
                        INSERT INTO npi_cache (npi, provider_name, status, error_message, fetched_at_unix)
                        VALUES (?1, ?2, 'ok', NULL, strftime('%s', 'now'))
                        ON CONFLICT(npi) DO UPDATE SET
                            provider_name = excluded.provider_name,
                            status = excluded.status,
                            error_message = excluded.error_message,
                            fetched_at_unix = excluded.fetched_at_unix
                        ",
                    )
                    .context("Failed preparing NPPES cache upsert statement")?,
            )
        } else {
            None
        };

        let scan_result: Result<(usize, usize)> = (|| {
            let mut processed = 0usize;
            let mut emitted = 0usize;
            for row in reader.records() {
                let row = row.with_context(|| {
                    format!("Failed reading record in {}", source.npidata_csv.display())
                })?;
                processed += 1;

                if processed % 50_000 == 0 {
                    if shutdown_requested.load(Ordering::SeqCst) {
                        println!(
                            "Shutdown requested while reading {}. Stopping bulk export early.",
                            source.npidata_csv.display()
                        );
                        break;
                    }
                    if processed % 1_000_000 == 0 {
                        println!(
                            "Scanned {} rows from {} (emitted {} remaining {}).",
                            format_count(processed),
                            source.label,
                            format_count(emitted),
                            format_count(self.remaining.len())
                        );
                    }
                }

                let npi = row.get(idx.npi).unwrap_or("").trim();
                if npi.is_empty() || !self.remaining.contains(npi) {
                    continue;
                }

                if let Some(stmt) = stmt.as_mut() {
                    let org_name = row_value(&row, idx.org_name);
                    let first_name = row_value(&row, idx.first_name);
                    let last_name = row_value(&row, idx.last_name);
                    let provider_name = if !org_name.is_empty() {
                        org_name.to_string()
                    } else if !first_name.is_empty() && !last_name.is_empty() {
                        format!("{first_name} {last_name}")
                    } else if !first_name.is_empty() {
                        first_name.to_string()
                    } else if !last_name.is_empty() {
                        last_name.to_string()
                    } else {
                        String::new()
                    };

                    stmt.execute(params![npi, provider_name])
                        .with_context(|| format!("Failed upserting preloaded NPI {npi}"))?;
                }

                let other_names = self.other_names.remove(npi).unwrap_or_default();
                let practice_locations = self.practice_locations.remove(npi).unwrap_or_default();
                let endpoints = self.endpoints.remove(npi).unwrap_or_default();

                let json_row = build_bulk_npi_json_row(
                    npi,
                    &row,
                    &idx,
                    &other_names,
                    &practice_locations,
                    &endpoints,
                );

                self.writer.push_row(&[
                    Some(npi),
                    Some(json_row.basic_json.as_str()),
                    Some(json_row.addresses_json.as_str()),
                    Some(json_row.practice_locations_json.as_str()),
                    Some(json_row.taxonomies_json.as_str()),
                    Some(json_row.identifiers_json.as_str()),
                    Some(json_row.other_names_json.as_str()),
                    Some(json_row.endpoints_json.as_str()),
                    Some(url_sentinel.as_str()),
                    None,
                    Some(api_run_id.as_str()),
                    Some(requested_at_utc.as_str()),
                    Some(request_params_json.as_str()),
                    Some(json_row.results_json.as_str()),
                    Some(json_row.response_json.as_str()),
                ])?;

                self.remaining.remove(npi);
                emitted += 1;
                if self.remaining.is_empty() {
                    break;
                }
            }
            Ok((processed, emitted))
        })();

        drop(stmt);
        match scan_result {
            Ok((processed, emitted)) => {
                if let Some(cache) = cache {
                    cache
                        .conn
                        .execute_batch("COMMIT;")
                        .context("Failed committing NPPES bulk transaction")?;
                }

                println!(
                    "Finished bulk export for {}: scanned {} emitted {} remaining {}",
                    source.label,
                    format_count(processed),
                    format_count(emitted),
                    format_count(self.remaining.len())
                );
                Ok(emitted)
            }
            Err(err) => {
                if let Some(cache) = cache {
                    let _ = cache.conn.execute_batch("ROLLBACK;");
                }
                Err(err)
            }
        }
    }

    fn write_remaining_from_api_responses(
        &mut self,
        cache: &NpiCache,
        shutdown_requested: &Arc<AtomicBool>,
    ) -> Result<()> {
        if self.remaining.is_empty() {
            return Ok(());
        }
        println!(
            "Appending {} NPIs from cached API responses / sentinels...",
            format_count(self.remaining.len())
        );

        let mut api_rows = HashMap::new();
        let mut stmt = cache
            .conn
            .prepare(
                "
                SELECT
                    npi,
                    basic_json,
                    addresses_json,
                    practice_locations_json,
                    taxonomies_json,
                    identifiers_json,
                    other_names_json,
                    endpoints_json,
                    url,
                    error_message,
                    api_run_id,
                    requested_at_utc,
                    request_params_json,
                    results_json,
                    response_json_raw
                FROM npi_api_responses
                ",
            )
            .context("Failed preparing NPI API responses load query")?;
        let mut rows = stmt
            .query([])
            .context("Failed querying NPI API responses rows")?;
        while let Some(row) = rows.next().context("Failed iterating API response rows")? {
            let npi: String = row.get(0).context("Failed reading npi")?;
            api_rows.insert(
                npi,
                (
                    row.get::<usize, Option<String>>(1)?,
                    row.get::<usize, Option<String>>(2)?,
                    row.get::<usize, Option<String>>(3)?,
                    row.get::<usize, Option<String>>(4)?,
                    row.get::<usize, Option<String>>(5)?,
                    row.get::<usize, Option<String>>(6)?,
                    row.get::<usize, Option<String>>(7)?,
                    row.get::<usize, Option<String>>(8)?,
                    row.get::<usize, Option<String>>(9)?,
                    row.get::<usize, Option<String>>(10)?,
                    row.get::<usize, Option<String>>(11)?,
                    row.get::<usize, Option<String>>(12)?,
                    row.get::<usize, Option<String>>(13)?,
                    row.get::<usize, Option<String>>(14)?,
                ),
            );
        }

        let missing_requested_at = self.requested_at_utc.clone();
        let missing_params = json!({"source":"missing_cache"}).to_string();
        let missing_response_json = "{\"result_count\":0,\"results\":[]}".to_string();

        let mut processed = 0usize;
        for npi in self.unique_npis {
            let key = npi.as_str();
            if !self.remaining.contains(key) {
                continue;
            }
            processed += 1;
            if processed % 50_000 == 0 && shutdown_requested.load(Ordering::SeqCst) {
                println!("Shutdown requested; stopping NPI remaining export early.");
                break;
            }

            if let Some((
                basic_json,
                addresses_json,
                practice_locations_json,
                taxonomies_json,
                identifiers_json,
                other_names_json,
                endpoints_json,
                url,
                error_message,
                api_run_id,
                requested_at_utc,
                request_params_json,
                results_json,
                response_json_raw,
            )) = api_rows.get(key)
            {
                self.writer.push_row(&[
                    Some(key),
                    basic_json.as_deref(),
                    addresses_json.as_deref(),
                    practice_locations_json.as_deref(),
                    taxonomies_json.as_deref(),
                    identifiers_json.as_deref(),
                    other_names_json.as_deref(),
                    endpoints_json.as_deref(),
                    url.as_deref(),
                    error_message.as_deref(),
                    api_run_id.as_deref(),
                    requested_at_utc.as_deref(),
                    request_params_json.as_deref(),
                    results_json.as_deref(),
                    response_json_raw.as_deref(),
                ])?;
            } else {
                self.writer.push_row(&[
                    Some(key),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some("missing_cache"),
                    Some("missing_cache"),
                    Some(self.api_run_id.as_str()),
                    Some(missing_requested_at.as_str()),
                    Some(missing_params.as_str()),
                    Some("[]"),
                    Some(missing_response_json.as_str()),
                ])?;
            }

            self.remaining.remove(key);
            if self.remaining.is_empty() {
                break;
            }
        }

        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.writer.finish()
    }

    fn abort(self) -> Result<()> {
        self.writer.abort()
    }
}

fn apply_npi_lookup_progress_style(progress: &ProgressBar) {
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.green} {prefix:.bold} [{elapsed_precise}] [{bar:32.cyan/blue}] \
{pos}/{len} ({percent}%) {per_sec} eta {eta_precise} {msg}",
    ) {
        progress.set_style(style.progress_chars("=> "));
    }
}

fn apply_npi_retry_wait_style(progress: &ProgressBar) {
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.yellow} {prefix:.bold} [{elapsed_precise}] [{bar:32.yellow/blue}] \
{pos:>3}/{len}s {msg}",
    ) {
        progress.set_style(style.progress_chars("=> "));
    }
}

async fn run_npi_retry_wait_countdown(
    progress: &ProgressBar,
    retry_delay: Duration,
    retry_round: u32,
    max_retry_rounds: u32,
    pending_retry: usize,
    found: usize,
    not_found: usize,
    failed: usize,
    shutdown_requested: &Arc<AtomicBool>,
) -> bool {
    if retry_delay.is_zero() {
        return false;
    }

    let total_secs = retry_delay.as_secs();
    progress.set_prefix("NPI RETRY");
    progress.set_length(total_secs);
    progress.set_position(0);
    apply_npi_retry_wait_style(progress);

    for elapsed in 0..total_secs {
        if shutdown_requested.load(Ordering::SeqCst) {
            return true;
        }
        let remaining = total_secs.saturating_sub(elapsed);
        progress.set_message(format!(
            "round {retry_round}/{max_retry_rounds} retry in {remaining}s pending={pending_retry} ok={found} not_found={not_found} failed={failed}"
        ));
        sleep(Duration::from_secs(1)).await;
        progress.inc(1);
    }
    shutdown_requested.load(Ordering::SeqCst)
}

async fn resolve_missing_npis(
    cache: &NpiCache,
    missing_npis: Vec<String>,
    client: &Client,
    args: &Args,
    api_run_id: &str,
    progress_hub: Option<Arc<MultiProgress>>,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<(bool, Vec<NpiApiReferenceRow>)> {
    if missing_npis.is_empty() {
        return Ok((false, Vec::new()));
    }

    let total = missing_npis.len();
    let concurrency = args.concurrency.max(1);
    let min_interval = if args.requests_per_second == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(1.0 / args.requests_per_second as f64)
    };
    let next_slot = Arc::new(Mutex::new(Instant::now()));

    let progress = if let Some(hub) = &progress_hub {
        hub.add(ProgressBar::new(total as u64))
    } else {
        ProgressBar::new(total as u64)
    };
    progress.set_prefix("NPI");
    apply_npi_lookup_progress_style(&progress);
    progress.enable_steady_tick(Duration::from_millis(250));
    progress.set_message("starting lookups");

    let mut interrupted = shutdown_requested.load(Ordering::SeqCst);
    let mut reference_rows = Vec::new();
    let mut round_npis = missing_npis;
    let mut retry_round = 0u32;
    let max_retry_rounds = args.failure_retry_rounds;
    let base_retry_delay = Duration::from_secs(args.failure_retry_delay_seconds);

    let mut attempts = 0usize;
    let mut found = 0usize;
    let mut not_found = 0usize;
    let mut failed = 0usize;

    while !round_npis.is_empty() {
        if shutdown_requested.load(Ordering::SeqCst) {
            interrupted = true;
            break;
        }

        if retry_round > 0 && !base_retry_delay.is_zero() {
            let retry_delay = base_retry_delay
                .checked_mul(1u32 << retry_round.saturating_sub(1).min(20u32))
                .unwrap_or(Duration::from_secs(3600));
            let pending_retry = round_npis.len();
            let stop_requested = run_npi_retry_wait_countdown(
                &progress,
                retry_delay,
                retry_round,
                max_retry_rounds,
                pending_retry,
                found,
                not_found,
                failed,
                &shutdown_requested,
            )
            .await;
            if stop_requested {
                interrupted = true;
                break;
            }
            progress.set_prefix("NPI");
            progress.set_length(total as u64);
            progress.set_position((found + not_found + failed) as u64);
            apply_npi_lookup_progress_style(&progress);
            // We temporarily used this bar as a seconds countdown; reset the rate estimator
            // before resuming lookup throughput so per_sec reflects API lookups only.
            progress.reset_eta();
            progress.set_message(format!(
                "retry round {retry_round}/{max_retry_rounds} resumed pending={pending_retry}"
            ));
            if !min_interval.is_zero() {
                // Give the per_sec estimator one request-interval to stabilize before
                // new completions arrive, so the first post-retry sample is not inflated.
                sleep(min_interval).await;
            }
        }

        let can_retry_errors_again = retry_round < max_retry_rounds;
        let mut queue = round_npis.into_iter();
        let mut in_flight = FuturesUnordered::new();
        let mut next_round_npis = Vec::new();
        let mut retry_failover_triggered = false;

        for _ in 0..concurrency {
            if shutdown_requested.load(Ordering::SeqCst) {
                interrupted = true;
                break;
            }
            if let Some(npi) = queue.next() {
                in_flight.push(resolve_npi(
                    npi,
                    client.clone(),
                    args.api_base_url.clone(),
                    args.api_version.clone(),
                    api_run_id.to_string(),
                    args.max_retries.max(1),
                    Arc::clone(&next_slot),
                    min_interval,
                ));
            }
        }

        let mut pending_current_round = in_flight.len() + queue.len();
        while let Some((npi, result)) = in_flight.next().await {
            attempts += 1;
            pending_current_round = pending_current_round.saturating_sub(1);

            match result {
                NpiResolveResult::Found {
                    provider_name,
                    reference_row,
                } => {
                    cache.upsert_ok(&npi, &provider_name)?;
                    reference_rows.push(reference_row);
                    found += 1;
                    progress.inc(1);
                }
                NpiResolveResult::NotFound { reference_row } => {
                    cache.upsert_not_found(&npi)?;
                    reference_rows.push(reference_row);
                    not_found += 1;
                    progress.inc(1);
                }
                NpiResolveResult::Error {
                    error_message,
                    reference_row,
                } => {
                    cache.upsert_error(&npi, &error_message)?;
                    reference_rows.push(reference_row);
                    if can_retry_errors_again && !shutdown_requested.load(Ordering::SeqCst) {
                        next_round_npis.push(npi);
                        retry_failover_triggered = true;
                    } else {
                        failed += 1;
                        progress.inc(1);
                    }
                }
            }

            let remaining_in_round = if retry_failover_triggered {
                in_flight.len()
            } else {
                pending_current_round
            };
            let retry_queued = next_round_npis.len()
                + if retry_failover_triggered {
                    queue.len()
                } else {
                    0
                };
            let mode = if retry_failover_triggered {
                "retry_prep"
            } else {
                "lookup"
            };
            progress.set_message(format!(
                "mode={mode} ok={found} not_found={not_found} failed={failed} remaining={remaining_in_round} retry_queued={retry_queued}"
            ));

            if shutdown_requested.load(Ordering::SeqCst) {
                interrupted = true;
            } else if !retry_failover_triggered {
                if let Some(next_npi) = queue.next() {
                    in_flight.push(resolve_npi(
                        next_npi,
                        client.clone(),
                        args.api_base_url.clone(),
                        args.api_version.clone(),
                        api_run_id.to_string(),
                        args.max_retries.max(1),
                        Arc::clone(&next_slot),
                        min_interval,
                    ));
                }
            }
        }

        next_round_npis.extend(queue);
        round_npis = next_round_npis;

        if round_npis.is_empty() || interrupted {
            break;
        }
        retry_round = retry_round.saturating_add(1);
    }

    let settled = found + not_found + failed;
    if interrupted {
        progress.abandon_with_message(format!(
            "graceful stop: settled={settled}/{total} ok={found} not_found={not_found} failed={failed} pending_retry={} attempts={attempts}",
            round_npis.len()
        ));
    } else {
        progress.finish_with_message(format!(
            "done: settled={settled}/{total} ok={found} not_found={not_found} failed={failed} attempts={attempts}"
        ));
    }
    Ok((interrupted, reference_rows))
}

async fn resolve_npi(
    npi: String,
    client: Client,
    api_base_url: String,
    api_version: String,
    api_run_id: String,
    max_retries: u32,
    next_slot: Arc<Mutex<Instant>>,
    min_interval: Duration,
) -> (String, NpiResolveResult) {
    wait_for_rate_slot(&next_slot, min_interval).await;
    let result = fetch_npi_name(
        &client,
        &api_base_url,
        &api_version,
        &npi,
        &api_run_id,
        max_retries,
    )
    .await;
    (npi, result)
}

async fn fetch_npi_name(
    client: &Client,
    api_base_url: &str,
    api_version: &str,
    npi: &str,
    api_run_id: &str,
    max_retries: u32,
) -> NpiResolveResult {
    let request_params_json = json!({
        "version": api_version,
        "number": npi,
    })
    .to_string();
    let request_url =
        reqwest::Url::parse_with_params(api_base_url, &[("version", api_version), ("number", npi)])
            .map(|url| url.to_string())
            .unwrap_or_else(|_| format!("{api_base_url}?version={api_version}&number={npi}"));
    let requested_at_utc = now_unix_seconds().to_string();

    let make_base_row = || NpiApiReferenceRow {
        npi: npi.to_string(),
        basic_json: None,
        addresses_json: None,
        practice_locations_json: None,
        taxonomies_json: None,
        identifiers_json: None,
        other_names_json: None,
        endpoints_json: None,
        request_url: request_url.clone(),
        http_status: None,
        error_message: None,
        api_run_id: api_run_id.to_string(),
        requested_at_utc: requested_at_utc.clone(),
        request_params_json: request_params_json.clone(),
        results_json: None,
        response_json_raw: None,
    };

    let attempts = max_retries.max(1);
    let mut backoff = Duration::from_secs(1);

    for attempt in 1..=attempts {
        let response = client
            .get(api_base_url)
            .query(&[("version", api_version), ("number", npi)])
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    let body_text = match resp.text().await {
                        Ok(text) => text,
                        Err(err) => {
                            let mut row = make_base_row();
                            row.http_status = Some(status.as_u16() as i64);
                            row.error_message = Some(format!(
                                "Failed reading NPI API response body for {npi}: {err}"
                            ));
                            return NpiResolveResult::Error {
                                error_message: row.error_message.clone().unwrap_or_default(),
                                reference_row: row,
                            };
                        }
                    };

                    let body_value: Value = match serde_json::from_str(&body_text) {
                        Ok(value) => value,
                        Err(err) => {
                            let mut row = make_base_row();
                            row.http_status = Some(status.as_u16() as i64);
                            row.error_message =
                                Some(format!("Invalid NPI API JSON for {npi}: {err}"));
                            return NpiResolveResult::Error {
                                error_message: row.error_message.clone().unwrap_or_default(),
                                reference_row: row,
                            };
                        }
                    };

                    let mut row = build_npi_reference_row_from_value(
                        &body_value,
                        npi,
                        &request_url,
                        status.as_u16() as i64,
                        api_run_id,
                        &requested_at_utc,
                        &request_params_json,
                    );

                    let parsed: NpiApiResponse = match serde_json::from_value(body_value) {
                        Ok(parsed) => parsed,
                        Err(err) => {
                            row.error_message =
                                Some(format!("Failed decoding NPI API response for {npi}: {err}"));
                            return NpiResolveResult::Error {
                                error_message: row.error_message.clone().unwrap_or_default(),
                                reference_row: row,
                            };
                        }
                    };

                    return match extract_name_from_response(&parsed) {
                        Some(name) => NpiResolveResult::Found {
                            provider_name: name,
                            reference_row: row,
                        },
                        None => NpiResolveResult::NotFound { reference_row: row },
                    };
                }

                let retry_after = parse_retry_after(resp.headers().get(RETRY_AFTER));
                let body = resp.text().await.unwrap_or_default();
                if is_retryable_status(status) {
                    if attempt == attempts {
                        let mut row = make_base_row();
                        row.http_status = Some(status.as_u16() as i64);
                        let message = format!(
                            "NPI API retryable status {} for {} after {} attempts. Body: {}",
                            status,
                            npi,
                            attempts,
                            truncate_for_log(&body)
                        );
                        row.error_message = Some(message.clone());
                        return NpiResolveResult::Error {
                            error_message: message,
                            reference_row: row,
                        };
                    }
                    tokio::time::sleep(retry_after.unwrap_or(backoff)).await;
                    backoff = (backoff + backoff).min(Duration::from_secs(60));
                    continue;
                }

                let mut row = make_base_row();
                row.http_status = Some(status.as_u16() as i64);
                let message = format!(
                    "NPI API non-retryable status {} for {}. Body: {}",
                    status,
                    npi,
                    truncate_for_log(&body)
                );
                row.error_message = Some(message.clone());
                return NpiResolveResult::Error {
                    error_message: message,
                    reference_row: row,
                };
            }
            Err(err) => {
                if attempt == attempts {
                    let mut row = make_base_row();
                    let message = format!("NPI API request failed for {npi}: {err}");
                    row.error_message = Some(message.clone());
                    return NpiResolveResult::Error {
                        error_message: message,
                        reference_row: row,
                    };
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff + backoff).min(Duration::from_secs(60));
            }
        }
    }

    let mut row = make_base_row();
    let message = format!("Unexpected NPI API flow for {npi}");
    row.error_message = Some(message.clone());
    NpiResolveResult::Error {
        error_message: message,
        reference_row: row,
    }
}

fn extract_name_from_response(response: &NpiApiResponse) -> Option<String> {
    let first_result = response.results.first()?;
    let basic = first_result.basic.as_ref()?;

    let organization_name = basic
        .organization_name
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned);
    if organization_name.is_some() {
        return organization_name;
    }

    let first_name = basic
        .first_name
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let last_name = basic
        .last_name
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    match (first_name, last_name) {
        (Some(first), Some(last)) => Some(format!("{first} {last}")),
        (Some(first), None) => Some(first.to_string()),
        (None, Some(last)) => Some(last.to_string()),
        (None, None) => None,
    }
}
