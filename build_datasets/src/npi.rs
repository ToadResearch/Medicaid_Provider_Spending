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
    collections::HashSet,
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

    fn export_api_responses_parquet(&self, output_path: &Path) -> Result<()> {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed creating NPI API responses parent directory {}",
                    parent.display()
                )
            })?;
        }

        let file_name = output_path
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("npi.parquet");
        let tmp_csv_path = output_path.with_file_name(format!("{file_name}.tmp.csv"));
        let tmp_parquet_path = output_path.with_file_name(format!("{file_name}.tmp"));
        let null_token = "\\N";

        let mut writer = Writer::from_path(&tmp_csv_path).with_context(|| {
            format!(
                "Failed creating temp NPI API responses CSV {}",
                tmp_csv_path.display()
            )
        })?;
        writer
            .write_record([
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
            ])
            .context("Failed writing NPI API responses header")?;

        let mut stmt = self
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
                ORDER BY npi
                ",
            )
            .context("Failed preparing NPI API responses export query")?;
        let mut rows = stmt
            .query([])
            .context("Failed querying NPI API responses rows")?;

        while let Some(row) = rows
            .next()
            .context("Failed iterating NPI API responses rows")?
        {
            let npi: String = row.get(0).context("Failed reading npi")?;
            let basic_json: Option<String> = row.get(1).context("Failed reading basic_json")?;
            let addresses_json: Option<String> =
                row.get(2).context("Failed reading addresses_json")?;
            let practice_locations_json: Option<String> = row
                .get(3)
                .context("Failed reading practice_locations_json")?;
            let taxonomies_json: Option<String> =
                row.get(4).context("Failed reading taxonomies_json")?;
            let identifiers_json: Option<String> =
                row.get(5).context("Failed reading identifiers_json")?;
            let other_names_json: Option<String> =
                row.get(6).context("Failed reading other_names_json")?;
            let endpoints_json: Option<String> =
                row.get(7).context("Failed reading endpoints_json")?;
            let url: Option<String> = row.get(8).context("Failed reading url")?;
            let error_message: Option<String> =
                row.get(9).context("Failed reading error_message")?;
            let api_run_id: Option<String> = row.get(10).context("Failed reading api_run_id")?;
            let requested_at_utc: Option<String> =
                row.get(11).context("Failed reading requested_at_utc")?;
            let request_params_json: Option<String> =
                row.get(12).context("Failed reading request_params_json")?;
            let results_json: Option<String> =
                row.get(13).context("Failed reading results_json")?;
            let response_json_raw: Option<String> =
                row.get(14).context("Failed reading response_json_raw")?;

            let field = |value: Option<String>| value.unwrap_or_else(|| null_token.to_string());

            writer
                .write_record([
                    npi,
                    field(basic_json),
                    field(addresses_json),
                    field(practice_locations_json),
                    field(taxonomies_json),
                    field(identifiers_json),
                    field(other_names_json),
                    field(endpoints_json),
                    field(url),
                    field(error_message),
                    field(api_run_id),
                    field(requested_at_utc),
                    field(request_params_json),
                    field(results_json),
                    field(response_json_raw),
                ])
                .context("Failed writing NPI API responses row")?;
        }

        writer
            .flush()
            .context("Failed flushing NPI API responses CSV writer")?;

        let conn = Connection::open_in_memory()
            .context("Failed opening DuckDB for NPI API responses export")?;
        let csv_escaped = sql_escape_path(&tmp_csv_path);
        let parquet_escaped = sql_escape_path(&tmp_parquet_path);
        conn.execute_batch(&format!(
            "COPY (
                SELECT * FROM read_csv_auto('{csv_escaped}', header=true, nullstr='{null_token}', all_varchar=true)
            ) TO '{parquet_escaped}' (FORMAT PARQUET);"
        ))
        .context("Failed writing NPI API responses parquet")?;

        fs::remove_file(&tmp_csv_path).with_context(|| {
            format!(
                "Failed deleting temp NPI API responses CSV {}",
                tmp_csv_path.display()
            )
        })?;
        fs::rename(&tmp_parquet_path, output_path).with_context(|| {
            format!(
                "Failed moving temp NPI API responses parquet {} to {}",
                tmp_parquet_path.display(),
                output_path.display()
            )
        })?;
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
    let target_npis: HashSet<&str> = unique_npis.iter().map(String::as_str).collect();

    let mut cache = NpiCache::open(cache_db)?;
    let (resolved_before_bulk, _) = cache.classify_for_lookup(&unique_npis)?;
    let mut monthly_loaded = 0usize;
    let mut weekly_loaded = 0usize;
    let mut used_monthly_file: Option<PathBuf> = None;
    let mut used_weekly_file: Option<PathBuf> = None;

    if !args.skip_nppes_bulk {
        let monthly_csv = select_latest_nppes_csv(nppes_monthly_dir)?;
        let weekly_csv = select_latest_nppes_csv(nppes_weekly_dir)?;
        if monthly_csv.is_none() && weekly_csv.is_none() {
            println!(
                "No local NPPES bulk files found under {} and {}. Falling back to cache/API.",
                nppes_monthly_dir.display(),
                nppes_weekly_dir.display()
            );
        } else {
            println!("Loading local NPPES bulk files before API fallback...");
            if let Some(monthly) = monthly_csv {
                let loaded = load_nppes_csv_into_cache(
                    &cache,
                    &monthly,
                    &target_npis,
                    &shutdown_requested,
                    "monthly",
                )?;
                monthly_loaded = loaded;
                used_monthly_file = Some(monthly.clone());
                println!("Monthly NPPES preload matched {} NPIs.", loaded);
            }
            if let Some(weekly) = weekly_csv {
                let loaded = load_nppes_csv_into_cache(
                    &cache,
                    &weekly,
                    &target_npis,
                    &shutdown_requested,
                    "weekly",
                )?;
                weekly_loaded = loaded;
                used_weekly_file = Some(weekly.clone());
                println!(
                    "Weekly NPPES preload {} matched {} NPIs.",
                    weekly.display(),
                    loaded
                );
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
    cache.export_api_responses_parquet(api_responses_parquet)?;
    println!(
        "Wrote NPI API responses dataset {}",
        api_responses_parquet.display()
    );
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
    let (_, missing_npis) = cache.classify_for_lookup(&unique_npis)?;
    Ok(missing_npis.is_empty())
}

pub fn export_npi_api_responses_parquet(cache_db: &Path, output_path: &Path) -> Result<()> {
    let cache = NpiCache::open(cache_db)?;
    cache.export_api_responses_parquet(output_path)
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

// NOTE: NPI API response parquet export is handled via NpiCache::export_api_responses_parquet,
// backed by the `npi_api_responses` table.

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

fn load_nppes_csv_into_cache(
    cache: &NpiCache,
    csv_path: &Path,
    target_npis: &HashSet<&str>,
    shutdown_requested: &Arc<AtomicBool>,
    label: &str,
) -> Result<usize> {
    println!(
        "Preloading NPI names from {} file {}",
        label,
        csv_path.display()
    );
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .with_context(|| format!("Failed opening NPPES CSV {}", csv_path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("Failed reading NPPES headers from {}", csv_path.display()))?
        .clone();

    let npi_idx = header_index(&headers, "NPI")?;
    let org_idx = headers
        .iter()
        .position(|h| h.trim() == "Provider Organization Name (Legal Business Name)");
    let first_idx = headers
        .iter()
        .position(|h| h.trim() == "Provider First Name");
    let last_idx = headers
        .iter()
        .position(|h| h.trim() == "Provider Last Name (Legal Name)");

    cache
        .conn
        .execute_batch("BEGIN IMMEDIATE TRANSACTION;")
        .context("Failed beginning NPPES preload transaction")?;

    let preload_result: Result<(usize, usize)> = (|| {
        let mut stmt = cache
            .conn
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
            .context("Failed preparing NPPES preload upsert statement")?;

        let mut processed = 0usize;
        let mut loaded = 0usize;
        for row in reader.records() {
            let row =
                row.with_context(|| format!("Failed reading record in {}", csv_path.display()))?;
            processed += 1;

            if processed % 50_000 == 0 {
                if shutdown_requested.load(Ordering::SeqCst) {
                    println!(
                        "Shutdown requested while reading {}. Committing partial NPPES preload.",
                        csv_path.display()
                    );
                    break;
                }
                if processed % 1_000_000 == 0 {
                    println!(
                        "Scanned {} rows from {} (loaded {}).",
                        processed, label, loaded
                    );
                }
            }

            let npi = row.get(npi_idx).unwrap_or("").trim();
            if npi.is_empty() || !target_npis.contains(npi) {
                continue;
            }

            let org_name = org_idx
                .and_then(|idx| row.get(idx))
                .map(str::trim)
                .unwrap_or("");
            let first_name = first_idx
                .and_then(|idx| row.get(idx))
                .map(str::trim)
                .unwrap_or("");
            let last_name = last_idx
                .and_then(|idx| row.get(idx))
                .map(str::trim)
                .unwrap_or("");
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

            if provider_name.is_empty() {
                continue;
            }

            stmt.execute(params![npi, provider_name])
                .with_context(|| format!("Failed upserting preloaded NPI {npi}"))?;
            loaded += 1;
        }
        Ok((processed, loaded))
    })();

    let (processed, loaded) = match preload_result {
        Ok(stats) => {
            cache
                .conn
                .execute_batch("COMMIT;")
                .context("Failed committing NPPES preload transaction")?;
            stats
        }
        Err(err) => {
            let _ = cache.conn.execute_batch("ROLLBACK;");
            return Err(err);
        }
    };

    println!(
        "Finished {} preload from {}: scanned {}, loaded {} provider names.",
        label,
        csv_path.display(),
        processed,
        loaded
    );
    Ok(loaded)
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
