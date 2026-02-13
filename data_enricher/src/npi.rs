use anyhow::{Context, Result, anyhow};
use csv::Writer;
use duckdb::Connection;
use futures::{StreamExt, stream::FuturesUnordered};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, header::RETRY_AFTER};
use rusqlite::{Connection as SqliteConnection, OptionalExtension, params};
use serde::Deserialize;
use std::{fs, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::{
    args::Args,
    common::{
        is_retryable_status, parse_retry_after, source_expr, truncate_for_log, wait_for_rate_slot,
    },
};

struct NpiCache {
    conn: SqliteConnection,
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

enum NpiLookupOutcome {
    Found(String),
    NotFound,
}

pub async fn build_npi_mapping(
    args: &Args,
    client: &Client,
    input_path: &Path,
    cache_db: &Path,
    mapping_csv: &Path,
) -> Result<()> {
    println!("Extracting unique NPIs...");
    let unique_npis = extract_unique_npis(input_path)?;
    println!(
        "Discovered {} unique NPIs in source data.",
        unique_npis.len()
    );

    let cache = NpiCache::open(cache_db)?;
    let (resolved_count, mut missing_npis) = cache.classify_for_lookup(&unique_npis)?;
    println!(
        "NPI cache status: {} resolved in cache, {} unresolved.",
        resolved_count,
        missing_npis.len()
    );

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

    if args.skip_api {
        println!("--skip-api set; unresolved NPIs remain unresolved.");
    } else if !missing_npis.is_empty() {
        resolve_missing_npis(&cache, missing_npis, client, args).await?;
    }

    cache.export_mapping_csv(mapping_csv)?;
    println!("Wrote NPI mapping CSV {}", mapping_csv.display());
    Ok(())
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

async fn resolve_missing_npis(
    cache: &NpiCache,
    missing_npis: Vec<String>,
    client: &Client,
    args: &Args,
) -> Result<()> {
    if missing_npis.is_empty() {
        return Ok(());
    }

    let total = missing_npis.len();
    let concurrency = args.concurrency.max(1);
    let min_interval = if args.requests_per_second == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(1.0 / args.requests_per_second as f64)
    };
    let next_slot = Arc::new(Mutex::new(Instant::now()));

    let progress = ProgressBar::new(total as u64);
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.green} [NPI {elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
    ) {
        progress.set_style(style.progress_chars("=> "));
    }
    progress.set_message("starting lookups");

    let mut queue = missing_npis.into_iter();
    let mut in_flight = FuturesUnordered::new();

    for _ in 0..concurrency {
        if let Some(npi) = queue.next() {
            in_flight.push(resolve_npi(
                npi,
                client.clone(),
                args.api_base_url.clone(),
                args.api_version.clone(),
                args.max_retries.max(1),
                Arc::clone(&next_slot),
                min_interval,
            ));
        }
    }

    let mut processed = 0usize;
    let mut found = 0usize;
    let mut not_found = 0usize;
    let mut failed = 0usize;

    while let Some((npi, result)) = in_flight.next().await {
        processed += 1;
        progress.inc(1);

        match result {
            Ok(NpiLookupOutcome::Found(name)) => {
                cache.upsert_ok(&npi, &name)?;
                found += 1;
            }
            Ok(NpiLookupOutcome::NotFound) => {
                cache.upsert_not_found(&npi)?;
                not_found += 1;
            }
            Err(err) => {
                cache.upsert_error(&npi, &err.to_string())?;
                failed += 1;
            }
        }

        progress.set_message(format!("ok={found} not_found={not_found} failed={failed}"));

        if let Some(next_npi) = queue.next() {
            in_flight.push(resolve_npi(
                next_npi,
                client.clone(),
                args.api_base_url.clone(),
                args.api_version.clone(),
                args.max_retries.max(1),
                Arc::clone(&next_slot),
                min_interval,
            ));
        }
    }

    progress.finish_with_message(format!(
        "done: processed={processed} ok={found} not_found={not_found} failed={failed}"
    ));
    Ok(())
}

async fn resolve_npi(
    npi: String,
    client: Client,
    api_base_url: String,
    api_version: String,
    max_retries: u32,
    next_slot: Arc<Mutex<Instant>>,
    min_interval: Duration,
) -> (String, Result<NpiLookupOutcome>) {
    wait_for_rate_slot(&next_slot, min_interval).await;
    let result = fetch_npi_name(&client, &api_base_url, &api_version, &npi, max_retries).await;
    (npi, result)
}

async fn fetch_npi_name(
    client: &Client,
    api_base_url: &str,
    api_version: &str,
    npi: &str,
    max_retries: u32,
) -> Result<NpiLookupOutcome> {
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
                    let body: NpiApiResponse = resp
                        .json()
                        .await
                        .with_context(|| format!("Invalid NPI API JSON for {npi}"))?;
                    return Ok(match extract_name_from_response(&body) {
                        Some(name) => NpiLookupOutcome::Found(name),
                        None => NpiLookupOutcome::NotFound,
                    });
                }

                let retry_after = parse_retry_after(resp.headers().get(RETRY_AFTER));
                let body = resp.text().await.unwrap_or_default();
                if is_retryable_status(status) {
                    if attempt == attempts {
                        return Err(anyhow!(
                            "NPI API retryable status {} for {} after {} attempts. Body: {}",
                            status,
                            npi,
                            attempts,
                            truncate_for_log(&body)
                        ));
                    }
                    tokio::time::sleep(retry_after.unwrap_or(backoff)).await;
                    backoff = (backoff + backoff).min(Duration::from_secs(60));
                    continue;
                }

                return Err(anyhow!(
                    "NPI API non-retryable status {} for {}. Body: {}",
                    status,
                    npi,
                    truncate_for_log(&body)
                ));
            }
            Err(err) => {
                if attempt == attempts {
                    return Err(anyhow!("NPI API request failed for {npi}: {err}"));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff + backoff).min(Duration::from_secs(60));
            }
        }
    }

    Err(anyhow!("Unexpected NPI API flow for {npi}"))
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
