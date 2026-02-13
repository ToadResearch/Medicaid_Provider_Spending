use anyhow::{Context, Result, anyhow};
use csv::Writer;
use duckdb::Connection;
use futures::{StreamExt, stream::FuturesUnordered};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, header::RETRY_AFTER};
use rusqlite::{Connection as SqliteConnection, OptionalExtension, params};
use serde_json::Value;
use std::{collections::HashSet, fs, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::{
    args::Args,
    common::{
        is_retryable_status, parse_retry_after, source_expr, truncate_for_log, wait_for_rate_slot,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HcpcsApiRecord {
    hcpcs_code: String,
    short_desc: String,
    long_desc: String,
    add_dt: String,
    act_eff_dt: String,
    term_dt: String,
    obsolete: bool,
    is_noc: bool,
}

struct HcpcsCache {
    conn: SqliteConnection,
}

impl HcpcsCache {
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
            CREATE TABLE IF NOT EXISTS hcpcs_cache (
                hcpcs_code TEXT NOT NULL,
                short_desc TEXT NOT NULL DEFAULT '',
                long_desc TEXT NOT NULL DEFAULT '',
                add_dt TEXT NOT NULL DEFAULT '',
                act_eff_dt TEXT NOT NULL DEFAULT '',
                term_dt TEXT NOT NULL DEFAULT '',
                obsolete TEXT NOT NULL DEFAULT '',
                is_noc TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                fetched_at_unix INTEGER NOT NULL,
                PRIMARY KEY (
                    hcpcs_code,
                    short_desc,
                    long_desc,
                    add_dt,
                    act_eff_dt,
                    term_dt,
                    obsolete,
                    is_noc,
                    status
                )
            );
            CREATE INDEX IF NOT EXISTS idx_hcpcs_cache_code_status
                ON hcpcs_cache(hcpcs_code, status);
            ",
        )
        .context("Failed initializing HCPCS cache schema")?;
        Ok(Self { conn })
    }

    fn classify_for_lookup(&self, codes: &[String]) -> Result<(usize, Vec<String>)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT 1 FROM hcpcs_cache
                 WHERE hcpcs_code = ?1 AND status IN ('ok', 'not_found')
                 LIMIT 1",
            )
            .context("Failed preparing HCPCS cache lookup statement")?;

        let mut resolved = 0usize;
        let mut missing = Vec::new();

        for code in codes {
            let exists: Option<i64> = stmt
                .query_row([code], |row| row.get(0))
                .optional()
                .with_context(|| format!("Failed HCPCS cache lookup for {code}"))?;
            if exists.is_some() {
                resolved += 1;
            } else {
                missing.push(code.clone());
            }
        }

        Ok((resolved, missing))
    }

    fn replace_with_ok_records(&self, code: &str, records: &[HcpcsApiRecord]) -> Result<()> {
        self.conn
            .execute("DELETE FROM hcpcs_cache WHERE hcpcs_code = ?1", [code])
            .with_context(|| format!("Failed clearing HCPCS cache rows for {code}"))?;

        for record in records {
            self.conn
                .execute(
                    "
                    INSERT INTO hcpcs_cache (
                        hcpcs_code,
                        short_desc,
                        long_desc,
                        add_dt,
                        act_eff_dt,
                        term_dt,
                        obsolete,
                        is_noc,
                        status,
                        error_message,
                        fetched_at_unix
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'ok', '', strftime('%s', 'now'))
                    ",
                    params![
                        record.hcpcs_code,
                        record.short_desc,
                        record.long_desc,
                        record.add_dt,
                        record.act_eff_dt,
                        record.term_dt,
                        if record.obsolete { "true" } else { "false" },
                        if record.is_noc { "true" } else { "false" },
                    ],
                )
                .with_context(|| format!("Failed inserting HCPCS cache row for {code}"))?;
        }
        Ok(())
    }

    fn set_not_found(&self, code: &str, reason: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM hcpcs_cache WHERE hcpcs_code = ?1", [code])
            .with_context(|| format!("Failed clearing HCPCS cache rows for {code}"))?;
        self.conn
            .execute(
                "
                INSERT INTO hcpcs_cache (
                    hcpcs_code,
                    short_desc,
                    long_desc,
                    add_dt,
                    act_eff_dt,
                    term_dt,
                    obsolete,
                    is_noc,
                    status,
                    error_message,
                    fetched_at_unix
                )
                VALUES (?1, '', '', '', '', '', '', '', 'not_found', ?2, strftime('%s', 'now'))
                ",
                params![code, reason],
            )
            .with_context(|| format!("Failed inserting HCPCS not_found sentinel for {code}"))?;
        Ok(())
    }

    fn set_error(&self, code: &str, message: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM hcpcs_cache WHERE hcpcs_code = ?1", [code])
            .with_context(|| format!("Failed clearing HCPCS cache rows for {code}"))?;
        self.conn
            .execute(
                "
                INSERT INTO hcpcs_cache (
                    hcpcs_code,
                    short_desc,
                    long_desc,
                    add_dt,
                    act_eff_dt,
                    term_dt,
                    obsolete,
                    is_noc,
                    status,
                    error_message,
                    fetched_at_unix
                )
                VALUES (?1, '', '', '', '', '', '', '', 'error', ?2, strftime('%s', 'now'))
                ",
                params![code, truncate_for_log(message)],
            )
            .with_context(|| format!("Failed inserting HCPCS error sentinel for {code}"))?;
        Ok(())
    }

    fn export_mapping_csv(&self, output_path: &Path) -> Result<()> {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed creating HCPCS mapping parent directory {}",
                    parent.display()
                )
            })?;
        }

        let file_name = output_path
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("hcpcs_code_mapping.csv");
        let tmp_path = output_path.with_file_name(format!("{file_name}.tmp"));

        let mut writer = Writer::from_path(&tmp_path).with_context(|| {
            format!(
                "Failed creating temp HCPCS mapping CSV {}",
                tmp_path.display()
            )
        })?;
        writer
            .write_record([
                "hcpcs_code",
                "short_desc",
                "long_desc",
                "add_dt",
                "act_eff_dt",
                "term_dt",
                "obsolete",
                "is_noc",
                "status",
                "fetched_at_unix",
            ])
            .context("Failed writing HCPCS mapping CSV header")?;

        // Export all successful HCPCS records (including NOC).
        // Selection preference (non-NOC preferred, NOC fallback) is applied during enrichment.
        // `is_noc` field definition:
        // https://clinicaltables.nlm.nih.gov/apidoc/hcpcs/v3/doc.html
        let mut stmt = self
            .conn
            .prepare(
                "
                SELECT
                    hcpcs_code,
                    short_desc,
                    long_desc,
                    add_dt,
                    act_eff_dt,
                    term_dt,
                    obsolete,
                    is_noc,
                    status,
                    fetched_at_unix
                FROM hcpcs_cache
                WHERE status = 'ok'
                ORDER BY
                    hcpcs_code,
                    CASE WHEN LOWER(COALESCE(is_noc, 'false')) = 'false' THEN 0 ELSE 1 END,
                    act_eff_dt,
                    add_dt,
                    term_dt,
                    short_desc
                ",
            )
            .context("Failed preparing HCPCS mapping export query")?;
        let mut rows = stmt
            .query([])
            .context("Failed querying HCPCS mapping rows")?;

        while let Some(row) = rows.next().context("Failed iterating HCPCS mapping rows")? {
            let hcpcs_code: String = row.get(0).context("Failed reading hcpcs_code")?;
            let short_desc: String = row.get(1).context("Failed reading short_desc")?;
            let long_desc: String = row.get(2).context("Failed reading long_desc")?;
            let add_dt: String = row.get(3).context("Failed reading add_dt")?;
            let act_eff_dt: String = row.get(4).context("Failed reading act_eff_dt")?;
            let term_dt: String = row.get(5).context("Failed reading term_dt")?;
            let obsolete: String = row.get(6).context("Failed reading obsolete")?;
            let is_noc: String = row.get(7).context("Failed reading is_noc")?;
            let status: String = row.get(8).context("Failed reading status")?;
            let fetched_at_unix: i64 = row.get(9).context("Failed reading fetched_at_unix")?;

            writer
                .write_record([
                    hcpcs_code,
                    short_desc,
                    long_desc,
                    add_dt,
                    act_eff_dt,
                    term_dt,
                    obsolete,
                    is_noc,
                    status,
                    fetched_at_unix.to_string(),
                ])
                .context("Failed writing HCPCS mapping row")?;
        }
        writer
            .flush()
            .context("Failed flushing HCPCS mapping CSV writer")?;

        fs::rename(&tmp_path, output_path).with_context(|| {
            format!(
                "Failed moving temp HCPCS mapping {} to {}",
                tmp_path.display(),
                output_path.display()
            )
        })?;
        Ok(())
    }
}

pub async fn build_hcpcs_mapping(
    args: &Args,
    client: &Client,
    input_path: &Path,
    cache_db: &Path,
    mapping_csv: &Path,
) -> Result<()> {
    println!("Extracting unique HCPCS codes...");
    let unique_codes = extract_unique_hcpcs_codes(input_path)?;
    println!(
        "Discovered {} unique HCPCS codes in source data.",
        unique_codes.len()
    );

    let cache = HcpcsCache::open(cache_db)?;
    let (resolved_count, mut missing_codes) = cache.classify_for_lookup(&unique_codes)?;
    println!(
        "HCPCS cache status: {} resolved in cache, {} unresolved.",
        resolved_count,
        missing_codes.len()
    );

    if let Some(limit) = args.max_new_lookups {
        if missing_codes.len() > limit {
            println!(
                "Applying --max-new-lookups={} to HCPCS lookups (from {}).",
                limit,
                missing_codes.len()
            );
            missing_codes.truncate(limit);
        }
    }

    if args.skip_api {
        println!("--skip-api set; unresolved HCPCS codes remain unresolved.");
    } else if !missing_codes.is_empty() {
        resolve_missing_hcpcs(&cache, missing_codes, client, args).await?;
    }

    cache.export_mapping_csv(mapping_csv)?;
    println!("Wrote HCPCS mapping CSV {}", mapping_csv.display());
    Ok(())
}

fn extract_unique_hcpcs_codes(input_path: &Path) -> Result<Vec<String>> {
    let conn = Connection::open_in_memory().context("Failed opening DuckDB")?;
    let source = source_expr(input_path)?;
    let query = format!(
        "
        WITH src AS (
            SELECT * FROM {source}
        )
        SELECT DISTINCT TRIM(CAST(HCPCS_CODE AS VARCHAR)) AS hcpcs_code
        FROM src
        WHERE HCPCS_CODE IS NOT NULL
          AND TRIM(CAST(HCPCS_CODE AS VARCHAR)) <> ''
        "
    );

    let mut stmt = conn
        .prepare(&query)
        .context("Failed preparing unique HCPCS query")?;
    let rows = stmt
        .query_map([], |row| row.get::<usize, String>(0))
        .context("Failed running unique HCPCS query")?;

    let mut codes = Vec::new();
    for row in rows {
        codes.push(row.context("Failed reading HCPCS row")?);
    }
    Ok(codes)
}

async fn resolve_missing_hcpcs(
    cache: &HcpcsCache,
    missing_codes: Vec<String>,
    client: &Client,
    args: &Args,
) -> Result<()> {
    if missing_codes.is_empty() {
        return Ok(());
    }

    let total = missing_codes.len();
    let concurrency = args.concurrency.max(1);
    let min_interval = if args.requests_per_second == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(1.0 / args.requests_per_second as f64)
    };
    let next_slot = Arc::new(Mutex::new(Instant::now()));

    let progress = ProgressBar::new(total as u64);
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.green} [HCPCS {elapsed_precise}] [{bar:40.magenta/blue}] {pos}/{len} {msg}",
    ) {
        progress.set_style(style.progress_chars("=> "));
    }
    progress.set_message("starting lookups");

    let mut queue = missing_codes.into_iter();
    let mut in_flight = FuturesUnordered::new();

    for _ in 0..concurrency {
        if let Some(code) = queue.next() {
            in_flight.push(resolve_hcpcs(
                code,
                client.clone(),
                args.hcpcs_api_base_url.clone(),
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

    while let Some((code, result)) = in_flight.next().await {
        processed += 1;
        progress.inc(1);

        match result {
            Ok(mut records) => {
                records.retain(|record| record.hcpcs_code.eq_ignore_ascii_case(&code));
                let mut dedup = HashSet::new();
                records.retain(|record| dedup.insert(record.clone()));

                if records.is_empty() {
                    cache.set_not_found(&code, "not_found")?;
                    not_found += 1;
                } else {
                    // Keep both non-NOC and NOC records so enrichment can prefer non-NOC
                    // and still fall back to NOC when that's all we have.
                    cache.replace_with_ok_records(&code, &records)?;
                    found += 1;
                }
            }
            Err(err) => {
                cache.set_error(&code, &err.to_string())?;
                failed += 1;
            }
        }

        progress.set_message(format!("ok={found} not_found={not_found} failed={failed}"));

        if let Some(next_code) = queue.next() {
            in_flight.push(resolve_hcpcs(
                next_code,
                client.clone(),
                args.hcpcs_api_base_url.clone(),
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

async fn resolve_hcpcs(
    code: String,
    client: Client,
    api_base_url: String,
    max_retries: u32,
    next_slot: Arc<Mutex<Instant>>,
    min_interval: Duration,
) -> (String, Result<Vec<HcpcsApiRecord>>) {
    wait_for_rate_slot(&next_slot, min_interval).await;
    let result = fetch_hcpcs_records(&client, &api_base_url, &code, max_retries).await;
    (code, result)
}

async fn fetch_hcpcs_records(
    client: &Client,
    api_base_url: &str,
    hcpcs_code: &str,
    max_retries: u32,
) -> Result<Vec<HcpcsApiRecord>> {
    let attempts = max_retries.max(1);
    let mut backoff = Duration::from_secs(1);

    for attempt in 1..=attempts {
        let code_filter = format!("code:{hcpcs_code}");
        let response = client
            .get(api_base_url)
            .query(&[
                ("terms", hcpcs_code),
                ("sf", "code"),
                ("q", code_filter.as_str()),
                ("count", "20"),
                ("df", "code,display"),
                (
                    "ef",
                    "short_desc,long_desc,add_dt,term_dt,act_eff_dt,obsolete,is_noc",
                ),
            ])
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    let body: Value = resp
                        .json()
                        .await
                        .with_context(|| format!("Invalid HCPCS API JSON for {hcpcs_code}"))?;
                    return parse_hcpcs_payload(hcpcs_code, &body);
                }

                let retry_after = parse_retry_after(resp.headers().get(RETRY_AFTER));
                let body = resp.text().await.unwrap_or_default();
                if is_retryable_status(status) {
                    if attempt == attempts {
                        return Err(anyhow!(
                            "HCPCS API retryable status {} for {} after {} attempts. Body: {}",
                            status,
                            hcpcs_code,
                            attempts,
                            truncate_for_log(&body)
                        ));
                    }
                    tokio::time::sleep(retry_after.unwrap_or(backoff)).await;
                    backoff = (backoff + backoff).min(Duration::from_secs(60));
                    continue;
                }

                return Err(anyhow!(
                    "HCPCS API non-retryable status {} for {}. Body: {}",
                    status,
                    hcpcs_code,
                    truncate_for_log(&body)
                ));
            }
            Err(err) => {
                if attempt == attempts {
                    return Err(anyhow!("HCPCS API request failed for {hcpcs_code}: {err}"));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff + backoff).min(Duration::from_secs(60));
            }
        }
    }

    Err(anyhow!("Unexpected HCPCS API flow for {hcpcs_code}"))
}

fn parse_hcpcs_payload(requested_code: &str, payload: &Value) -> Result<Vec<HcpcsApiRecord>> {
    let arr = payload
        .as_array()
        .context("HCPCS API payload is not an array")?;
    let code_values = arr
        .get(1)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let extra = arr.get(2).unwrap_or(&Value::Null);

    let mut records = Vec::new();
    for idx in 0..code_values.len() {
        let code = value_to_string(&code_values[idx]).trim().to_string();
        if code.is_empty() || !code.eq_ignore_ascii_case(requested_code) {
            continue;
        }

        let short_desc = field_value(extra, "short_desc", idx).trim().to_string();
        let long_desc = field_value(extra, "long_desc", idx).trim().to_string();
        let add_dt = normalize_yyyymmdd(&field_value(extra, "add_dt", idx));
        let act_eff_dt = normalize_yyyymmdd(&field_value(extra, "act_eff_dt", idx));
        let term_dt = normalize_yyyymmdd(&field_value(extra, "term_dt", idx));
        let obsolete = parse_bool_flag(&field_value(extra, "obsolete", idx)).unwrap_or(false);
        let is_noc = parse_bool_flag(&field_value(extra, "is_noc", idx)).unwrap_or(false);

        records.push(HcpcsApiRecord {
            hcpcs_code: code,
            short_desc,
            long_desc,
            add_dt,
            act_eff_dt,
            term_dt,
            obsolete,
            is_noc,
        });
    }

    Ok(records)
}

fn field_value(extra: &Value, field: &str, idx: usize) -> String {
    let Some(obj) = extra.as_object() else {
        return String::new();
    };
    let Some(value) = obj.get(field) else {
        return String::new();
    };
    match value {
        Value::Array(items) => items.get(idx).map(value_to_string).unwrap_or_default(),
        _ => value_to_string(value),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        _ => value.to_string(),
    }
}

fn normalize_yyyymmdd(value: &str) -> String {
    let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() == 8 {
        digits
    } else {
        String::new()
    }
}

fn parse_bool_flag(value: &str) -> Option<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "true" | "t" | "1" | "yes" | "y" => Some(true),
        "false" | "f" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}
