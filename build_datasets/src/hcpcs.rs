use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord, Writer};
use duckdb::Connection;
use futures::{StreamExt, stream::FuturesUnordered};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{Client, header::RETRY_AFTER};
use rusqlite::{Connection as SqliteConnection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::IsTerminal,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
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

#[derive(Debug, Clone)]
struct HcpcsApiReferenceRow {
    hcpcs_code: String,
    ef_short_desc_json: Option<String>,
    ef_long_desc_json: Option<String>,
    ef_add_dt_json: Option<String>,
    ef_act_eff_dt_json: Option<String>,
    ef_term_dt_json: Option<String>,
    ef_obsolete_json: Option<String>,
    ef_is_noc_json: Option<String>,
    response_total_count: Option<i64>,
    response_codes_json: Option<String>,
    response_display_json: Option<String>,
    response_extra_fields_json: Option<String>,
    request_url: String,
    http_status: Option<i64>,
    error_message: Option<String>,
    api_run_id: String,
    requested_at_utc: String,
    request_params_json: String,
    response_json_raw: Option<String>,
}

enum HcpcsResolveResult {
    Found {
        records: Vec<HcpcsApiRecord>,
        reference_row: HcpcsApiReferenceRow,
    },
    NotFound {
        reason: String,
        reference_row: HcpcsApiReferenceRow,
    },
    Error {
        error_message: String,
        reference_row: HcpcsApiReferenceRow,
    },
}

struct HcpcsCache {
    conn: SqliteConnection,
}

#[derive(Debug, Clone)]
pub struct UnresolvedHcpcsEntry {
    pub hcpcs_code: String,
    pub status: String,
    pub error_message: Option<String>,
    pub fetched_at_unix: Option<i64>,
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
                 WHERE hcpcs_code = ?1 COLLATE NOCASE AND status IN ('ok', 'not_found')
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

    fn has_ok_record(&self, code: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT 1 FROM hcpcs_cache
                 WHERE hcpcs_code = ?1 COLLATE NOCASE AND status = 'ok'
                 LIMIT 1",
            )
            .context("Failed preparing HCPCS ok-status lookup statement")?;
        let exists: Option<i64> = stmt
            .query_row([code], |row| row.get(0))
            .optional()
            .with_context(|| format!("Failed HCPCS ok-status lookup for {code}"))?;
        Ok(exists.is_some())
    }

    fn replace_with_ok_records(&self, code: &str, records: &[HcpcsApiRecord]) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM hcpcs_cache WHERE hcpcs_code = ?1 COLLATE NOCASE",
                [code],
            )
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
            .execute(
                "DELETE FROM hcpcs_cache WHERE hcpcs_code = ?1 COLLATE NOCASE",
                [code],
            )
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
            .execute(
                "DELETE FROM hcpcs_cache WHERE hcpcs_code = ?1 COLLATE NOCASE",
                [code],
            )
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

fn normalize_header_name(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn find_header_index(headers: &StringRecord, aliases: &[&str]) -> Option<usize> {
    let header_norm: Vec<String> = headers.iter().map(normalize_header_name).collect();
    for alias in aliases {
        let target = normalize_header_name(alias);
        if let Some((idx, _)) = header_norm.iter().enumerate().find(|(_, h)| **h == target) {
            return Some(idx);
        }
    }
    None
}

fn field_at(record: &StringRecord, idx: Option<usize>) -> String {
    idx.and_then(|i| record.get(i))
        .map(str::trim)
        .unwrap_or("")
        .to_string()
}

fn parse_boolish(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "t" | "yes" | "y"
    )
}

fn normalize_hcpcs_code(raw: &str) -> Option<String> {
    let mut compact: String = raw.trim().chars().filter(|c| !c.is_whitespace()).collect();
    if compact.is_empty() {
        return None;
    }
    if compact.ends_with(".0") {
        compact.truncate(compact.len().saturating_sub(2));
    }
    let normalized = compact.to_ascii_uppercase();
    let valid = normalized.len() == 5 && normalized.chars().all(|c| c.is_ascii_alphanumeric());
    if valid { Some(normalized) } else { None }
}

fn load_local_hcpcs_fallback_records(
    fallback_csv: &Path,
    verbose: bool,
) -> Result<HashMap<String, Vec<HcpcsApiRecord>>> {
    if !fallback_csv.exists() {
        return Ok(HashMap::new());
    }

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(fallback_csv)
        .with_context(|| {
            format!(
                "Failed opening local HCPCS fallback CSV {}",
                fallback_csv.display()
            )
        })?;
    let headers = reader
        .headers()
        .with_context(|| {
            format!(
                "Failed reading local HCPCS fallback CSV headers {}",
                fallback_csv.display()
            )
        })?
        .clone();

    let code_idx = find_header_index(
        &headers,
        &[
            "hcpcs_code",
            "cpt_code",
            "procedure_code",
            "billing_code",
            "code",
            "hcpcs",
            "cpt",
        ],
    )
    .context(
        "Local HCPCS fallback CSV is missing a code column. Expected one of: hcpcs_code, cpt_code, procedure_code, code",
    )?;
    let short_desc_idx = find_header_index(
        &headers,
        &[
            "short_desc",
            "short_description",
            "description_short",
            "desc_short",
            "display",
        ],
    );
    let long_desc_idx = find_header_index(
        &headers,
        &[
            "long_desc",
            "long_description",
            "description_long",
            "description",
            "desc_long",
        ],
    );
    let add_dt_idx = find_header_index(&headers, &["add_dt", "add_date", "effective_from"]);
    let act_eff_dt_idx = find_header_index(
        &headers,
        &[
            "act_eff_dt",
            "act_eff_date",
            "effective_date",
            "effective_dt",
        ],
    );
    let term_dt_idx = find_header_index(&headers, &["term_dt", "term_date", "end_date"]);
    let obsolete_idx = find_header_index(&headers, &["obsolete", "is_obsolete"]);
    let is_noc_idx = find_header_index(&headers, &["is_noc", "noc"]);

    let mut fallback_records: HashMap<String, Vec<HcpcsApiRecord>> = HashMap::new();
    let mut loaded_rows = 0usize;

    for row in reader.records() {
        let row = row.with_context(|| {
            format!(
                "Failed reading row from local HCPCS fallback CSV {}",
                fallback_csv.display()
            )
        })?;
        let Some(code) = normalize_hcpcs_code(row.get(code_idx).unwrap_or_default()) else {
            continue;
        };

        let mut short_desc = field_at(&row, short_desc_idx);
        let mut long_desc = field_at(&row, long_desc_idx);
        if short_desc.is_empty() && long_desc.is_empty() {
            continue;
        }
        if short_desc.is_empty() {
            short_desc = long_desc.clone();
        }
        if long_desc.is_empty() {
            long_desc = short_desc.clone();
        }

        let record = HcpcsApiRecord {
            hcpcs_code: code.clone(),
            short_desc,
            long_desc,
            add_dt: field_at(&row, add_dt_idx),
            act_eff_dt: field_at(&row, act_eff_dt_idx),
            term_dt: field_at(&row, term_dt_idx),
            obsolete: parse_boolish(&field_at(&row, obsolete_idx)),
            is_noc: parse_boolish(&field_at(&row, is_noc_idx)),
        };

        fallback_records.entry(code).or_default().push(record);
        loaded_rows += 1;
    }

    for records in fallback_records.values_mut() {
        let mut dedup = HashSet::new();
        records.retain(|record| dedup.insert(record.clone()));
    }

    if verbose && !fallback_records.is_empty() {
        println!(
            "Loaded local HCPCS/CPT fallback records from {} (rows={}, unique_codes={}).",
            fallback_csv.display(),
            loaded_rows,
            fallback_records.len()
        );
    }
    Ok(fallback_records)
}

fn seed_hcpcs_cache_from_local_fallback(
    cache: &HcpcsCache,
    target_codes: &[String],
    local_fallback: &HashMap<String, Vec<HcpcsApiRecord>>,
) -> Result<usize> {
    if local_fallback.is_empty() {
        return Ok(0);
    }

    let mut seeded = 0usize;
    for code in target_codes {
        let normalized =
            normalize_hcpcs_code(code).unwrap_or_else(|| code.trim().to_ascii_uppercase());
        let Some(records) = local_fallback.get(&normalized) else {
            continue;
        };
        if cache.has_ok_record(code)? {
            continue;
        }
        cache.replace_with_ok_records(code, records)?;
        seeded += 1;
    }
    Ok(seeded)
}

fn local_fallback_dataset_stats(
    cache: &HcpcsCache,
    target_codes: &[String],
    local_fallback: &HashMap<String, Vec<HcpcsApiRecord>>,
) -> Result<(usize, usize)> {
    if local_fallback.is_empty() {
        return Ok((0, 0));
    }

    let mut overlap = 0usize;
    let mut already_ok = 0usize;
    for code in target_codes {
        let normalized =
            normalize_hcpcs_code(code).unwrap_or_else(|| code.trim().to_ascii_uppercase());
        if !local_fallback.contains_key(&normalized) {
            continue;
        }
        overlap += 1;
        if cache.has_ok_record(code)? {
            already_ok += 1;
        }
    }
    Ok((overlap, already_ok))
}

fn fallback_records_for_code(
    local_fallback: &HashMap<String, Vec<HcpcsApiRecord>>,
    code: &str,
) -> Option<Vec<HcpcsApiRecord>> {
    let normalized = normalize_hcpcs_code(code).unwrap_or_else(|| code.trim().to_ascii_uppercase());
    local_fallback.get(&normalized).cloned()
}

fn cached_not_found_code_keys(cache: &HcpcsCache) -> Result<HashSet<String>> {
    let mut stmt = cache
        .conn
        .prepare("SELECT DISTINCT hcpcs_code FROM hcpcs_cache WHERE status = 'not_found'")
        .context("Failed preparing cached HCPCS not_found query")?;
    let rows = stmt
        .query_map([], |row| row.get::<usize, String>(0))
        .context("Failed querying cached HCPCS not_found codes")?;

    let mut keys = HashSet::new();
    for row in rows {
        let code: String = row.context("Failed reading cached HCPCS not_found code")?;
        let key = normalize_code_key(&code);
        if !key.is_empty() {
            keys.insert(key);
        }
    }
    Ok(keys)
}

fn recheck_cached_not_found_against_local_fallback(
    cache: &HcpcsCache,
    target_codes: &[String],
    local_fallback: &HashMap<String, Vec<HcpcsApiRecord>>,
) -> Result<(usize, usize)> {
    if target_codes.is_empty() || local_fallback.is_empty() {
        return Ok((0, 0));
    }

    let not_found_keys = cached_not_found_code_keys(cache)?;
    if not_found_keys.is_empty() {
        return Ok((0, 0));
    }

    let mut checked = 0usize;
    let mut recovered = 0usize;
    for code in target_codes {
        let key = normalize_code_key(code);
        if !not_found_keys.contains(&key) {
            continue;
        }
        // Handle legacy cache rows where a mixed-case key could leave multiple statuses behind.
        if cache.has_ok_record(code)? {
            continue;
        }
        checked += 1;
        if let Some(records) = fallback_records_for_code(local_fallback, code) {
            cache.replace_with_ok_records(code, &records)?;
            recovered += 1;
        }
    }
    Ok((checked, recovered))
}

pub async fn build_hcpcs_mapping(
    args: &Args,
    client: &Client,
    input_path: &Path,
    cache_db: &Path,
    mapping_csv: &Path,
    api_reference_parquet: &Path,
    hcpcs_fallback_csv: &Path,
    api_run_id: &str,
    progress_hub: Option<Arc<MultiProgress>>,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<bool> {
    println!("Extracting unique HCPCS codes...");
    let unique_codes = extract_unique_hcpcs_codes(input_path)?;
    println!(
        "Discovered {} unique HCPCS codes in source data.",
        unique_codes.len()
    );

    let cache = HcpcsCache::open(cache_db)?;
    let local_fallback_records = load_local_hcpcs_fallback_records(hcpcs_fallback_csv, true)?;
    let local_fallback_code_count = local_fallback_records.len();
    let (dataset_codes_in_fallback, fallback_ok_before_seed) =
        local_fallback_dataset_stats(&cache, &unique_codes, &local_fallback_records)?;
    let (not_found_checked, not_found_recovered) = recheck_cached_not_found_against_local_fallback(
        &cache,
        &unique_codes,
        &local_fallback_records,
    )?;
    let seeded_from_local_fallback =
        seed_hcpcs_cache_from_local_fallback(&cache, &unique_codes, &local_fallback_records)?;
    let total_recovered_from_local_fallback = not_found_recovered + seeded_from_local_fallback;
    let fallback_ok_after_seed = fallback_ok_before_seed + total_recovered_from_local_fallback;
    if local_fallback_code_count > 0 {
        println!(
            "Local fallback matched {} dataset HCPCS/CPT codes: already_ok_before={} newly_seeded={} total_ok_after_seed={} (fallback affects HCPCS only; NPI uses NPPES/API). Source={}",
            dataset_codes_in_fallback,
            fallback_ok_before_seed,
            total_recovered_from_local_fallback,
            fallback_ok_after_seed,
            hcpcs_fallback_csv.display()
        );
    }
    if not_found_checked > 0 {
        println!(
            "Rechecked cached HCPCS not_found codes against local fallback: checked={} recovered={}.",
            not_found_checked, not_found_recovered
        );
    }

    let (resolved_count, mut missing_codes) = cache.classify_for_lookup(&unique_codes)?;
    let unresolved_before_limit = missing_codes.len();

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
    let planned_api_lookups = if args.skip_api {
        0
    } else {
        missing_codes.len()
    };
    print_hcpcs_download_plan_table(
        unique_codes.len(),
        resolved_count,
        unresolved_before_limit,
        planned_api_lookups,
        args.hcpcs_batch_size.max(1),
        local_fallback_code_count,
        dataset_codes_in_fallback,
        fallback_ok_before_seed,
        total_recovered_from_local_fallback,
        fallback_ok_after_seed,
    );

    let mut interrupted = shutdown_requested.load(Ordering::SeqCst);
    let mut api_reference_rows: Vec<HcpcsApiReferenceRow> = Vec::new();
    if interrupted {
        println!("Shutdown requested; skipping new HCPCS API lookups.");
    } else if args.skip_api {
        println!("--skip-api set; unresolved HCPCS codes remain unresolved.");
    } else if !missing_codes.is_empty() {
        let (api_interrupted, rows) = resolve_missing_hcpcs(
            &cache,
            missing_codes,
            &local_fallback_records,
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

    cache.export_mapping_csv(mapping_csv)?;
    println!("Wrote HCPCS mapping CSV {}", mapping_csv.display());
    export_hcpcs_api_reference_parquet(&api_reference_rows, api_reference_parquet)?;
    println!(
        "Wrote HCPCS API reference dataset {}",
        api_reference_parquet.display()
    );
    Ok(interrupted || shutdown_requested.load(Ordering::SeqCst))
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

fn print_hcpcs_download_plan_table(
    unique_hcpcs: usize,
    resolved_in_cache: usize,
    unresolved_before_limit: usize,
    planned_api_lookups: usize,
    batch_size: usize,
    local_fallback_loaded: usize,
    dataset_codes_in_fallback: usize,
    fallback_ok_before_seed: usize,
    local_fallback_seeded: usize,
    fallback_ok_after_seed: usize,
) {
    let use_color = std::io::stdout().is_terminal();
    let reset = if use_color { "\x1b[0m" } else { "" };
    let bold = if use_color { "\x1b[1m" } else { "" };
    let cyan = if use_color { "\x1b[36m" } else { "" };
    let green = if use_color { "\x1b[32m" } else { "" };
    let yellow = if use_color { "\x1b[33m" } else { "" };
    let magenta = if use_color { "\x1b[35m" } else { "" };
    let white = if use_color { "\x1b[97m" } else { "" };

    let border = "+--------------------------------------------+--------------------------+";
    let section = "| HCPCS API PRE-DOWNLOAD SUMMARY             |                          |";
    let seeded_percent = if unique_hcpcs == 0 {
        "0.00%".to_string()
    } else {
        format!(
            "{:.2}%",
            (local_fallback_seeded as f64 / unique_hcpcs as f64) * 100.0
        )
    };

    println!();
    println!("{bold}{cyan}{border}{reset}");
    println!("{bold}{cyan}{section}{reset}");
    println!("{bold}{cyan}{border}{reset}");
    println!(
        "| {:<42} | {:<24} |",
        "Unique HCPCS codes in dataset",
        format_count(unique_hcpcs)
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Already saved in cache",
        green,
        format_count(resolved_in_cache),
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
        "Lookup mode",
        format!("batched OR (size={batch_size})")
    );
    println!(
        "| {:<42} | {:<24} |",
        "Local fallback codes loaded",
        format_count(local_fallback_loaded)
    );
    println!(
        "| {:<42} | {:<24} |",
        "Dataset codes in fallback",
        format_count(dataset_codes_in_fallback)
    );
    println!(
        "| {:<42} | {:<24} |",
        "Fallback already ok (before run)",
        format_count(fallback_ok_before_seed)
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Fallback newly seeded (this run)",
        green,
        format!(
            "{} ({})",
            format_count(local_fallback_seeded),
            seeded_percent
        ),
        reset
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Fallback total ok (after seed)",
        green,
        format_count(fallback_ok_after_seed),
        reset
    );
    println!(
        "| {:<42} | {}{:<24}{} |",
        "Fallback applies to", white, "HCPCS/CPT only (not NPI)", reset
    );
    println!("{bold}{cyan}{border}{reset}");
    println!();
}

pub fn is_hcpcs_dataset_complete(
    input_path: &Path,
    cache_db: &Path,
    mapping_csv: &Path,
    api_reference_parquet: &Path,
    hcpcs_fallback_csv: &Path,
) -> Result<bool> {
    if !cache_db.exists() || !mapping_csv.exists() || !api_reference_parquet.exists() {
        return Ok(false);
    }

    let unique_codes = extract_unique_hcpcs_codes(input_path)?;
    let cache = HcpcsCache::open(cache_db)?;
    let (_, missing_codes) = cache.classify_for_lookup(&unique_codes)?;
    if !missing_codes.is_empty() {
        return Ok(false);
    }

    let local_fallback_records = load_local_hcpcs_fallback_records(hcpcs_fallback_csv, false)?;
    if local_fallback_records.is_empty() {
        return Ok(true);
    }

    for code in unique_codes {
        let normalized =
            normalize_hcpcs_code(&code).unwrap_or_else(|| code.trim().to_ascii_uppercase());
        if local_fallback_records.contains_key(&normalized) && !cache.has_ok_record(&code)? {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn collect_unresolved_hcpcs(
    input_path: &Path,
    cache_db: &Path,
) -> Result<Vec<UnresolvedHcpcsEntry>> {
    let unique_codes = extract_unique_hcpcs_codes(input_path)?;
    let cache = HcpcsCache::open(cache_db)?;
    let mut stmt = cache
        .conn
        .prepare(
            "
            SELECT status, error_message, fetched_at_unix
            FROM hcpcs_cache
            WHERE hcpcs_code = ?1 COLLATE NOCASE
            ORDER BY
                CASE status
                    WHEN 'ok' THEN 0
                    WHEN 'not_found' THEN 1
                    WHEN 'error' THEN 2
                    ELSE 3
                END,
                fetched_at_unix DESC
            LIMIT 1
            ",
        )
        .context("Failed preparing unresolved HCPCS lookup statement")?;

    let mut unresolved = Vec::new();
    for code in unique_codes {
        let row: Option<(String, String, i64)> = stmt
            .query_row([&code], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .optional()
            .with_context(|| format!("Failed unresolved HCPCS lookup for {code}"))?;

        match row {
            Some((status, _error_message, _fetched_at_unix)) if status == "ok" => {}
            Some((status, error_message, fetched_at_unix)) => {
                unresolved.push(UnresolvedHcpcsEntry {
                    hcpcs_code: code,
                    status,
                    error_message: normalize_error_message(&error_message),
                    fetched_at_unix: Some(fetched_at_unix),
                });
            }
            None => unresolved.push(UnresolvedHcpcsEntry {
                hcpcs_code: code,
                status: "missing_cache".to_string(),
                error_message: None,
                fetched_at_unix: None,
            }),
        }
    }
    unresolved.sort_by(|a, b| a.hcpcs_code.cmp(&b.hcpcs_code));
    Ok(unresolved)
}

fn normalize_error_message(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn json_to_string_opt(value: Option<&Value>) -> Option<String> {
    value
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::to_string(v).ok())
}

fn build_hcpcs_reference_row_from_value(
    response_value: &Value,
    hcpcs_code: &str,
    request_url: &str,
    http_status: i64,
    api_run_id: &str,
    requested_at_utc: &str,
    request_params_json: &str,
) -> HcpcsApiReferenceRow {
    let array = response_value.as_array();
    let extra_obj = array
        .and_then(|values| values.get(2))
        .and_then(Value::as_object);

    HcpcsApiReferenceRow {
        hcpcs_code: hcpcs_code.to_string(),
        ef_short_desc_json: extra_obj
            .and_then(|obj| obj.get("short_desc"))
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_long_desc_json: extra_obj
            .and_then(|obj| obj.get("long_desc"))
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_add_dt_json: extra_obj
            .and_then(|obj| obj.get("add_dt"))
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_act_eff_dt_json: extra_obj
            .and_then(|obj| obj.get("act_eff_dt"))
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_term_dt_json: extra_obj
            .and_then(|obj| obj.get("term_dt"))
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_obsolete_json: extra_obj
            .and_then(|obj| obj.get("obsolete"))
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_is_noc_json: extra_obj
            .and_then(|obj| obj.get("is_noc"))
            .and_then(|v| serde_json::to_string(v).ok()),
        response_total_count: array
            .and_then(|values| values.first())
            .and_then(Value::as_i64),
        response_codes_json: array.and_then(|values| json_to_string_opt(values.get(1))),
        response_display_json: array.and_then(|values| json_to_string_opt(values.get(3))),
        response_extra_fields_json: array.and_then(|values| json_to_string_opt(values.get(2))),
        request_url: request_url.to_string(),
        http_status: Some(http_status),
        error_message: None,
        api_run_id: api_run_id.to_string(),
        requested_at_utc: requested_at_utc.to_string(),
        request_params_json: request_params_json.to_string(),
        response_json_raw: serde_json::to_string(response_value).ok(),
    }
}

fn export_hcpcs_api_reference_parquet(
    rows: &[HcpcsApiReferenceRow],
    output_path: &Path,
) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "Failed creating HCPCS API reference parent directory {}",
                parent.display()
            )
        })?;
    }

    let file_name = output_path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("hcpcs_api_reference.parquet");
    let tmp_csv_path = output_path.with_file_name(format!("{file_name}.tmp.csv"));
    let tmp_parquet_path = output_path.with_file_name(format!("{file_name}.tmp"));

    let mut writer = Writer::from_path(&tmp_csv_path).with_context(|| {
        format!(
            "Failed creating temp HCPCS API reference CSV {}",
            tmp_csv_path.display()
        )
    })?;
    writer
        .write_record([
            "hcpcs_code",
            "ef_short_desc_json",
            "ef_long_desc_json",
            "ef_add_dt_json",
            "ef_act_eff_dt_json",
            "ef_term_dt_json",
            "ef_obsolete_json",
            "ef_is_noc_json",
            "response_total_count",
            "response_codes_json",
            "response_display_json",
            "response_extra_fields_json",
            "request_url",
            "http_status",
            "error_message",
            "api_run_id",
            "requested_at_utc",
            "request_params_json",
            "response_json_raw",
        ])
        .context("Failed writing HCPCS API reference header")?;

    for row in rows {
        writer
            .write_record([
                row.hcpcs_code.clone(),
                row.ef_short_desc_json.clone().unwrap_or_default(),
                row.ef_long_desc_json.clone().unwrap_or_default(),
                row.ef_add_dt_json.clone().unwrap_or_default(),
                row.ef_act_eff_dt_json.clone().unwrap_or_default(),
                row.ef_term_dt_json.clone().unwrap_or_default(),
                row.ef_obsolete_json.clone().unwrap_or_default(),
                row.ef_is_noc_json.clone().unwrap_or_default(),
                row.response_total_count
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                row.response_codes_json.clone().unwrap_or_default(),
                row.response_display_json.clone().unwrap_or_default(),
                row.response_extra_fields_json.clone().unwrap_or_default(),
                row.request_url.clone(),
                row.http_status.map(|v| v.to_string()).unwrap_or_default(),
                row.error_message.clone().unwrap_or_default(),
                row.api_run_id.clone(),
                row.requested_at_utc.clone(),
                row.request_params_json.clone(),
                row.response_json_raw.clone().unwrap_or_default(),
            ])
            .context("Failed writing HCPCS API reference row")?;
    }
    writer
        .flush()
        .context("Failed flushing HCPCS API reference CSV writer")?;

    let conn = Connection::open_in_memory()
        .context("Failed opening DuckDB for HCPCS API reference export")?;
    let csv_escaped = sql_escape_path(&tmp_csv_path);
    let parquet_escaped = sql_escape_path(&tmp_parquet_path);
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM read_csv_auto('{csv_escaped}', header=true)) TO '{parquet_escaped}' (FORMAT PARQUET);"
    ))
    .context("Failed writing HCPCS API reference parquet")?;

    fs::remove_file(&tmp_csv_path).with_context(|| {
        format!(
            "Failed deleting temp HCPCS API reference CSV {}",
            tmp_csv_path.display()
        )
    })?;
    fs::rename(&tmp_parquet_path, output_path).with_context(|| {
        format!(
            "Failed moving temp HCPCS API reference parquet {} to {}",
            tmp_parquet_path.display(),
            output_path.display()
        )
    })?;
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

fn chunk_hcpcs_codes(codes: Vec<String>, batch_size: usize) -> Vec<Vec<String>> {
    let size = batch_size.max(1);
    if codes.is_empty() {
        return Vec::new();
    }
    codes.chunks(size).map(|chunk| chunk.to_vec()).collect()
}

fn apply_hcpcs_lookup_progress_style(progress: &ProgressBar) {
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.green} {prefix:.bold} [{elapsed_precise}] [{bar:32.magenta/blue}] \
{pos}/{len} ({percent}%) {per_sec} eta {eta_precise} {msg}",
    ) {
        progress.set_style(style.progress_chars("=> "));
    }
}

fn apply_hcpcs_retry_wait_style(progress: &ProgressBar) {
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.yellow} {prefix:.bold} [{elapsed_precise}] [{bar:32.yellow/blue}] \
{pos:>3}/{len}s {msg}",
    ) {
        progress.set_style(style.progress_chars("=> "));
    }
}

async fn run_hcpcs_retry_wait_countdown(
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
    progress.set_prefix("HCPCS RETRY");
    progress.set_length(total_secs);
    progress.set_position(0);
    apply_hcpcs_retry_wait_style(progress);

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

async fn resolve_missing_hcpcs(
    cache: &HcpcsCache,
    missing_codes: Vec<String>,
    local_fallback: &HashMap<String, Vec<HcpcsApiRecord>>,
    client: &Client,
    args: &Args,
    api_run_id: &str,
    progress_hub: Option<Arc<MultiProgress>>,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<(bool, Vec<HcpcsApiReferenceRow>)> {
    if missing_codes.is_empty() {
        return Ok((false, Vec::new()));
    }

    let total = missing_codes.len();
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
    progress.set_prefix("HCPCS");
    apply_hcpcs_lookup_progress_style(&progress);
    progress.enable_steady_tick(Duration::from_millis(250));
    progress.set_message("starting lookups");

    let mut interrupted = shutdown_requested.load(Ordering::SeqCst);
    let mut reference_rows = Vec::new();
    let batch_size = args.hcpcs_batch_size.max(1);
    let mut round_codes = missing_codes;
    let mut retry_round = 0u32;
    let max_retry_rounds = args.failure_retry_rounds;
    let base_retry_delay = Duration::from_secs(args.failure_retry_delay_seconds);

    let mut attempts = 0usize;
    let mut found = 0usize;
    let mut not_found = 0usize;
    let mut failed = 0usize;
    let mut fallback_hits = 0usize;

    while !round_codes.is_empty() {
        if shutdown_requested.load(Ordering::SeqCst) {
            interrupted = true;
            break;
        }

        if retry_round > 0 && !base_retry_delay.is_zero() {
            let retry_delay = base_retry_delay
                .checked_mul(1u32 << retry_round.saturating_sub(1).min(20u32))
                .unwrap_or(Duration::from_secs(3600));
            let pending_retry = round_codes.len();
            let stop_requested = run_hcpcs_retry_wait_countdown(
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
            progress.set_prefix("HCPCS");
            progress.set_length(total as u64);
            progress.set_position((found + not_found + failed) as u64);
            apply_hcpcs_lookup_progress_style(&progress);
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
        let current_round_total = round_codes.len();
        let mut queue = chunk_hcpcs_codes(round_codes, batch_size).into_iter();
        let mut in_flight = FuturesUnordered::new();
        let mut next_round_codes = Vec::new();
        let mut retry_failover_triggered = false;

        for _ in 0..concurrency {
            if shutdown_requested.load(Ordering::SeqCst) {
                interrupted = true;
                break;
            }
            if let Some(batch_codes) = queue.next() {
                in_flight.push(resolve_hcpcs_batch(
                    batch_codes,
                    client.clone(),
                    args.hcpcs_api_base_url.clone(),
                    api_run_id.to_string(),
                    args.max_retries.max(1),
                    Arc::clone(&next_slot),
                    min_interval,
                ));
            }
        }

        let mut pending_current_round = current_round_total;
        while let Some(batch_results) = in_flight.next().await {
            for (code, result) in batch_results {
                attempts += 1;
                pending_current_round = pending_current_round.saturating_sub(1);

                match result {
                    HcpcsResolveResult::Found {
                        mut records,
                        reference_row,
                    } => {
                        reference_rows.push(reference_row);
                        records.retain(|record| record.hcpcs_code.eq_ignore_ascii_case(&code));
                        let mut dedup = HashSet::new();
                        records.retain(|record| dedup.insert(record.clone()));

                        if records.is_empty() {
                            if let Some(fallback_records) =
                                fallback_records_for_code(local_fallback, &code)
                            {
                                cache.replace_with_ok_records(&code, &fallback_records)?;
                                found += 1;
                                fallback_hits += 1;
                            } else {
                                cache.set_not_found(&code, "not_found")?;
                                not_found += 1;
                            }
                        } else {
                            // Keep both non-NOC and NOC records so enrichment can prefer non-NOC
                            // and still fall back to NOC when that's all we have.
                            cache.replace_with_ok_records(&code, &records)?;
                            found += 1;
                        }
                        progress.inc(1);
                    }
                    HcpcsResolveResult::NotFound {
                        reason,
                        reference_row,
                    } => {
                        if let Some(fallback_records) =
                            fallback_records_for_code(local_fallback, &code)
                        {
                            cache.replace_with_ok_records(&code, &fallback_records)?;
                            found += 1;
                            fallback_hits += 1;
                        } else {
                            cache.set_not_found(&code, &reason)?;
                            not_found += 1;
                        }
                        reference_rows.push(reference_row);
                        progress.inc(1);
                    }
                    HcpcsResolveResult::Error {
                        error_message,
                        reference_row,
                    } => {
                        cache.set_error(&code, &error_message)?;
                        reference_rows.push(reference_row);
                        if can_retry_errors_again && !shutdown_requested.load(Ordering::SeqCst) {
                            next_round_codes.push(code);
                            retry_failover_triggered = true;
                        } else {
                            failed += 1;
                            progress.inc(1);
                        }
                    }
                }
            }

            let remaining_in_round = if retry_failover_triggered {
                in_flight.len()
            } else {
                pending_current_round
            };
            let retry_queued = next_round_codes.len()
                + if retry_failover_triggered {
                    queue.as_slice().iter().map(Vec::len).sum::<usize>()
                } else {
                    0
                };
            let mode = if retry_failover_triggered {
                "retry_prep"
            } else {
                "lookup"
            };
            progress.set_message(format!(
                "mode={mode} ok={found} not_found={not_found} failed={failed} fallback={fallback_hits} remaining={remaining_in_round} retry_queued={retry_queued}"
            ));

            if shutdown_requested.load(Ordering::SeqCst) {
                interrupted = true;
            } else if !retry_failover_triggered {
                if let Some(next_batch) = queue.next() {
                    in_flight.push(resolve_hcpcs_batch(
                        next_batch,
                        client.clone(),
                        args.hcpcs_api_base_url.clone(),
                        api_run_id.to_string(),
                        args.max_retries.max(1),
                        Arc::clone(&next_slot),
                        min_interval,
                    ));
                }
            }
        }

        for pending_batch in queue {
            next_round_codes.extend(pending_batch);
        }
        round_codes = next_round_codes;

        if round_codes.is_empty() || interrupted {
            break;
        }
        retry_round = retry_round.saturating_add(1);
    }

    let settled = found + not_found + failed;
    if interrupted {
        progress.abandon_with_message(format!(
            "graceful stop: settled={settled}/{total} ok={found} not_found={not_found} failed={failed} fallback={fallback_hits} pending_retry={} attempts={attempts}",
            round_codes.len()
        ));
    } else {
        progress.finish_with_message(format!(
            "done: settled={settled}/{total} ok={found} not_found={not_found} failed={failed} fallback={fallback_hits} attempts={attempts}"
        ));
    }
    Ok((interrupted, reference_rows))
}

async fn resolve_hcpcs_batch(
    codes: Vec<String>,
    client: Client,
    api_base_url: String,
    api_run_id: String,
    max_retries: u32,
    next_slot: Arc<Mutex<Instant>>,
    min_interval: Duration,
) -> Vec<(String, HcpcsResolveResult)> {
    if codes.is_empty() {
        return Vec::new();
    }
    if codes.len() == 1 {
        let (code, result) = resolve_hcpcs(
            codes[0].clone(),
            client,
            api_base_url,
            api_run_id,
            max_retries,
            next_slot,
            min_interval,
        )
        .await;
        return vec![(code, result)];
    }

    wait_for_rate_slot(&next_slot, min_interval).await;
    match fetch_hcpcs_batch_records(&client, &api_base_url, &codes, &api_run_id, max_retries).await
    {
        Ok(results) => results,
        Err(batch_error) => {
            let mut fallback_results = Vec::with_capacity(codes.len());
            for code in codes {
                wait_for_rate_slot(&next_slot, min_interval).await;
                let single_result =
                    fetch_hcpcs_records(&client, &api_base_url, &code, &api_run_id, max_retries)
                        .await;
                match single_result {
                    HcpcsResolveResult::Error {
                        error_message,
                        mut reference_row,
                    } => {
                        let merged_message = format!(
                            "Batch lookup failed, then single lookup failed. batch_error={batch_error}; single_error={error_message}"
                        );
                        reference_row.error_message = Some(merged_message.clone());
                        fallback_results.push((
                            code,
                            HcpcsResolveResult::Error {
                                error_message: merged_message,
                                reference_row,
                            },
                        ));
                    }
                    other => fallback_results.push((code, other)),
                }
            }
            fallback_results
        }
    }
}

async fn resolve_hcpcs(
    code: String,
    client: Client,
    api_base_url: String,
    api_run_id: String,
    max_retries: u32,
    next_slot: Arc<Mutex<Instant>>,
    min_interval: Duration,
) -> (String, HcpcsResolveResult) {
    wait_for_rate_slot(&next_slot, min_interval).await;
    let result = fetch_hcpcs_records(&client, &api_base_url, &code, &api_run_id, max_retries).await;
    (code, result)
}

async fn fetch_hcpcs_records(
    client: &Client,
    api_base_url: &str,
    hcpcs_code: &str,
    api_run_id: &str,
    max_retries: u32,
) -> HcpcsResolveResult {
    let code_filter = format!("code:{hcpcs_code}");
    let request_params_json = json!({
        "terms": hcpcs_code,
        "sf": "code",
        "q": code_filter,
        "count": 20,
        "df": "code,display",
        "ef": "short_desc,long_desc,add_dt,term_dt,act_eff_dt,obsolete,is_noc"
    })
    .to_string();
    let request_url = reqwest::Url::parse_with_params(
        api_base_url,
        &[
            ("terms", hcpcs_code),
            ("sf", "code"),
            ("q", code_filter.as_str()),
            ("count", "20"),
            ("df", "code,display"),
            (
                "ef",
                "short_desc,long_desc,add_dt,term_dt,act_eff_dt,obsolete,is_noc",
            ),
        ],
    )
    .map(|url| url.to_string())
    .unwrap_or_else(|_| format!("{api_base_url}?terms={hcpcs_code}"));
    let requested_at_utc = now_unix_seconds().to_string();

    let make_base_row = || HcpcsApiReferenceRow {
        hcpcs_code: hcpcs_code.to_string(),
        ef_short_desc_json: None,
        ef_long_desc_json: None,
        ef_add_dt_json: None,
        ef_act_eff_dt_json: None,
        ef_term_dt_json: None,
        ef_obsolete_json: None,
        ef_is_noc_json: None,
        response_total_count: None,
        response_codes_json: None,
        response_display_json: None,
        response_extra_fields_json: None,
        request_url: request_url.clone(),
        http_status: None,
        error_message: None,
        api_run_id: api_run_id.to_string(),
        requested_at_utc: requested_at_utc.clone(),
        request_params_json: request_params_json.clone(),
        response_json_raw: None,
    };

    let attempts = max_retries.max(1);
    let mut backoff = Duration::from_secs(1);

    for attempt in 1..=attempts {
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
                    let body_text = match resp.text().await {
                        Ok(text) => text,
                        Err(err) => {
                            let mut row = make_base_row();
                            row.http_status = Some(status.as_u16() as i64);
                            row.error_message = Some(format!(
                                "Failed reading HCPCS API response body for {hcpcs_code}: {err}"
                            ));
                            return HcpcsResolveResult::Error {
                                error_message: row.error_message.clone().unwrap_or_default(),
                                reference_row: row,
                            };
                        }
                    };
                    let body: Value = match serde_json::from_str(&body_text) {
                        Ok(value) => value,
                        Err(err) => {
                            let mut row = make_base_row();
                            row.http_status = Some(status.as_u16() as i64);
                            row.error_message =
                                Some(format!("Invalid HCPCS API JSON for {hcpcs_code}: {err}"));
                            return HcpcsResolveResult::Error {
                                error_message: row.error_message.clone().unwrap_or_default(),
                                reference_row: row,
                            };
                        }
                    };

                    let row = build_hcpcs_reference_row_from_value(
                        &body,
                        hcpcs_code,
                        &request_url,
                        status.as_u16() as i64,
                        api_run_id,
                        &requested_at_utc,
                        &request_params_json,
                    );

                    return match parse_hcpcs_payload(hcpcs_code, &body) {
                        Ok(records) if records.is_empty() => HcpcsResolveResult::NotFound {
                            reason: "not_found".to_string(),
                            reference_row: row,
                        },
                        Ok(records) => HcpcsResolveResult::Found {
                            records,
                            reference_row: row,
                        },
                        Err(err) => {
                            let mut error_row = row;
                            let message =
                                format!("Failed parsing HCPCS payload for {hcpcs_code}: {err}");
                            error_row.error_message = Some(message.clone());
                            HcpcsResolveResult::Error {
                                error_message: message,
                                reference_row: error_row,
                            }
                        }
                    };
                }

                let retry_after = parse_retry_after(resp.headers().get(RETRY_AFTER));
                let body = resp.text().await.unwrap_or_default();
                if is_retryable_status(status) {
                    if attempt == attempts {
                        let mut row = make_base_row();
                        row.http_status = Some(status.as_u16() as i64);
                        let message = format!(
                            "HCPCS API retryable status {} for {} after {} attempts. Body: {}",
                            status,
                            hcpcs_code,
                            attempts,
                            truncate_for_log(&body)
                        );
                        row.error_message = Some(message.clone());
                        return HcpcsResolveResult::Error {
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
                    "HCPCS API non-retryable status {} for {}. Body: {}",
                    status,
                    hcpcs_code,
                    truncate_for_log(&body)
                );
                row.error_message = Some(message.clone());
                return HcpcsResolveResult::Error {
                    error_message: message,
                    reference_row: row,
                };
            }
            Err(err) => {
                if attempt == attempts {
                    let mut row = make_base_row();
                    let message = format!("HCPCS API request failed for {hcpcs_code}: {err}");
                    row.error_message = Some(message.clone());
                    return HcpcsResolveResult::Error {
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
    let message = format!("Unexpected HCPCS API flow for {hcpcs_code}");
    row.error_message = Some(message.clone());
    HcpcsResolveResult::Error {
        error_message: message,
        reference_row: row,
    }
}

fn normalize_code_key(code: &str) -> String {
    code.trim().to_ascii_uppercase()
}

async fn fetch_hcpcs_batch_records(
    client: &Client,
    api_base_url: &str,
    hcpcs_codes: &[String],
    api_run_id: &str,
    max_retries: u32,
) -> std::result::Result<Vec<(String, HcpcsResolveResult)>, String> {
    if hcpcs_codes.is_empty() {
        return Ok(Vec::new());
    }

    let cleaned_codes: Vec<String> = hcpcs_codes
        .iter()
        .map(|code| normalize_code_key(code))
        .filter(|code| !code.is_empty())
        .collect();
    if cleaned_codes.is_empty() {
        return Ok(Vec::new());
    }

    let code_filter = format!("code:({})", cleaned_codes.join(" OR "));
    let request_params_json = json!({
        "terms": "",
        "sf": "code",
        "q": code_filter,
        "count": 500,
        "df": "code,display",
        "ef": "short_desc,long_desc,add_dt,term_dt,act_eff_dt,obsolete,is_noc"
    })
    .to_string();
    let request_url = reqwest::Url::parse_with_params(
        api_base_url,
        &[
            ("terms", ""),
            ("sf", "code"),
            ("q", code_filter.as_str()),
            ("count", "500"),
            ("df", "code,display"),
            (
                "ef",
                "short_desc,long_desc,add_dt,term_dt,act_eff_dt,obsolete,is_noc",
            ),
        ],
    )
    .map(|url| url.to_string())
    .unwrap_or_else(|_| format!("{api_base_url}?q={code_filter}"));
    let requested_at_utc = now_unix_seconds().to_string();

    let attempts = max_retries.max(1);
    let mut backoff = Duration::from_secs(1);

    for attempt in 1..=attempts {
        let response = client
            .get(api_base_url)
            .query(&[
                ("terms", ""),
                ("sf", "code"),
                ("q", code_filter.as_str()),
                ("count", "500"),
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
                    let body_text = resp.text().await.map_err(|err| {
                        format!("Failed reading HCPCS batch response body: {err}")
                    })?;
                    let body: Value = serde_json::from_str(&body_text)
                        .map_err(|err| format!("Invalid HCPCS batch JSON: {err}"))?;
                    let records_by_code = parse_hcpcs_payload_by_code(&body).map_err(|err| {
                        format!("Failed parsing HCPCS batch payload for requested codes: {err}")
                    })?;

                    let mut outcomes = Vec::with_capacity(hcpcs_codes.len());
                    for code in hcpcs_codes {
                        let lookup_key = normalize_code_key(code);
                        let records = records_by_code
                            .get(&lookup_key)
                            .cloned()
                            .unwrap_or_default();
                        let reference_row = build_hcpcs_reference_row_for_code(
                            &body,
                            code,
                            &request_url,
                            status.as_u16() as i64,
                            api_run_id,
                            &requested_at_utc,
                            &request_params_json,
                        );

                        if records.is_empty() {
                            outcomes.push((
                                code.clone(),
                                HcpcsResolveResult::NotFound {
                                    reason: "not_found".to_string(),
                                    reference_row,
                                },
                            ));
                        } else {
                            outcomes.push((
                                code.clone(),
                                HcpcsResolveResult::Found {
                                    records,
                                    reference_row,
                                },
                            ));
                        }
                    }
                    return Ok(outcomes);
                }

                let retry_after = parse_retry_after(resp.headers().get(RETRY_AFTER));
                let body = resp.text().await.unwrap_or_default();
                if is_retryable_status(status) {
                    if attempt == attempts {
                        return Err(format!(
                            "HCPCS batch retryable status {} after {} attempts. Body: {}",
                            status,
                            attempts,
                            truncate_for_log(&body)
                        ));
                    }
                    tokio::time::sleep(retry_after.unwrap_or(backoff)).await;
                    backoff = (backoff + backoff).min(Duration::from_secs(60));
                    continue;
                }

                return Err(format!(
                    "HCPCS batch non-retryable status {}. Body: {}",
                    status,
                    truncate_for_log(&body)
                ));
            }
            Err(err) => {
                if attempt == attempts {
                    return Err(format!("HCPCS batch request failed: {err}"));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff + backoff).min(Duration::from_secs(60));
            }
        }
    }

    Err("Unexpected HCPCS batch API flow".to_string())
}

fn parse_hcpcs_payload_by_code(payload: &Value) -> Result<HashMap<String, Vec<HcpcsApiRecord>>> {
    let arr = payload
        .as_array()
        .context("HCPCS API payload is not an array")?;
    let code_values = arr
        .get(1)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let extra = arr.get(2).unwrap_or(&Value::Null);

    let mut by_code: HashMap<String, Vec<HcpcsApiRecord>> = HashMap::new();
    for idx in 0..code_values.len() {
        let code = value_to_string(&code_values[idx]).trim().to_string();
        if code.is_empty() {
            continue;
        }

        let short_desc = field_value(extra, "short_desc", idx).trim().to_string();
        let long_desc = field_value(extra, "long_desc", idx).trim().to_string();
        let add_dt = normalize_yyyymmdd(&field_value(extra, "add_dt", idx));
        let act_eff_dt = normalize_yyyymmdd(&field_value(extra, "act_eff_dt", idx));
        let term_dt = normalize_yyyymmdd(&field_value(extra, "term_dt", idx));
        let obsolete = parse_bool_flag(&field_value(extra, "obsolete", idx)).unwrap_or(false);
        let is_noc = parse_bool_flag(&field_value(extra, "is_noc", idx)).unwrap_or(false);

        by_code
            .entry(normalize_code_key(&code))
            .or_default()
            .push(HcpcsApiRecord {
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
    Ok(by_code)
}

fn build_hcpcs_reference_row_for_code(
    response_value: &Value,
    hcpcs_code: &str,
    request_url: &str,
    http_status: i64,
    api_run_id: &str,
    requested_at_utc: &str,
    request_params_json: &str,
) -> HcpcsApiReferenceRow {
    let array = response_value.as_array();
    let code_values = array
        .and_then(|values| values.get(1))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let matching_indices: Vec<usize> = code_values
        .iter()
        .enumerate()
        .filter_map(|(idx, value)| {
            let code = value_to_string(value);
            if !code.trim().is_empty() && code.eq_ignore_ascii_case(hcpcs_code) {
                Some(idx)
            } else {
                None
            }
        })
        .collect();
    let filtered_codes: Vec<Value> = matching_indices
        .iter()
        .filter_map(|idx| code_values.get(*idx).cloned())
        .collect();
    let filtered_display: Vec<Value> = array
        .and_then(|values| values.get(3))
        .and_then(Value::as_array)
        .map(|items| {
            matching_indices
                .iter()
                .filter_map(|idx| items.get(*idx).cloned())
                .collect()
        })
        .unwrap_or_default();

    let filtered_extra_obj = array
        .and_then(|values| values.get(2))
        .and_then(Value::as_object)
        .map(|obj| {
            let mut filtered = serde_json::Map::new();
            for (key, value) in obj {
                let filtered_value = match value {
                    Value::Array(items) => Value::Array(
                        matching_indices
                            .iter()
                            .filter_map(|idx| items.get(*idx).cloned())
                            .collect(),
                    ),
                    _ => value.clone(),
                };
                filtered.insert(key.clone(), filtered_value);
            }
            filtered
        })
        .unwrap_or_default();
    let filtered_extra_value = Value::Object(filtered_extra_obj.clone());

    HcpcsApiReferenceRow {
        hcpcs_code: hcpcs_code.to_string(),
        ef_short_desc_json: filtered_extra_obj
            .get("short_desc")
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_long_desc_json: filtered_extra_obj
            .get("long_desc")
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_add_dt_json: filtered_extra_obj
            .get("add_dt")
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_act_eff_dt_json: filtered_extra_obj
            .get("act_eff_dt")
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_term_dt_json: filtered_extra_obj
            .get("term_dt")
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_obsolete_json: filtered_extra_obj
            .get("obsolete")
            .and_then(|v| serde_json::to_string(v).ok()),
        ef_is_noc_json: filtered_extra_obj
            .get("is_noc")
            .and_then(|v| serde_json::to_string(v).ok()),
        response_total_count: array
            .and_then(|values| values.first())
            .and_then(Value::as_i64),
        response_codes_json: serde_json::to_string(&filtered_codes).ok(),
        response_display_json: serde_json::to_string(&filtered_display).ok(),
        response_extra_fields_json: serde_json::to_string(&filtered_extra_value).ok(),
        request_url: request_url.to_string(),
        http_status: Some(http_status),
        error_message: None,
        api_run_id: api_run_id.to_string(),
        requested_at_utc: requested_at_utc.to_string(),
        request_params_json: request_params_json.to_string(),
        response_json_raw: serde_json::to_string(response_value).ok(),
    }
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
