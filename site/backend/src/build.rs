use std::path::Path;

use anyhow::{Context, anyhow};
use duckdb::{Connection, params};
use serde::Serialize;

use crate::cli::BuildArgs;
use crate::download;
use crate::geo;
use crate::hcpcs;
use crate::index;
use crate::npi;
use crate::storage::StoragePaths;

#[derive(Debug, Serialize)]
struct BuildMeta {
    built_at_utc: String,
    hf_repo: String,
    hf_revision: String,
    duckdb_path: String,
    provider_index_dir: String,
    hcpcs_index_dir: String,
    provider_count: u64,
    hcpcs_count: u64,
}

pub async fn run(opts: BuildArgs) -> anyhow::Result<()> {
    tracing::info!("site-backend build");
    tracing::info!("data_dir={}", opts.data_dir);
    tracing::info!("hf_repo={} hf_revision={}", opts.hf_repo, opts.hf_revision);
    if opts.offline {
        tracing::info!("offline=true (will not download missing inputs)");
    }
    if opts.force_download {
        tracing::info!("force_download=true (will re-download inputs)");
    }
    if opts.rebuild {
        tracing::info!("rebuild=true (will rebuild tables and indices)");
    }

    let paths = StoragePaths::new(&opts.data_dir);
    paths
        .ensure_dirs()
        .context("create backend data directories")?;

    tracing::info!("Step 1/6: ensure inputs (parquets + zip centroids)");
    let t0 = std::time::Instant::now();
    let (sources, geonames_txt) = download::ensure_inputs(&paths, &opts).await?;
    tracing::info!(
        "Inputs ready in {:.1}s: spending={} npi={} hcpcs={} zip_centroids={}",
        t0.elapsed().as_secs_f64(),
        sources.spending.display(),
        sources.npi.display(),
        sources.hcpcs.display(),
        geonames_txt.display()
    );

    tracing::info!("Step 2/6: open DuckDB + create parquet views");
    let t1 = std::time::Instant::now();
    let mut conn = Connection::open(&paths.duckdb_path)
        .with_context(|| format!("open duckdb at {}", paths.duckdb_path.display()))?;

    // Basic performance tuning; keep conservative defaults.
    let _ = conn.execute("PRAGMA threads=4", []);

    create_or_replace_views(&mut conn, &sources.spending, &sources.npi, &sources.hcpcs)
        .context("create views")?;
    tracing::info!(
        "DuckDB ready in {:.1}s: {}",
        t1.elapsed().as_secs_f64(),
        paths.duckdb_path.display()
    );

    tracing::info!("Step 3/6: build rollups (provider_totals + hcpcs_totals)");
    if opts.rebuild || !table_exists(&mut conn, "provider_totals")? {
        rebuild_provider_totals(&mut conn).context("build provider_totals")?;
    } else {
        tracing::info!("DuckDB table provider_totals already exists; skipping");
    }

    if opts.rebuild || !table_exists(&mut conn, "hcpcs_totals")? {
        rebuild_hcpcs_totals(&mut conn).context("build hcpcs_totals")?;
    } else {
        tracing::info!("DuckDB table hcpcs_totals already exists; skipping");
    }

    tracing::info!(
        "Step 4/6: build geo + metadata tables (zip_centroids + provider_info + hcpcs_info)"
    );
    if opts.rebuild || !table_exists(&mut conn, "zip_centroids")? {
        rebuild_zip_centroids(&mut conn, &geonames_txt).context("build zip_centroids")?;
    } else {
        tracing::info!("DuckDB table zip_centroids already exists; skipping");
    }

    if opts.rebuild || !table_exists(&mut conn, "provider_info")? {
        rebuild_provider_info(&mut conn).context("build provider_info")?;
    } else {
        tracing::info!("DuckDB table provider_info already exists; skipping");
    }

    if opts.rebuild || !table_exists(&mut conn, "hcpcs_info")? {
        rebuild_hcpcs_info(&mut conn).context("build hcpcs_info")?;
    } else {
        tracing::info!("DuckDB table hcpcs_info already exists; skipping");
    }

    tracing::info!("Step 5/6: build serving tables (provider_search + hcpcs_search)");
    let provider_search_exists = table_exists(&mut conn, "provider_search")?;
    let provider_search_bad = if provider_search_exists && !opts.rebuild {
        let bad = count_bad_keys(&mut conn, "provider_search", "npi")?;
        if bad > 0 {
            tracing::info!(
                "DuckDB table provider_search has {} rows with NULL/empty npi; rebuilding",
                bad
            );
            true
        } else {
            false
        }
    } else {
        false
    };

    if opts.rebuild || !provider_search_exists || provider_search_bad {
        rebuild_provider_search(&mut conn).context("build provider_search")?;
    } else {
        tracing::info!("DuckDB table provider_search already exists; skipping");
    }

    let hcpcs_search_exists = table_exists(&mut conn, "hcpcs_search")?;
    let hcpcs_search_bad = if hcpcs_search_exists && !opts.rebuild {
        let bad = count_bad_keys(&mut conn, "hcpcs_search", "hcpcs_code")?;
        if bad > 0 {
            tracing::info!(
                "DuckDB table hcpcs_search has {} rows with NULL/empty hcpcs_code; rebuilding",
                bad
            );
            true
        } else {
            false
        }
    } else {
        false
    };

    if opts.rebuild || !hcpcs_search_exists || hcpcs_search_bad {
        rebuild_hcpcs_search(&mut conn).context("build hcpcs_search")?;
    } else {
        tracing::info!("DuckDB table hcpcs_search already exists; skipping");
    }

    tracing::info!("Step 6/6: build search indices (Tantivy)");
    index::providers::build_provider_index(&conn, &paths.provider_index_dir, opts.rebuild)
        .context("build provider tantivy index")?;
    index::hcpcs::build_hcpcs_index(&conn, &paths.hcpcs_index_dir, opts.rebuild)
        .context("build hcpcs tantivy index")?;

    let provider_count: u64 = one_u64(&mut conn, "SELECT COUNT(*) FROM provider_search")?;
    let hcpcs_count: u64 = one_u64(&mut conn, "SELECT COUNT(*) FROM hcpcs_search")?;

    let meta = BuildMeta {
        built_at_utc: now_utc_rfc3339(),
        hf_repo: opts.hf_repo.clone(),
        hf_revision: opts.hf_revision.clone(),
        duckdb_path: paths.duckdb_path.display().to_string(),
        provider_index_dir: paths.provider_index_dir.display().to_string(),
        hcpcs_index_dir: paths.hcpcs_index_dir.display().to_string(),
        provider_count,
        hcpcs_count,
    };
    write_json(&paths.meta_path, &meta).context("write meta.json")?;

    tracing::info!("Build complete.");
    tracing::info!("DuckDB: {}", paths.duckdb_path.display());
    tracing::info!("Provider index: {}", paths.provider_index_dir.display());
    tracing::info!("HCPCS index: {}", paths.hcpcs_index_dir.display());

    Ok(())
}

fn create_or_replace_views(
    conn: &mut Connection,
    spending: &Path,
    npi: &Path,
    hcpcs: &Path,
) -> anyhow::Result<()> {
    let spending = sql_quote_path(spending);
    let npi = sql_quote_path(npi);
    let hcpcs = sql_quote_path(hcpcs);

    conn.execute(
        &format!("CREATE OR REPLACE VIEW spending_raw AS SELECT * FROM read_parquet('{spending}')"),
        [],
    )?;
    conn.execute(
        &format!("CREATE OR REPLACE VIEW npi_api_raw AS SELECT * FROM read_parquet('{npi}')"),
        [],
    )?;
    conn.execute(
        &format!("CREATE OR REPLACE VIEW hcpcs_api_raw AS SELECT * FROM read_parquet('{hcpcs}')"),
        [],
    )?;
    Ok(())
}

fn rebuild_provider_totals(conn: &mut Connection) -> anyhow::Result<()> {
    tracing::info!(
        "Building provider_totals (this will scan the spending parquet; can take a while)..."
    );
    conn.execute("DROP TABLE IF EXISTS provider_totals", [])?;

    let sql = r#"
        CREATE TABLE provider_totals AS
        WITH billing AS (
          SELECT
            NULLIF(TRIM(BILLING_PROVIDER_NPI_NUM), '') AS npi,
            SUM(TOTAL_PAID) AS paid_billing,
            SUM(TOTAL_CLAIMS) AS claims_billing,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS bene_billing
          FROM spending_raw
          WHERE BILLING_PROVIDER_NPI_NUM IS NOT NULL AND TRIM(BILLING_PROVIDER_NPI_NUM) <> ''
          GROUP BY 1
        ),
        servicing AS (
          SELECT
            NULLIF(TRIM(SERVICING_PROVIDER_NPI_NUM), '') AS npi,
            SUM(TOTAL_PAID) AS paid_servicing,
            SUM(TOTAL_CLAIMS) AS claims_servicing,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS bene_servicing
          FROM spending_raw
          WHERE SERVICING_PROVIDER_NPI_NUM IS NOT NULL AND TRIM(SERVICING_PROVIDER_NPI_NUM) <> ''
          GROUP BY 1
        )
        SELECT
          COALESCE(billing.npi, servicing.npi) AS npi,
          COALESCE(paid_billing, 0) AS paid_billing,
          COALESCE(claims_billing, 0) AS claims_billing,
          COALESCE(bene_billing, 0) AS bene_billing,
          COALESCE(paid_servicing, 0) AS paid_servicing,
          COALESCE(claims_servicing, 0) AS claims_servicing,
          COALESCE(bene_servicing, 0) AS bene_servicing,
          COALESCE(paid_billing, 0) + COALESCE(paid_servicing, 0) AS paid_total,
          COALESCE(claims_billing, 0) + COALESCE(claims_servicing, 0) AS claims_total,
          COALESCE(bene_billing, 0) + COALESCE(bene_servicing, 0) AS bene_total
        FROM billing
        FULL OUTER JOIN servicing ON billing.npi = servicing.npi
        WHERE COALESCE(billing.npi, servicing.npi) IS NOT NULL
    "#;
    conn.execute(sql, [])?;
    Ok(())
}

fn rebuild_hcpcs_totals(conn: &mut Connection) -> anyhow::Result<()> {
    tracing::info!(
        "Building hcpcs_totals (this will scan the spending parquet; can take a while)..."
    );
    conn.execute("DROP TABLE IF EXISTS hcpcs_totals", [])?;
    let sql = r#"
        CREATE TABLE hcpcs_totals AS
        SELECT
          NULLIF(TRIM(HCPCS_CODE), '') AS hcpcs_code,
          SUM(TOTAL_PAID) AS paid_total,
          SUM(TOTAL_CLAIMS) AS claims_total,
          SUM(TOTAL_UNIQUE_BENEFICIARIES) AS bene_total
        FROM spending_raw
        WHERE HCPCS_CODE IS NOT NULL AND TRIM(HCPCS_CODE) <> ''
        GROUP BY 1
    "#;
    conn.execute(sql, [])?;
    Ok(())
}

fn rebuild_zip_centroids(conn: &mut Connection, geonames_txt: &Path) -> anyhow::Result<()> {
    tracing::info!("Building zip_centroids from {}...", geonames_txt.display());
    conn.execute("DROP TABLE IF EXISTS zip_centroids", [])?;
    conn.execute(
        "CREATE TABLE zip_centroids (zip5 TEXT PRIMARY KEY, lat DOUBLE, lon DOUBLE)",
        [],
    )?;

    let centroids = geo::parse_geonames_us_txt(geonames_txt)?;

    let tx = conn.transaction().context("begin tx")?;
    {
        let mut stmt = tx
            .prepare("INSERT OR REPLACE INTO zip_centroids (zip5, lat, lon) VALUES (?, ?, ?)")
            .context("prepare insert zip_centroids")?;
        for c in centroids {
            stmt.execute(params![c.zip5, c.lat, c.lon])?;
        }
    }
    tx.commit().context("commit zip_centroids")?;
    Ok(())
}

fn rebuild_provider_info(conn: &mut Connection) -> anyhow::Result<()> {
    tracing::info!("Building provider_info from npi_api_raw...");
    conn.execute("DROP TABLE IF EXISTS provider_info", [])?;
    conn.execute(
        r#"
        CREATE TABLE provider_info (
          npi TEXT PRIMARY KEY,
          display_name TEXT,
          enumeration_type TEXT,
          primary_taxonomy_code TEXT,
          primary_taxonomy_desc TEXT,
          state TEXT,
          city TEXT,
          zip5 TEXT
        )
    "#,
        [],
    )?;

    let json_col = detect_json_col(conn, "npi_api_raw")?;
    let sql = format!("SELECT npi, {json_col} FROM npi_api_raw");
    let tx = conn.transaction().context("begin tx")?;
    {
        let mut sel = tx.prepare(&sql).context("prepare npi_api_raw scan")?;
        let mut rows = sel.query([]).context("query npi_api_raw")?;
        let mut ins = tx.prepare(
            r#"
            INSERT OR REPLACE INTO provider_info
              (npi, display_name, enumeration_type, primary_taxonomy_code, primary_taxonomy_desc, state, city, zip5)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )?;

        while let Some(row) = rows.next()? {
            let npi_id: String = row.get(0)?;
            let response: Option<String> = row.get(1)?;
            let ex = npi::extract_provider_fields(&npi_id, response.as_deref());
            ins.execute(params![
                ex.npi,
                ex.display_name,
                ex.enumeration_type,
                ex.primary_taxonomy_code,
                ex.primary_taxonomy_desc,
                ex.state,
                ex.city,
                ex.zip5
            ])?;
        }
    }
    tx.commit().context("commit provider_info")?;

    Ok(())
}

fn rebuild_hcpcs_info(conn: &mut Connection) -> anyhow::Result<()> {
    tracing::info!("Building hcpcs_info from hcpcs_api_raw...");
    conn.execute("DROP TABLE IF EXISTS hcpcs_info", [])?;
    conn.execute(
        r#"
        CREATE TABLE hcpcs_info (
          hcpcs_code TEXT PRIMARY KEY,
          short_desc TEXT,
          long_desc TEXT,
          add_dt TEXT,
          act_eff_dt TEXT,
          term_dt TEXT,
          obsolete TEXT,
          is_noc TEXT
        )
    "#,
        [],
    )?;

    let json_col = detect_json_col(conn, "hcpcs_api_raw")?;
    let sql = format!("SELECT hcpcs_code, {json_col} FROM hcpcs_api_raw");
    let tx = conn.transaction().context("begin tx")?;
    {
        let mut sel = tx.prepare(&sql).context("prepare hcpcs_api_raw scan")?;
        let mut rows = sel.query([]).context("query hcpcs_api_raw")?;
        let mut ins = tx.prepare(
            r#"
            INSERT OR REPLACE INTO hcpcs_info
              (hcpcs_code, short_desc, long_desc, add_dt, act_eff_dt, term_dt, obsolete, is_noc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )?;
        while let Some(row) = rows.next()? {
            let code: String = row.get(0)?;
            let response: Option<String> = row.get(1)?;
            let ex = hcpcs::extract_hcpcs_fields(&code, response.as_deref());
            ins.execute(params![
                ex.hcpcs_code,
                ex.short_desc,
                ex.long_desc,
                ex.add_dt,
                ex.act_eff_dt,
                ex.term_dt,
                ex.obsolete,
                ex.is_noc
            ])?;
        }
    }
    tx.commit().context("commit hcpcs_info")?;

    Ok(())
}

fn rebuild_provider_search(conn: &mut Connection) -> anyhow::Result<()> {
    tracing::info!(
        "Building provider_search (joining provider_totals + provider_info + zip_centroids)..."
    );
    conn.execute("DROP TABLE IF EXISTS provider_search", [])?;

    let sql = r#"
        CREATE TABLE provider_search AS
        WITH joined AS (
          SELECT
            COALESCE(pi.npi, pt.npi) AS npi,
            pi.display_name,
            pi.enumeration_type,
            pi.primary_taxonomy_code,
            pi.primary_taxonomy_desc,
            pi.state,
            pi.city,
            pi.zip5,
            COALESCE(pt.paid_billing, 0) AS paid_billing,
            COALESCE(pt.claims_billing, 0) AS claims_billing,
            COALESCE(pt.bene_billing, 0) AS bene_billing,
            COALESCE(pt.paid_servicing, 0) AS paid_servicing,
            COALESCE(pt.claims_servicing, 0) AS claims_servicing,
            COALESCE(pt.bene_servicing, 0) AS bene_servicing,
            COALESCE(pt.paid_total, 0) AS paid_total,
            COALESCE(pt.claims_total, 0) AS claims_total,
            COALESCE(pt.bene_total, 0) AS bene_total
          FROM provider_totals pt
          FULL OUTER JOIN provider_info pi ON pi.npi = pt.npi
        )
        SELECT
          joined.*,
          z.lat,
          z.lon
        FROM joined
        LEFT JOIN zip_centroids z ON z.zip5 = joined.zip5
        WHERE joined.npi IS NOT NULL AND TRIM(joined.npi) <> ''
    "#;
    conn.execute(sql, [])?;
    Ok(())
}

fn rebuild_hcpcs_search(conn: &mut Connection) -> anyhow::Result<()> {
    tracing::info!("Building hcpcs_search (joining hcpcs_totals + hcpcs_info)...");
    conn.execute("DROP TABLE IF EXISTS hcpcs_search", [])?;

    let sql = r#"
        CREATE TABLE hcpcs_search AS
        WITH joined AS (
          SELECT
            COALESCE(hi.hcpcs_code, ht.hcpcs_code) AS hcpcs_code,
            hi.short_desc,
            hi.long_desc,
            hi.add_dt,
            hi.act_eff_dt,
            hi.term_dt,
            hi.obsolete,
            hi.is_noc,
            COALESCE(ht.paid_total, 0) AS paid_total,
            COALESCE(ht.claims_total, 0) AS claims_total,
            COALESCE(ht.bene_total, 0) AS bene_total
          FROM hcpcs_totals ht
          FULL OUTER JOIN hcpcs_info hi ON hi.hcpcs_code = ht.hcpcs_code
        )
        SELECT * FROM joined
        WHERE hcpcs_code IS NOT NULL AND TRIM(hcpcs_code) <> ''
    "#;
    conn.execute(sql, [])?;
    Ok(())
}

fn table_exists(conn: &mut Connection, name: &str) -> anyhow::Result<bool> {
    let mut stmt = conn.prepare(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
    "#,
    )?;
    let count: i64 = stmt.query_row(params![name], |row| row.get(0))?;
    Ok(count > 0)
}

fn count_bad_keys(conn: &mut Connection, table: &str, col: &str) -> anyhow::Result<i64> {
    // table/col are compile-time constants in this crate; keep this helper private.
    let sql =
        format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE {col} IS NULL OR TRIM({col}) = ''");
    let mut stmt = conn.prepare(&sql)?;
    let v: i64 = stmt.query_row([], |row| row.get(0))?;
    Ok(v)
}

fn detect_json_col(conn: &mut Connection, view: &str) -> anyhow::Result<String> {
    for candidate in ["response_json", "response"] {
        let sql = format!("SELECT {candidate} FROM {view} LIMIT 1");
        if conn.prepare(&sql).is_ok() {
            return Ok(candidate.to_string());
        }
    }
    Err(anyhow!(
        "Could not find response JSON column in {view}; expected response_json or response"
    ))
}

fn one_u64(conn: &mut Connection, sql: &str) -> anyhow::Result<u64> {
    let mut stmt = conn.prepare(sql)?;
    let v: i64 = stmt.query_row([], |row| row.get(0))?;
    Ok(v.max(0) as u64)
}

fn write_json(path: &Path, v: &impl Serialize) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let s = serde_json::to_string_pretty(v)?;
    std::fs::write(path, s)?;
    Ok(())
}

fn now_utc_rfc3339() -> String {
    // Avoid extra chrono/time dependency; use a simple ISO-like timestamp.
    let now = std::time::SystemTime::now();
    let dur = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}s_since_epoch", dur.as_secs())
}

fn sql_quote_path(path: &Path) -> String {
    // DuckDB expects single-quoted string literals; escape embedded single quotes.
    path.display().to_string().replace('\'', "''")
}
