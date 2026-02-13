use anyhow::{Context, Result, bail};
use duckdb::Connection;
use std::{fs, path::Path};

use crate::common::{source_expr, sql_escape_path};

pub fn enrich_dataset(
    input_path: &Path,
    output_path: &Path,
    npi_mapping_csv: &Path,
    hcpcs_mapping_csv: &Path,
) -> Result<()> {
    if !npi_mapping_csv.exists() {
        bail!("NPI mapping CSV not found at {}", npi_mapping_csv.display());
    }
    if !hcpcs_mapping_csv.exists() {
        bail!(
            "HCPCS mapping CSV not found at {}",
            hcpcs_mapping_csv.display()
        );
    }
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed creating output dir {}", parent.display()))?;
    }

    let conn = Connection::open_in_memory().context("Failed opening DuckDB for enrichment")?;
    let source = source_expr(input_path)?;
    let npi_mapping_path_sql = sql_escape_path(npi_mapping_csv);
    let hcpcs_mapping_path_sql = sql_escape_path(hcpcs_mapping_csv);
    let output_path_sql = sql_escape_path(output_path);

    let select_sql = format!(
        "
        WITH src_raw AS (
            SELECT * FROM {source}
        ),
        src AS (
            SELECT
                ROW_NUMBER() OVER () AS _row_id,
                src_raw.*,
                COALESCE(
                    TRY_CAST(src_raw.CLAIM_FROM_MONTH AS DATE),
                    TRY_STRPTIME(CAST(src_raw.CLAIM_FROM_MONTH AS VARCHAR), '%Y-%m')::DATE
                ) AS _claim_from_date
            FROM src_raw
        ),
        npi_map AS (
            SELECT CAST(npi AS VARCHAR) AS npi, NULLIF(provider_name, '') AS provider_name
            FROM read_csv_auto('{npi_mapping_path_sql}', header=true)
        ),
        hcpcs_map AS (
            SELECT
                CAST(hcpcs_code AS VARCHAR) AS hcpcs_code,
                NULLIF(short_desc, '') AS short_desc,
                NULLIF(long_desc, '') AS long_desc,
                CASE
                    WHEN NULLIF(TRIM(add_dt), '') IS NULL THEN NULL
                    ELSE STRPTIME(TRIM(add_dt), '%Y%m%d')::DATE
                END AS add_date,
                CASE
                    WHEN NULLIF(TRIM(act_eff_dt), '') IS NULL THEN NULL
                    ELSE STRPTIME(TRIM(act_eff_dt), '%Y%m%d')::DATE
                END AS act_eff_date,
                CASE
                    WHEN NULLIF(TRIM(term_dt), '') IS NULL THEN NULL
                    ELSE STRPTIME(TRIM(term_dt), '%Y%m%d')::DATE
                END AS term_date,
                LOWER(COALESCE(NULLIF(TRIM(obsolete), ''), 'false')) AS obsolete,
                LOWER(COALESCE(NULLIF(TRIM(is_noc), ''), 'false')) AS is_noc,
                LOWER(COALESCE(NULLIF(TRIM(status), ''), 'ok')) AS status
            FROM read_csv_auto('{hcpcs_mapping_path_sql}', header=true)
        ),
        joined AS (
            SELECT
                src.*,
                billing.provider_name AS BILLING_PROVIDER,
                servicing.provider_name AS SERVICING_PROVIDER,
                hcpcs.short_desc AS HCPCS_SHORT_DESC,
                hcpcs.long_desc AS HCPCS_LONG_DESC,
                CASE
                    WHEN hcpcs.add_date IS NULL THEN NULL
                    ELSE STRFTIME(hcpcs.add_date, '%Y-%m-%d')
                END AS HCPCS_ADD_DATE,
                CASE
                    WHEN hcpcs.act_eff_date IS NULL THEN NULL
                    ELSE STRFTIME(hcpcs.act_eff_date, '%Y-%m-%d')
                END AS HCPCS_ACT_EFF_DATE,
                CASE
                    WHEN hcpcs.term_date IS NULL THEN NULL
                    ELSE STRFTIME(hcpcs.term_date, '%Y-%m-%d')
                END AS HCPCS_TERM_DATE,
                hcpcs.obsolete = 'true' AS HCPCS_OBSOLETE,
                hcpcs.is_noc = 'true' AS HCPCS_IS_NOC,
                ROW_NUMBER() OVER (
                    PARTITION BY src._row_id
                    ORDER BY
                        CASE
                            WHEN hcpcs.hcpcs_code IS NULL THEN 2
                            WHEN src._claim_from_date IS NOT NULL
                                 AND COALESCE(hcpcs.act_eff_date, hcpcs.add_date, DATE '1900-01-01') <= src._claim_from_date
                                 AND (hcpcs.term_date IS NULL OR src._claim_from_date <= hcpcs.term_date)
                                THEN 0
                            ELSE 1
                        END,
                        CASE WHEN hcpcs.is_noc = 'false' THEN 0 ELSE 1 END,
                        COALESCE(hcpcs.act_eff_date, hcpcs.add_date, DATE '1900-01-01') DESC,
                        COALESCE(hcpcs.add_date, DATE '1900-01-01') DESC,
                        LENGTH(COALESCE(hcpcs.long_desc, '')) DESC
                ) AS _hcpcs_rank
            FROM src
            LEFT JOIN npi_map AS billing
                ON CAST(src.BILLING_PROVIDER_NPI_NUM AS VARCHAR) = billing.npi
            LEFT JOIN npi_map AS servicing
                ON CAST(src.SERVICING_PROVIDER_NPI_NUM AS VARCHAR) = servicing.npi
            LEFT JOIN hcpcs_map AS hcpcs
                ON CAST(src.HCPCS_CODE AS VARCHAR) = hcpcs.hcpcs_code
               AND hcpcs.status = 'ok'
        )
        SELECT * EXCLUDE (_row_id, _claim_from_date, _hcpcs_rank)
        FROM joined
        WHERE _hcpcs_rank = 1
        "
    );

    let extension = output_path
        .extension()
        .and_then(|x| x.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let copy_sql = match extension.as_str() {
        "parquet" => {
            format!("COPY ({select_sql}) TO '{output_path_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        }
        "csv" => format!("COPY ({select_sql}) TO '{output_path_sql}' (FORMAT CSV, HEADER)"),
        _ => bail!(
            "Unsupported output extension for {}. Use .csv or .parquet",
            output_path.display()
        ),
    };

    conn.execute_batch(&copy_sql)
        .context("Failed writing enriched dataset")?;
    Ok(())
}
