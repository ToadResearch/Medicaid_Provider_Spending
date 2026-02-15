use anyhow::{Context, Result, bail};
use duckdb::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::common::now_unix_seconds;
use crate::common::{project_root, sql_escape_path};

#[derive(Debug, Clone)]
struct ColumnAuditRow {
    column: String,
    rows_total: i64,
    null_count: i64,
    empty_list_count: i64,
}

#[derive(Debug, Clone)]
struct ParquetAuditSection {
    parquet_path: PathBuf,
    parquet_label: String,
    columns: Vec<ColumnAuditRow>,
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('\"', "\"\""))
}

fn escape_markdown_code(text: &str) -> String {
    text.replace('`', "\\`")
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed creating {}", parent.display()))?;
    }
    Ok(())
}

fn relative_label(path: &Path) -> String {
    let root = project_root();
    path.strip_prefix(&root)
        .ok()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string_lossy().to_string())
}

fn load_column_names(conn: &Connection, view_name: &str) -> Result<Vec<String>> {
    let query = format!("SELECT name FROM pragma_table_info('{view_name}') ORDER BY cid");
    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed preparing DuckDB pragma_table_info for {view_name}"))?;
    let mut rows = stmt
        .query([])
        .with_context(|| format!("Failed querying DuckDB pragma_table_info for {view_name}"))?;
    let mut names = Vec::new();
    while let Some(row) = rows
        .next()
        .with_context(|| format!("Failed iterating pragma_table_info rows for {view_name}"))?
    {
        let name: String = row.get(0).context("Failed reading column name")?;
        names.push(name);
    }
    Ok(names)
}

fn compute_parquet_audit(
    conn: &Connection,
    parquet_path: &Path,
    view_name: &str,
) -> Result<ParquetAuditSection> {
    if !parquet_path.exists() {
        bail!("Parquet file not found: {}", parquet_path.display());
    }

    let escaped = sql_escape_path(parquet_path);
    conn.execute(&format!("DROP VIEW IF EXISTS {view_name}"), [])
        .with_context(|| format!("Failed dropping DuckDB view {view_name}"))?;
    conn.execute(
        &format!("CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{escaped}')"),
        [],
    )
    .with_context(|| {
        format!(
            "Failed creating DuckDB view {view_name} for {}",
            parquet_path.display()
        )
    })?;

    let columns = load_column_names(conn, view_name)?;
    let select_exprs = {
        let mut parts = Vec::with_capacity(1 + columns.len() * 2);
        parts.push("COUNT(*)".to_string());
        for col in &columns {
            let ident = quote_ident(col);
            parts.push(format!(
                "COALESCE(SUM(CASE WHEN {ident} IS NULL THEN 1 ELSE 0 END), 0)"
            ));
            parts.push(format!(
                "COALESCE(SUM(CASE WHEN CAST({ident} AS VARCHAR) = '[]' THEN 1 ELSE 0 END), 0)"
            ));
        }
        parts
    };
    let query = format!("SELECT {} FROM {view_name}", select_exprs.join(", "));
    let mut stmt = conn
        .prepare(&query)
        .context("Failed preparing DuckDB audit aggregation query")?;
    let mut rows = stmt
        .query([])
        .context("Failed running DuckDB audit aggregation query")?;
    let row = rows
        .next()
        .context("Failed reading DuckDB audit row")?
        .context("DuckDB audit query returned no rows")?;

    let rows_total: i64 = row.get(0).context("Failed reading rows_total")?;
    let mut audits = Vec::with_capacity(columns.len());
    for (idx, col) in columns.into_iter().enumerate() {
        let null_count: i64 = row
            .get(1 + idx * 2)
            .with_context(|| format!("Failed reading null_count for {col}"))?;
        let empty_list_count: i64 = row
            .get(1 + idx * 2 + 1)
            .with_context(|| format!("Failed reading empty_list_count for {col}"))?;
        audits.push(ColumnAuditRow {
            column: col,
            rows_total,
            null_count,
            empty_list_count,
        });
    }

    audits.sort_by(|a, b| {
        b.null_count
            .cmp(&a.null_count)
            .then_with(|| b.empty_list_count.cmp(&a.empty_list_count))
            .then_with(|| a.column.cmp(&b.column))
    });

    Ok(ParquetAuditSection {
        parquet_path: parquet_path.to_path_buf(),
        parquet_label: relative_label(parquet_path),
        columns: audits,
    })
}

fn fmt_pct(numer: i64, denom: i64) -> String {
    if denom <= 0 {
        return "0.00%".to_string();
    }
    format!("{:.2}%", (numer as f64) * 100.0 / (denom as f64))
}

fn render_markdown_table(section: &ParquetAuditSection) -> String {
    let mut out = String::new();
    out.push_str(
        "| column | rows_total | null_count | null_pct | empty_list_count | empty_list_pct |\n",
    );
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: |\n");
    for row in &section.columns {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            row.column,
            row.rows_total,
            row.null_count,
            fmt_pct(row.null_count, row.rows_total),
            row.empty_list_count,
            fmt_pct(row.empty_list_count, row.rows_total),
        ));
    }
    out
}

fn write_atomic(path: &Path, contents: &str) -> Result<()> {
    ensure_parent_dir(path)?;
    let file_name = path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("output.md");
    let tmp_path = path.with_file_name(format!("{file_name}.tmp"));
    fs::write(&tmp_path, contents)
        .with_context(|| format!("Failed writing {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("Failed moving {} -> {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn update_hf_readme_with_audit(hf_readme_path: &Path, block: &str) -> Result<()> {
    if !hf_readme_path.exists() {
        bail!(
            "HF dataset README not found at {}",
            hf_readme_path.display()
        );
    }

    let begin = "<!-- BEGIN PARQUET_NULL_AUDIT -->";
    let end = "<!-- END PARQUET_NULL_AUDIT -->";

    let original = fs::read_to_string(hf_readme_path)
        .with_context(|| format!("Failed reading {}", hf_readme_path.display()))?;

    let replaced = if let (Some(b), Some(e)) = (original.find(begin), original.find(end)) {
        if e < b {
            bail!("Malformed audit markers in {}", hf_readme_path.display());
        }
        let mut next = String::new();
        let body_start = b + begin.len();
        next.push_str(&original[..body_start]);
        next.push('\n');
        next.push_str(block.trim());
        next.push('\n');
        next.push_str(&original[e..]);
        next
    } else if let Some(insert_at) = original.find("## Unmapped / Unresolved Identifier Counts") {
        let mut next = String::new();
        next.push_str(&original[..insert_at]);
        if !next.ends_with("\n\n") {
            if !next.ends_with('\n') {
                next.push('\n');
            }
            next.push('\n');
        }
        next.push_str("## Parquet Null / Empty-List Audit\n\n");
        next.push_str(begin);
        next.push('\n');
        next.push_str(block.trim());
        next.push('\n');
        next.push_str(end);
        next.push_str("\n\n");
        next.push_str(&original[insert_at..]);
        next
    } else {
        let mut next = original;
        if !next.ends_with("\n\n") {
            if !next.ends_with('\n') {
                next.push('\n');
            }
            next.push('\n');
        }
        next.push_str("## Parquet Null / Empty-List Audit\n\n");
        next.push_str(begin);
        next.push('\n');
        next.push_str(block.trim());
        next.push('\n');
        next.push_str(end);
        next.push('\n');
        next
    };

    write_atomic(hf_readme_path, &replaced)
}

pub fn generate_and_update_hf_docs(npi_parquet: &Path, hcpcs_parquet: &Path) -> Result<()> {
    let conn = Connection::open_in_memory().context("Failed opening DuckDB for null audit")?;
    let generated_at_unix = now_unix_seconds();

    let npi_section = compute_parquet_audit(&conn, npi_parquet, "npi")?;
    let hcpcs_section = compute_parquet_audit(&conn, hcpcs_parquet, "hcpcs")?;

    let audit_md_path = project_root().join("hf").join("parquet_null_audit.md");
    let hf_readme_path = project_root().join("hf").join("README.md");

    let standalone = {
        let mut out = String::new();
        out.push_str("# Parquet Null / Empty-List Audit\n\n");
        out.push_str(&format!(
            "- Generated at (unix seconds): {generated_at_unix}\n"
        ));
        out.push_str(&format!(
            "- NPI parquet: `{}`\n",
            escape_markdown_code(&npi_section.parquet_path.to_string_lossy())
        ));
        out.push_str(&format!(
            "- HCPCS parquet: `{}`\n\n",
            escape_markdown_code(&hcpcs_section.parquet_path.to_string_lossy())
        ));
        out.push_str("Notes:\n");
        out.push_str("- `null_count` counts actual Parquet nulls.\n");
        out.push_str("- `empty_list_count` counts the literal string value `\"[]\"` (JSON-encoded empty list).\n\n");
        out.push_str(&format!("## NPI (`{}`)\n\n", npi_section.parquet_label));
        out.push_str(&render_markdown_table(&npi_section));
        out.push('\n');
        out.push_str(&format!("## HCPCS (`{}`)\n\n", hcpcs_section.parquet_label));
        out.push_str(&render_markdown_table(&hcpcs_section));
        out
    };
    write_atomic(&audit_md_path, &standalone)?;

    let readme_block = {
        let mut out = String::new();
        out.push_str("_Auto-generated by `./build_datasets.sh` (or `cargo run --release --manifest-path build_datasets/Cargo.toml -- --null-check`)._\n\n");
        out.push_str(&format!(
            "- Generated at (unix seconds): {generated_at_unix}\n"
        ));
        out.push_str(&format!(
            "- NPI parquet: `{}`\n",
            escape_markdown_code(&npi_section.parquet_label)
        ));
        out.push_str(&format!(
            "- HCPCS parquet: `{}`\n\n",
            escape_markdown_code(&hcpcs_section.parquet_label)
        ));
        out.push_str("Notes:\n");
        out.push_str("- `null_count` counts actual Parquet nulls.\n");
        out.push_str("- `empty_list_count` counts the literal string value `\"[]\"` (JSON-encoded empty list).\n\n");
        out.push_str(&format!("### NPI (`{}`)\n\n", npi_section.parquet_label));
        out.push_str(&render_markdown_table(&npi_section));
        out.push('\n');
        out.push_str(&format!(
            "### HCPCS (`{}`)\n\n",
            hcpcs_section.parquet_label
        ));
        out.push_str(&render_markdown_table(&hcpcs_section));
        out
    };
    update_hf_readme_with_audit(&hf_readme_path, &readme_block)?;

    println!(
        "Wrote parquet null audit {} and updated HF dataset card {}",
        audit_md_path.display(),
        hf_readme_path.display()
    );

    Ok(())
}
