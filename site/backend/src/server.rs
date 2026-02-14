use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use duckdb::{Connection, OptionalExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use crate::cli::ServeArgs;
use crate::index::hcpcs::{HcpcsEngine, HcpcsSearchQuery, Sort as HcpcsSort};
use crate::index::providers::{ProviderEngine, ProviderSearchQuery, Role, Sort as ProviderSort};
use crate::storage::{StoragePaths, file_present_nonempty};

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    providers: Arc<ProviderEngine>,
    hcpcs: Arc<HcpcsEngine>,
    npi_json_col: String,
    hcpcs_json_col: String,
    meta: Option<serde_json::Value>,
}

pub async fn run(opts: ServeArgs) -> anyhow::Result<()> {
    let paths = StoragePaths::new(&opts.data_dir);
    if !file_present_nonempty(&paths.duckdb_path) {
        return Err(anyhow!(
            "DuckDB not found at {}. Run: site-backend build",
            paths.duckdb_path.display()
        ));
    }
    if !paths.provider_index_dir.exists() {
        return Err(anyhow!(
            "Provider index not found at {}. Run: site-backend build",
            paths.provider_index_dir.display()
        ));
    }
    if !paths.hcpcs_index_dir.exists() {
        return Err(anyhow!(
            "HCPCS index not found at {}. Run: site-backend build",
            paths.hcpcs_index_dir.display()
        ));
    }

    let mut conn = Connection::open(&paths.duckdb_path)
        .with_context(|| format!("open duckdb at {}", paths.duckdb_path.display()))?;

    // Ensure parquet-backed views exist (for detail endpoints).
    let sources = paths.source_files();
    create_or_replace_views(&mut conn, &sources.spending, &sources.npi, &sources.hcpcs)
        .context("create views")?;

    let npi_json_col = detect_json_col(&mut conn, "npi_api_raw")?;
    let hcpcs_json_col = detect_json_col(&mut conn, "hcpcs_api_raw")?;

    let providers =
        ProviderEngine::open(&paths.provider_index_dir).context("open provider index")?;
    let hcpcs = HcpcsEngine::open(&paths.hcpcs_index_dir).context("open hcpcs index")?;

    let meta = if std::fs::metadata(&paths.meta_path)
        .map(|m| m.len() > 0)
        .unwrap_or(false)
    {
        let s = std::fs::read_to_string(&paths.meta_path)?;
        serde_json::from_str(&s).ok()
    } else {
        None
    };

    let state = AppState {
        db: Arc::new(Mutex::new(conn)),
        providers: Arc::new(providers),
        hcpcs: Arc::new(hcpcs),
        npi_json_col,
        hcpcs_json_col,
        meta,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/stats", get(api_stats))
        .route("/api/search", get(api_global_search))
        .route("/api/filters/providers", get(api_provider_filters))
        .route("/api/providers/search", get(api_provider_search))
        .route("/api/providers/:npi", get(api_provider_detail))
        .route("/api/hcpcs/search", get(api_hcpcs_search))
        .route("/api/hcpcs/:code", get(api_hcpcs_detail))
        .route("/api/map/zips", get(api_map_zips))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", opts.host, opts.port)
        .parse()
        .context("parse host:port")?;

    tracing::info!("Listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    meta: Option<serde_json::Value>,
}

async fn api_stats(State(st): State<AppState>) -> impl IntoResponse {
    Json(StatsResponse { meta: st.meta })
}

#[derive(Debug, Deserialize)]
struct GlobalSearchParams {
    q: String,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct GlobalSearchResponse {
    providers: Vec<crate::index::providers::ProviderHit>,
    hcpcs: Vec<crate::index::hcpcs::HcpcsHit>,
}

async fn api_global_search(
    State(st): State<AppState>,
    Query(p): Query<GlobalSearchParams>,
) -> impl IntoResponse {
    let limit = p.limit.unwrap_or(10);
    let providers = match st.providers.search_simple(&p.q, limit) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let hcpcs = match st.hcpcs.search_simple(&p.q, limit) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    Json(GlobalSearchResponse { providers, hcpcs }).into_response()
}

#[derive(Debug, Serialize)]
struct ProviderFiltersResponse {
    states: Vec<String>,
    entities: Vec<String>,
    taxonomies: Vec<TaxonomyOpt>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TaxonomyOpt {
    code: String,
    desc: Option<String>,
    count: u64,
}

async fn api_provider_filters(State(st): State<AppState>) -> impl IntoResponse {
    let mut db = st.db.lock().await;

    let states = query_string_list(
        &mut db,
        "SELECT DISTINCT state FROM provider_search WHERE state IS NOT NULL ORDER BY state ASC",
    )
    .unwrap_or_default();
    let entities = query_string_list(&mut db, "SELECT DISTINCT enumeration_type FROM provider_search WHERE enumeration_type IS NOT NULL ORDER BY enumeration_type ASC")
        .unwrap_or_default();

    let taxonomies = query_taxonomy_list(&mut db).unwrap_or_default();

    Json(ProviderFiltersResponse {
        states,
        entities,
        taxonomies,
    })
}

fn query_string_list(db: &mut Connection, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = db.prepare(sql)?;
    let mut out = Vec::new();
    let rows = stmt.query_map([], |row| row.get::<usize, String>(0))?;
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

fn query_taxonomy_list(db: &mut Connection) -> anyhow::Result<Vec<TaxonomyOpt>> {
    let sql = r#"
        SELECT
          primary_taxonomy_code,
          ANY_VALUE(primary_taxonomy_desc) AS primary_taxonomy_desc,
          COUNT(*) AS provider_count
        FROM provider_search
        WHERE primary_taxonomy_code IS NOT NULL
        GROUP BY primary_taxonomy_code
        ORDER BY provider_count DESC
        LIMIT 2000
    "#;
    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(TaxonomyOpt {
            code: row.get::<usize, String>(0)?,
            desc: row.get::<usize, Option<String>>(1)?,
            count: row.get::<usize, i64>(2)?.max(0) as u64,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct ProviderSearchParams {
    q: Option<String>,
    state: Option<Vec<String>>,
    taxonomy: Option<Vec<String>>,
    entity: Option<String>,
    role: Option<String>,
    paid_min: Option<f64>,
    paid_max: Option<f64>,
    claims_min: Option<i64>,
    claims_max: Option<i64>,
    sort: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

async fn api_provider_search(
    State(st): State<AppState>,
    Query(p): Query<ProviderSearchParams>,
) -> impl IntoResponse {
    let role = parse_role(p.role.as_deref());
    let sort = parse_provider_sort(p.sort.as_deref());

    // For fully alphabetical browsing, use DuckDB directly when q is empty.
    let q_empty = p.q.as_deref().map(str::trim).unwrap_or("").is_empty();
    if q_empty && sort == ProviderSort::NameAsc {
        return match duckdb_provider_search(&st, &p, role).await {
            Ok(r) => Json(r).into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        };
    }

    let query = ProviderSearchQuery {
        q: p.q.clone(),
        states: flatten_list(p.state),
        taxonomies: flatten_list(p.taxonomy),
        entity: p.entity.clone(),
        role,
        paid_min: p.paid_min,
        paid_max: p.paid_max,
        claims_min: p.claims_min,
        claims_max: p.claims_max,
        sort,
        page: p.page.unwrap_or(0),
        page_size: p.page_size.unwrap_or(50),
    };

    match st.providers.search(query) {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn duckdb_provider_search(
    st: &AppState,
    p: &ProviderSearchParams,
    role: Role,
) -> anyhow::Result<crate::index::providers::ProviderSearchResponse> {
    let page_size = p.page_size.unwrap_or(50).clamp(1, 200);
    let offset = p.page.unwrap_or(0).saturating_mul(page_size);

    let mut where_sql = String::from("WHERE 1=1");
    if let Some(states) = &p.state {
        let states = flatten_list(Some(states.clone()));
        let states = states
            .into_iter()
            .filter(|s| s.len() == 2 && s.chars().all(|c| c.is_ascii_alphabetic()))
            .map(|s| s.to_uppercase())
            .collect::<Vec<_>>();
        if !states.is_empty() {
            where_sql.push_str(" AND state IN (");
            where_sql.push_str(
                &states
                    .iter()
                    .map(|s| format!("'{s}'"))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            where_sql.push(')');
        }
    }
    if let Some(entity) = p.entity.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        if entity == "NPI-1" || entity == "NPI-2" {
            where_sql.push_str(&format!(" AND enumeration_type = '{entity}'"));
        }
    }
    if let Some(taxes) = &p.taxonomy {
        let taxes = flatten_list(Some(taxes.clone()));
        let taxes = taxes
            .into_iter()
            .filter(|t| t.chars().all(|c| c.is_ascii_alphanumeric()))
            .collect::<Vec<_>>();
        if !taxes.is_empty() {
            where_sql.push_str(" AND primary_taxonomy_code IN (");
            where_sql.push_str(
                &taxes
                    .iter()
                    .map(|t| format!("'{t}'"))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            where_sql.push(')');
        }
    }

    let (paid_col, claims_col) = match role {
        Role::Billing => ("paid_billing", "claims_billing"),
        Role::Servicing => ("paid_servicing", "claims_servicing"),
        Role::Total => ("paid_total", "claims_total"),
    };

    if let Some(min) = p.paid_min {
        where_sql.push_str(&format!(" AND {paid_col} >= {min}"));
    }
    if let Some(max) = p.paid_max {
        where_sql.push_str(&format!(" AND {paid_col} <= {max}"));
    }
    if let Some(min) = p.claims_min {
        where_sql.push_str(&format!(" AND {claims_col} >= {min}"));
    }
    if let Some(max) = p.claims_max {
        where_sql.push_str(&format!(" AND {claims_col} <= {max}"));
    }

    let count_sql = format!("SELECT COUNT(*) FROM provider_search {where_sql}");
    let data_sql = format!(
        r#"
        SELECT
          npi,
          display_name,
          city,
          state,
          enumeration_type,
          primary_taxonomy_code,
          primary_taxonomy_desc,
          paid_billing,
          claims_billing,
          bene_billing,
          paid_servicing,
          claims_servicing,
          bene_servicing,
          paid_total,
          claims_total,
          bene_total
        FROM provider_search
        {where_sql}
        ORDER BY display_name ASC NULLS LAST, npi ASC
        LIMIT {page_size} OFFSET {offset}
    "#
    );

    let mut db = st.db.lock().await;
    let total_hits = query_one_i64(&mut db, &count_sql)? as usize;

    let mut stmt = db.prepare(&data_sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(crate::index::providers::ProviderHit {
            npi: row.get::<usize, String>(0)?,
            display_name: row.get::<usize, Option<String>>(1)?,
            city: row.get::<usize, Option<String>>(2)?,
            state: row.get::<usize, Option<String>>(3)?,
            enumeration_type: row.get::<usize, Option<String>>(4)?,
            primary_taxonomy_code: row.get::<usize, Option<String>>(5)?,
            primary_taxonomy_desc: row.get::<usize, Option<String>>(6)?,
            paid_billing: row.get::<usize, Option<f64>>(7)?.unwrap_or(0.0),
            claims_billing: row.get::<usize, Option<i64>>(8)?.unwrap_or(0),
            bene_billing: row.get::<usize, Option<i64>>(9)?.unwrap_or(0),
            paid_servicing: row.get::<usize, Option<f64>>(10)?.unwrap_or(0.0),
            claims_servicing: row.get::<usize, Option<i64>>(11)?.unwrap_or(0),
            bene_servicing: row.get::<usize, Option<i64>>(12)?.unwrap_or(0),
            paid_total: row.get::<usize, Option<f64>>(13)?.unwrap_or(0.0),
            claims_total: row.get::<usize, Option<i64>>(14)?.unwrap_or(0),
            bene_total: row.get::<usize, Option<i64>>(15)?.unwrap_or(0),
        })
    })?;
    let mut hits = Vec::new();
    for r in rows {
        hits.push(r?);
    }

    Ok(crate::index::providers::ProviderSearchResponse { total_hits, hits })
}

fn query_one_i64(db: &mut Connection, sql: &str) -> anyhow::Result<i64> {
    let mut stmt = db.prepare(sql)?;
    let v: i64 = stmt.query_row([], |row| row.get(0))?;
    Ok(v)
}

fn parse_role(s: Option<&str>) -> Role {
    match s.unwrap_or("total").to_ascii_lowercase().as_str() {
        "billing" => Role::Billing,
        "servicing" => Role::Servicing,
        _ => Role::Total,
    }
}

fn parse_provider_sort(s: Option<&str>) -> ProviderSort {
    match s.unwrap_or("paid_desc").to_ascii_lowercase().as_str() {
        "paid_asc" => ProviderSort::PaidAsc,
        "claims_desc" => ProviderSort::ClaimsDesc,
        "claims_asc" => ProviderSort::ClaimsAsc,
        "name_asc" => ProviderSort::NameAsc,
        "relevance" => ProviderSort::Relevance,
        _ => ProviderSort::PaidDesc,
    }
}

fn flatten_list(v: Option<Vec<String>>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(items) = v else {
        return out;
    };
    for item in items {
        for part in item.split(',') {
            let p = part.trim();
            if !p.is_empty() {
                out.push(p.to_string());
            }
        }
    }
    out
}

#[derive(Debug, Deserialize)]
struct HcpcsSearchParams {
    q: Option<String>,
    sort: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

async fn api_hcpcs_search(
    State(st): State<AppState>,
    Query(p): Query<HcpcsSearchParams>,
) -> impl IntoResponse {
    let sort = match p
        .sort
        .as_deref()
        .unwrap_or("paid_desc")
        .to_ascii_lowercase()
        .as_str()
    {
        "paid_asc" => HcpcsSort::PaidAsc,
        "claims_desc" => HcpcsSort::ClaimsDesc,
        "claims_asc" => HcpcsSort::ClaimsAsc,
        "relevance" => HcpcsSort::Relevance,
        _ => HcpcsSort::PaidDesc,
    };

    let query = HcpcsSearchQuery {
        q: p.q.clone(),
        sort,
        page: p.page.unwrap_or(0),
        page_size: p.page_size.unwrap_or(50),
    };
    match st.hcpcs.search(query) {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[derive(Debug, Serialize)]
struct ProviderDetailResponse {
    provider: Option<ProviderRow>,
    npi_api: Option<String>,
}

#[derive(Debug, Serialize)]
struct ProviderRow {
    npi: String,
    display_name: Option<String>,
    city: Option<String>,
    state: Option<String>,
    enumeration_type: Option<String>,
    primary_taxonomy_code: Option<String>,
    primary_taxonomy_desc: Option<String>,
    zip5: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,

    paid_billing: f64,
    claims_billing: i64,
    bene_billing: i64,
    paid_servicing: f64,
    claims_servicing: i64,
    bene_servicing: i64,
    paid_total: f64,
    claims_total: i64,
    bene_total: i64,
}

async fn api_provider_detail(
    State(st): State<AppState>,
    AxumPath(npi): AxumPath<String>,
) -> impl IntoResponse {
    let mut db = st.db.lock().await;
    match provider_detail(&mut db, &st.npi_json_col, &npi) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

fn provider_detail(
    db: &mut Connection,
    npi_json_col: &str,
    npi: &str,
) -> anyhow::Result<ProviderDetailResponse> {
    let provider_sql = r#"
        SELECT
          npi,
          display_name,
          city,
          state,
          enumeration_type,
          primary_taxonomy_code,
          primary_taxonomy_desc,
          zip5,
          lat,
          lon,
          paid_billing,
          claims_billing,
          bene_billing,
          paid_servicing,
          claims_servicing,
          bene_servicing,
          paid_total,
          claims_total,
          bene_total
        FROM provider_search
        WHERE npi = ?
        LIMIT 1
    "#;
    let provider: Option<ProviderRow> = {
        let mut stmt = db.prepare(provider_sql)?;
        stmt.query_row([npi], |row| {
            Ok(ProviderRow {
                npi: row.get(0)?,
                display_name: row.get(1)?,
                city: row.get(2)?,
                state: row.get(3)?,
                enumeration_type: row.get(4)?,
                primary_taxonomy_code: row.get(5)?,
                primary_taxonomy_desc: row.get(6)?,
                zip5: row.get(7)?,
                lat: row.get(8)?,
                lon: row.get(9)?,
                paid_billing: row.get::<usize, Option<f64>>(10)?.unwrap_or(0.0),
                claims_billing: row.get::<usize, Option<i64>>(11)?.unwrap_or(0),
                bene_billing: row.get::<usize, Option<i64>>(12)?.unwrap_or(0),
                paid_servicing: row.get::<usize, Option<f64>>(13)?.unwrap_or(0.0),
                claims_servicing: row.get::<usize, Option<i64>>(14)?.unwrap_or(0),
                bene_servicing: row.get::<usize, Option<i64>>(15)?.unwrap_or(0),
                paid_total: row.get::<usize, Option<f64>>(16)?.unwrap_or(0.0),
                claims_total: row.get::<usize, Option<i64>>(17)?.unwrap_or(0),
                bene_total: row.get::<usize, Option<i64>>(18)?.unwrap_or(0),
            })
        })
        .optional()?
    };

    let npi_sql = format!("SELECT {npi_json_col} FROM npi_api_raw WHERE npi = ? LIMIT 1");
    let npi_api: Option<String> = {
        let mut stmt = db.prepare(&npi_sql)?;
        stmt.query_row([npi], |row| row.get::<usize, Option<String>>(0))
            .optional()?
            .flatten()
    };

    Ok(ProviderDetailResponse { provider, npi_api })
}

#[derive(Debug, Serialize)]
struct HcpcsDetailResponse {
    hcpcs: Option<HcpcsRow>,
    hcpcs_api: Option<String>,
}

#[derive(Debug, Serialize)]
struct HcpcsRow {
    hcpcs_code: String,
    short_desc: Option<String>,
    long_desc: Option<String>,
    add_dt: Option<String>,
    act_eff_dt: Option<String>,
    term_dt: Option<String>,
    obsolete: Option<String>,
    is_noc: Option<String>,
    paid_total: f64,
    claims_total: i64,
    bene_total: i64,
}

async fn api_hcpcs_detail(
    State(st): State<AppState>,
    AxumPath(code): AxumPath<String>,
) -> impl IntoResponse {
    let mut db = st.db.lock().await;
    match hcpcs_detail(&mut db, &st.hcpcs_json_col, &code) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

fn hcpcs_detail(
    db: &mut Connection,
    hcpcs_json_col: &str,
    code: &str,
) -> anyhow::Result<HcpcsDetailResponse> {
    let hcpcs_sql = r#"
        SELECT
          hcpcs_code,
          short_desc,
          long_desc,
          add_dt,
          act_eff_dt,
          term_dt,
          obsolete,
          is_noc,
          paid_total,
          claims_total,
          bene_total
        FROM hcpcs_search
        WHERE hcpcs_code = ?
        LIMIT 1
    "#;
    let hcpcs: Option<HcpcsRow> = {
        let mut stmt = db.prepare(hcpcs_sql)?;
        stmt.query_row([code], |row| {
            Ok(HcpcsRow {
                hcpcs_code: row.get(0)?,
                short_desc: row.get(1)?,
                long_desc: row.get(2)?,
                add_dt: row.get(3)?,
                act_eff_dt: row.get(4)?,
                term_dt: row.get(5)?,
                obsolete: row.get(6)?,
                is_noc: row.get(7)?,
                paid_total: row.get::<usize, Option<f64>>(8)?.unwrap_or(0.0),
                claims_total: row.get::<usize, Option<i64>>(9)?.unwrap_or(0),
                bene_total: row.get::<usize, Option<i64>>(10)?.unwrap_or(0),
            })
        })
        .optional()?
    };

    let api_sql =
        format!("SELECT {hcpcs_json_col} FROM hcpcs_api_raw WHERE hcpcs_code = ? LIMIT 1");
    let hcpcs_api: Option<String> = {
        let mut stmt = db.prepare(&api_sql)?;
        stmt.query_row([code], |row| row.get::<usize, Option<String>>(0))
            .optional()?
            .flatten()
    };

    Ok(HcpcsDetailResponse { hcpcs, hcpcs_api })
}

#[derive(Debug, Deserialize)]
struct MapZipsParams {
    bbox: String,
    state: Option<Vec<String>>,
    taxonomy: Option<Vec<String>>,
    entity: Option<String>,
    role: Option<String>,
    metric: Option<String>,
}

#[derive(Debug, Serialize)]
struct MapZipPoint {
    zip5: String,
    lat: f64,
    lon: f64,
    provider_count: u64,
    metric_total: f64,
}

async fn api_map_zips(
    State(st): State<AppState>,
    Query(p): Query<MapZipsParams>,
) -> impl IntoResponse {
    let bbox = match parse_bbox(&p.bbox) {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let role = parse_role(p.role.as_deref());
    let metric = match p
        .metric
        .as_deref()
        .unwrap_or("paid")
        .to_ascii_lowercase()
        .as_str()
    {
        "claims" => "claims",
        "bene" => "bene",
        _ => "paid",
    };

    let (metric_col, metric_cast) = match (role, metric) {
        (Role::Billing, "paid") => ("paid_billing", "DOUBLE"),
        (Role::Billing, "claims") => ("claims_billing", "DOUBLE"),
        (Role::Billing, "bene") => ("bene_billing", "DOUBLE"),
        (Role::Servicing, "paid") => ("paid_servicing", "DOUBLE"),
        (Role::Servicing, "claims") => ("claims_servicing", "DOUBLE"),
        (Role::Servicing, "bene") => ("bene_servicing", "DOUBLE"),
        (Role::Total, "paid") => ("paid_total", "DOUBLE"),
        (Role::Total, "claims") => ("claims_total", "DOUBLE"),
        (Role::Total, "bene") => ("bene_total", "DOUBLE"),
        _ => ("paid_total", "DOUBLE"),
    };

    let mut where_sql = format!(
        "WHERE lat IS NOT NULL AND lon IS NOT NULL AND lat BETWEEN {} AND {} AND lon BETWEEN {} AND {}",
        bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon
    );

    if let Some(states) = &p.state {
        let states = flatten_list(Some(states.clone()));
        let states = states
            .into_iter()
            .filter(|s| s.len() == 2 && s.chars().all(|c| c.is_ascii_alphabetic()))
            .map(|s| s.to_uppercase())
            .collect::<Vec<_>>();
        if !states.is_empty() {
            where_sql.push_str(" AND state IN (");
            where_sql.push_str(
                &states
                    .iter()
                    .map(|s| format!("'{s}'"))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            where_sql.push(')');
        }
    }
    if let Some(entity) = p.entity.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        if entity == "NPI-1" || entity == "NPI-2" {
            where_sql.push_str(&format!(" AND enumeration_type = '{entity}'"));
        }
    }
    if let Some(taxes) = &p.taxonomy {
        let taxes = flatten_list(Some(taxes.clone()));
        let taxes = taxes
            .into_iter()
            .filter(|t| t.chars().all(|c| c.is_ascii_alphanumeric()))
            .collect::<Vec<_>>();
        if !taxes.is_empty() {
            where_sql.push_str(" AND primary_taxonomy_code IN (");
            where_sql.push_str(
                &taxes
                    .iter()
                    .map(|t| format!("'{t}'"))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            where_sql.push(')');
        }
    }

    let sql = format!(
        r#"
        SELECT
          zip5,
          lat,
          lon,
          COUNT(*) AS provider_count,
          SUM(CAST({metric_col} AS {metric_cast})) AS metric_total
        FROM provider_search
        {where_sql}
        GROUP BY zip5, lat, lon
        ORDER BY metric_total DESC
        LIMIT 20000
    "#
    );

    let db = st.db.lock().await;
    let mut stmt = match db.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let rows = match stmt.query_map([], |row| {
        Ok(MapZipPoint {
            zip5: row.get::<usize, String>(0)?,
            lat: row.get::<usize, f64>(1)?,
            lon: row.get::<usize, f64>(2)?,
            provider_count: row.get::<usize, i64>(3)?.max(0) as u64,
            metric_total: row.get::<usize, Option<f64>>(4)?.unwrap_or(0.0),
        })
    }) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let mut out = Vec::new();
    for r in rows {
        match r {
            Ok(v) => out.push(v),
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        }
    }

    Json(out).into_response()
}

#[derive(Debug, Clone, Copy)]
struct Bbox {
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

fn parse_bbox(s: &str) -> anyhow::Result<Bbox> {
    let parts: Vec<&str> = s.split(',').map(str::trim).collect();
    if parts.len() != 4 {
        return Err(anyhow!("bbox must be minLon,minLat,maxLon,maxLat"));
    }
    let min_lon: f64 = parts[0].parse()?;
    let min_lat: f64 = parts[1].parse()?;
    let max_lon: f64 = parts[2].parse()?;
    let max_lat: f64 = parts[3].parse()?;
    Ok(Bbox {
        min_lon,
        min_lat,
        max_lon,
        max_lat,
    })
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

fn sql_quote_path(path: &Path) -> String {
    path.display().to_string().replace('\'', "''")
}
