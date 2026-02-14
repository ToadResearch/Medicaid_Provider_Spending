use std::ops::Bound;
use std::path::Path;

use anyhow::{Context, anyhow};
use duckdb::Connection;
use serde::Serialize;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{AllQuery, BooleanQuery, Query, QueryParser, TermQuery};
use tantivy::schema::{
    Facet, FacetOptions, Field, IndexRecordOption, NumericOptions, STORED, STRING, Schema, TEXT,
    Value,
};
use tantivy::{DocAddress, Index, IndexReader, Order, Score, TantivyDocument, Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Billing,
    Servicing,
    Total,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    Relevance,
    PaidDesc,
    PaidAsc,
    ClaimsDesc,
    ClaimsAsc,
    NameAsc,
}

#[derive(Debug, Clone)]
pub struct ProviderSearchQuery {
    pub q: Option<String>,
    pub states: Vec<String>,
    pub taxonomies: Vec<String>,
    pub entity: Option<String>,
    pub role: Role,
    pub paid_min: Option<f64>,
    pub paid_max: Option<f64>,
    pub claims_min: Option<i64>,
    pub claims_max: Option<i64>,
    pub sort: Sort,
    pub page: usize,
    pub page_size: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderHit {
    pub npi: String,
    pub display_name: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub enumeration_type: Option<String>,
    pub primary_taxonomy_code: Option<String>,
    pub primary_taxonomy_desc: Option<String>,

    pub paid_billing: f64,
    pub claims_billing: i64,
    pub bene_billing: i64,
    pub paid_servicing: f64,
    pub claims_servicing: i64,
    pub bene_servicing: i64,
    pub paid_total: f64,
    pub claims_total: i64,
    pub bene_total: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderSearchResponse {
    pub total_hits: usize,
    pub hits: Vec<ProviderHit>,
}

#[derive(Clone)]
pub struct ProviderEngine {
    reader: IndexReader,
    fields: ProviderFields,
    query_parser: QueryParser,
}

#[derive(Debug, Clone)]
struct ProviderFields {
    npi: Field,
    display_name: Field,
    city: Field,
    state: Field,
    enumeration_type: Field,
    primary_taxonomy_code: Field,
    primary_taxonomy_desc: Field,

    state_facet: Field,
    entity_facet: Field,
    tax_facet: Field,

    paid_billing: Field,
    claims_billing: Field,
    bene_billing: Field,
    paid_servicing: Field,
    claims_servicing: Field,
    bene_servicing: Field,
    paid_total: Field,
    claims_total: Field,
    bene_total: Field,
}

impl ProviderEngine {
    pub fn open(index_dir: &Path) -> anyhow::Result<Self> {
        let dir = MmapDirectory::open(index_dir)
            .with_context(|| format!("open index dir {}", index_dir.display()))?;
        let index = Index::open(dir).context("open tantivy index")?;
        let schema = index.schema();
        let fields = provider_fields(&schema)?;

        let reader = index.reader().context("create index reader")?;
        let query_parser = QueryParser::for_index(
            &index,
            vec![
                fields.display_name,
                fields.city,
                fields.primary_taxonomy_desc,
            ],
        );

        Ok(Self {
            reader,
            fields,
            query_parser,
        })
    }

    pub fn search(&self, q: ProviderSearchQuery) -> anyhow::Result<ProviderSearchResponse> {
        let searcher = self.reader.searcher();
        let query = self.build_query(&q)?;

        // total hits (for pagination UI)
        let total_hits = searcher
            .search(&query, &tantivy::collector::Count)
            .context("count hits")?;

        let page_size = q.page_size.clamp(1, 200);
        let offset = q.page.saturating_mul(page_size);

        let hits: Vec<ProviderHit> = match q.sort {
            Sort::Relevance => {
                let top_docs: Vec<(Score, DocAddress)> = searcher
                    .search(&query, &TopDocs::with_limit(page_size).and_offset(offset))
                    .context("tantivy search")?;
                top_docs
                    .into_iter()
                    .map(|(_, addr)| self.doc_to_hit(&searcher, addr))
                    .collect::<anyhow::Result<Vec<_>>>()?
            }
            Sort::PaidDesc | Sort::PaidAsc | Sort::ClaimsDesc | Sort::ClaimsAsc => {
                let (paid_name, claims_name) = role_field_names(q.role);
                let doc_addrs: Vec<DocAddress> = match q.sort {
                    Sort::PaidDesc => {
                        let top_docs: Vec<(f64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<f64>(paid_name, Order::Desc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    Sort::PaidAsc => {
                        let top_docs: Vec<(f64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<f64>(paid_name, Order::Asc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    Sort::ClaimsDesc => {
                        let top_docs: Vec<(i64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<i64>(claims_name, Order::Desc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    Sort::ClaimsAsc => {
                        let top_docs: Vec<(i64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<i64>(claims_name, Order::Asc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    _ => unreachable!(),
                };

                // Note: top_docs carries the fast-field value, not Score.
                doc_addrs
                    .into_iter()
                    .map(|addr| self.doc_to_hit(&searcher, addr))
                    .collect::<anyhow::Result<Vec<_>>>()?
            }
            Sort::NameAsc => {
                // Tantivy doesn't support stable lexicographic sorting out of the box.
                // We approximate by taking a larger window and sorting in-memory.
                let window = ((offset + page_size) * 20).clamp(page_size, 5000);
                let top_docs: Vec<(Score, DocAddress)> = searcher
                    .search(&query, &TopDocs::with_limit(window))
                    .context("tantivy search (name_asc window)")?;
                let mut docs = top_docs
                    .into_iter()
                    .map(|(_, addr)| self.doc_to_hit(&searcher, addr))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                docs.sort_by(|a, b| {
                    a.display_name
                        .as_deref()
                        .unwrap_or("")
                        .cmp(b.display_name.as_deref().unwrap_or(""))
                });
                docs.into_iter().skip(offset).take(page_size).collect()
            }
        };

        Ok(ProviderSearchResponse { total_hits, hits })
    }

    pub fn search_simple(&self, q: &str, limit: usize) -> anyhow::Result<Vec<ProviderHit>> {
        let searcher = self.reader.searcher();
        let query = self.build_simple_query(q)?;
        let limit = limit.clamp(1, 50);
        let top_docs: Vec<(Score, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(limit))?;
        top_docs
            .into_iter()
            .map(|(_, addr)| self.doc_to_hit(&searcher, addr))
            .collect()
    }

    fn build_simple_query(&self, q: &str) -> anyhow::Result<Box<dyn Query>> {
        if looks_like_npi(q) {
            let term = Term::from_field_text(self.fields.npi, q.trim());
            Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
        } else {
            Ok(self.query_parser.parse_query(q).context("parse query")?)
        }
    }

    fn build_query(&self, q: &ProviderSearchQuery) -> anyhow::Result<Box<dyn Query>> {
        let mut clauses: Vec<(tantivy::query::Occur, Box<dyn Query>)> = Vec::new();

        // main query
        let base: Box<dyn Query> = match q.q.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            None => Box::new(AllQuery),
            Some(s) => self.build_simple_query(s)?,
        };
        clauses.push((tantivy::query::Occur::Must, base));

        // facet filters
        if !q.states.is_empty() {
            clauses.push((
                tantivy::query::Occur::Must,
                facet_or_query(self.fields.state_facet, "/state", &q.states),
            ));
        }
        if !q.taxonomies.is_empty() {
            clauses.push((
                tantivy::query::Occur::Must,
                facet_or_query(self.fields.tax_facet, "/tax", &q.taxonomies),
            ));
        }
        if let Some(entity) = q.entity.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            let f = Facet::from(&format!("/entity/{entity}"));
            let term = Term::from_facet(self.fields.entity_facet, &f);
            clauses.push((
                tantivy::query::Occur::Must,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }

        // numeric range filters (role-aware)
        let (paid_name, claims_name) = role_field_names(q.role);

        if q.paid_min.is_some() || q.paid_max.is_some() {
            let lb = q.paid_min.map(Bound::Included).unwrap_or(Bound::Unbounded);
            let ub = q.paid_max.map(Bound::Included).unwrap_or(Bound::Unbounded);
            clauses.push((
                tantivy::query::Occur::Must,
                Box::new(tantivy::query::RangeQuery::new_f64_bounds(
                    paid_name.to_string(),
                    lb,
                    ub,
                )),
            ));
        }

        if q.claims_min.is_some() || q.claims_max.is_some() {
            let lb = q
                .claims_min
                .map(Bound::Included)
                .unwrap_or(Bound::Unbounded);
            let ub = q
                .claims_max
                .map(Bound::Included)
                .unwrap_or(Bound::Unbounded);
            clauses.push((
                tantivy::query::Occur::Must,
                Box::new(tantivy::query::RangeQuery::new_i64_bounds(
                    claims_name.to_string(),
                    lb,
                    ub,
                )),
            ));
        }

        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    fn doc_to_hit(
        &self,
        searcher: &tantivy::Searcher,
        addr: DocAddress,
    ) -> anyhow::Result<ProviderHit> {
        let doc: TantivyDocument = searcher.doc(addr)?;

        let npi = doc
            .get_first(self.fields.npi)
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("missing npi in doc"))?
            .to_string();

        Ok(ProviderHit {
            npi,
            display_name: doc
                .get_first(self.fields.display_name)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            city: doc
                .get_first(self.fields.city)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            state: doc
                .get_first(self.fields.state)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            enumeration_type: doc
                .get_first(self.fields.enumeration_type)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            primary_taxonomy_code: doc
                .get_first(self.fields.primary_taxonomy_code)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            primary_taxonomy_desc: doc
                .get_first(self.fields.primary_taxonomy_desc)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            paid_billing: doc
                .get_first(self.fields.paid_billing)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            claims_billing: doc
                .get_first(self.fields.claims_billing)
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            bene_billing: doc
                .get_first(self.fields.bene_billing)
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            paid_servicing: doc
                .get_first(self.fields.paid_servicing)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            claims_servicing: doc
                .get_first(self.fields.claims_servicing)
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            bene_servicing: doc
                .get_first(self.fields.bene_servicing)
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            paid_total: doc
                .get_first(self.fields.paid_total)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            claims_total: doc
                .get_first(self.fields.claims_total)
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            bene_total: doc
                .get_first(self.fields.bene_total)
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
        })
    }
}

pub fn build_provider_index(
    conn: &Connection,
    index_dir: &Path,
    rebuild: bool,
) -> anyhow::Result<()> {
    let success_marker = index_dir.join("_SUCCESS");
    if index_dir.exists() && !rebuild {
        if success_marker.exists() {
            tracing::info!(
                "Provider index already exists at {}; skipping",
                index_dir.display()
            );
            return Ok(());
        }
        tracing::info!(
            "Provider index dir exists but is missing {} (previous build likely failed); rebuilding",
            success_marker.display()
        );
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("remove {}", index_dir.display()))?;
    } else if rebuild && index_dir.exists() {
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("remove {}", index_dir.display()))?;
    }
    std::fs::create_dir_all(index_dir).with_context(|| format!("mkdir {}", index_dir.display()))?;

    let schema = provider_schema();
    let index = Index::create_in_dir(index_dir, schema).context("create provider index")?;
    let mut writer = index
        .writer_with_num_threads(4, 512_000_000)
        .context("create index writer")?;

    let schema = index.schema();
    let fields = provider_fields(&schema)?;

    let sql = r#"
        SELECT
          npi,
          display_name,
          enumeration_type,
          primary_taxonomy_code,
          primary_taxonomy_desc,
          state,
          city,
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
    "#;

    let mut stmt = conn.prepare(sql).context("prepare provider_search scan")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<usize, Option<String>>(0)?,
            row.get::<usize, Option<String>>(1)?,
            row.get::<usize, Option<String>>(2)?,
            row.get::<usize, Option<String>>(3)?,
            row.get::<usize, Option<String>>(4)?,
            row.get::<usize, Option<String>>(5)?,
            row.get::<usize, Option<String>>(6)?,
            row.get::<usize, Option<f64>>(7)?,
            row.get::<usize, Option<i64>>(8)?,
            row.get::<usize, Option<i64>>(9)?,
            row.get::<usize, Option<f64>>(10)?,
            row.get::<usize, Option<i64>>(11)?,
            row.get::<usize, Option<i64>>(12)?,
            row.get::<usize, Option<f64>>(13)?,
            row.get::<usize, Option<i64>>(14)?,
            row.get::<usize, Option<i64>>(15)?,
        ))
    })?;

    let mut count: u64 = 0;
    let mut skipped: u64 = 0;
    for r in rows {
        let (
            npi,
            display_name,
            enumeration_type,
            tax_code,
            tax_desc,
            state,
            city,
            paid_billing,
            claims_billing,
            bene_billing,
            paid_servicing,
            claims_servicing,
            bene_servicing,
            paid_total,
            claims_total,
            bene_total,
        ) = r?;

        let Some(npi) = npi.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
            skipped += 1;
            continue;
        };
        let npi = npi.to_string();

        let mut doc = tantivy::doc!();
        doc.add_text(fields.npi, &npi);

        if let Some(v) = display_name.as_deref().filter(|s| !s.trim().is_empty()) {
            doc.add_text(fields.display_name, v);
        }
        if let Some(v) = city.as_deref().filter(|s| !s.trim().is_empty()) {
            doc.add_text(fields.city, v);
        }
        if let Some(v) = state.as_deref().filter(|s| !s.trim().is_empty()) {
            let state_up = v.trim().to_uppercase();
            doc.add_text(fields.state, &state_up);
            doc.add_facet(
                fields.state_facet,
                Facet::from(&format!("/state/{state_up}")),
            );
        }
        if let Some(v) = enumeration_type.as_deref().filter(|s| !s.trim().is_empty()) {
            let ent = v.trim();
            doc.add_text(fields.enumeration_type, ent);
            doc.add_facet(fields.entity_facet, Facet::from(&format!("/entity/{ent}")));
        }
        if let Some(v) = tax_code.as_deref().filter(|s| !s.trim().is_empty()) {
            let t = v.trim().to_string();
            doc.add_text(fields.primary_taxonomy_code, &t);
            doc.add_facet(fields.tax_facet, Facet::from(&format!("/tax/{t}")));
        }
        if let Some(v) = tax_desc.as_deref().filter(|s| !s.trim().is_empty()) {
            doc.add_text(fields.primary_taxonomy_desc, v);
        }

        doc.add_f64(fields.paid_billing, paid_billing.unwrap_or(0.0));
        doc.add_i64(fields.claims_billing, claims_billing.unwrap_or(0));
        doc.add_i64(fields.bene_billing, bene_billing.unwrap_or(0));
        doc.add_f64(fields.paid_servicing, paid_servicing.unwrap_or(0.0));
        doc.add_i64(fields.claims_servicing, claims_servicing.unwrap_or(0));
        doc.add_i64(fields.bene_servicing, bene_servicing.unwrap_or(0));
        doc.add_f64(fields.paid_total, paid_total.unwrap_or(0.0));
        doc.add_i64(fields.claims_total, claims_total.unwrap_or(0));
        doc.add_i64(fields.bene_total, bene_total.unwrap_or(0));

        writer.add_document(doc)?;
        count += 1;
        if count % 100_000 == 0 {
            tracing::info!("Indexed {} providers...", count);
        }
    }

    if skipped > 0 {
        tracing::info!("Skipped {} rows with NULL/empty npi", skipped);
    }
    tracing::info!("Committing provider index ({} docs)...", count);
    writer.commit().context("commit provider index")?;

    let _ = std::fs::write(&success_marker, "ok\n");
    Ok(())
}

fn provider_schema() -> Schema {
    let mut b = Schema::builder();

    b.add_text_field("npi", STRING | STORED);
    b.add_text_field("display_name", TEXT | STORED);
    b.add_text_field("city", TEXT | STORED);
    b.add_text_field("state", STRING | STORED);
    b.add_text_field("enumeration_type", STRING | STORED);
    b.add_text_field("primary_taxonomy_code", STRING | STORED);
    b.add_text_field("primary_taxonomy_desc", TEXT | STORED);

    b.add_facet_field("state_facet", FacetOptions::default());
    b.add_facet_field("entity_facet", FacetOptions::default());
    b.add_facet_field("tax_facet", FacetOptions::default());

    let f64o = NumericOptions::default()
        .set_fast()
        .set_indexed()
        .set_stored();
    let i64o = NumericOptions::default()
        .set_fast()
        .set_indexed()
        .set_stored();

    b.add_f64_field("paid_billing", f64o.clone());
    b.add_i64_field("claims_billing", i64o.clone());
    b.add_i64_field("bene_billing", i64o.clone());
    b.add_f64_field("paid_servicing", f64o.clone());
    b.add_i64_field("claims_servicing", i64o.clone());
    b.add_i64_field("bene_servicing", i64o.clone());
    b.add_f64_field("paid_total", f64o.clone());
    b.add_i64_field("claims_total", i64o.clone());
    b.add_i64_field("bene_total", i64o.clone());

    b.build()
}

fn provider_fields(schema: &Schema) -> anyhow::Result<ProviderFields> {
    Ok(ProviderFields {
        npi: schema.get_field("npi")?,
        display_name: schema.get_field("display_name")?,
        city: schema.get_field("city")?,
        state: schema.get_field("state")?,
        enumeration_type: schema.get_field("enumeration_type")?,
        primary_taxonomy_code: schema.get_field("primary_taxonomy_code")?,
        primary_taxonomy_desc: schema.get_field("primary_taxonomy_desc")?,

        state_facet: schema.get_field("state_facet")?,
        entity_facet: schema.get_field("entity_facet")?,
        tax_facet: schema.get_field("tax_facet")?,

        paid_billing: schema.get_field("paid_billing")?,
        claims_billing: schema.get_field("claims_billing")?,
        bene_billing: schema.get_field("bene_billing")?,
        paid_servicing: schema.get_field("paid_servicing")?,
        claims_servicing: schema.get_field("claims_servicing")?,
        bene_servicing: schema.get_field("bene_servicing")?,
        paid_total: schema.get_field("paid_total")?,
        claims_total: schema.get_field("claims_total")?,
        bene_total: schema.get_field("bene_total")?,
    })
}

fn facet_or_query(field: Field, prefix: &str, values: &[String]) -> Box<dyn Query> {
    let mut should = Vec::new();
    for v in values {
        let v = v.trim();
        if v.is_empty() {
            continue;
        }
        let f = Facet::from(&format!("{prefix}/{v}"));
        let term = Term::from_facet(field, &f);
        should.push((
            tantivy::query::Occur::Should,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
        ));
    }
    Box::new(BooleanQuery::new(should))
}

fn looks_like_npi(q: &str) -> bool {
    let s = q.trim();
    s.len() == 10 && s.chars().all(|c| c.is_ascii_digit())
}

fn role_field_names(role: Role) -> (&'static str, &'static str) {
    match role {
        Role::Billing => ("paid_billing", "claims_billing"),
        Role::Servicing => ("paid_servicing", "claims_servicing"),
        Role::Total => ("paid_total", "claims_total"),
    }
}
