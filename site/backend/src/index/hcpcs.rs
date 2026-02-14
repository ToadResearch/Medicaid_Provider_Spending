use std::path::Path;

use anyhow::{Context, anyhow};
use duckdb::Connection;
use serde::Serialize;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{AllQuery, BooleanQuery, Query, QueryParser};
use tantivy::schema::{Field, NumericOptions, STORED, STRING, Schema, TEXT, Value};
use tantivy::{DocAddress, Index, IndexReader, Order, Score, TantivyDocument};

#[derive(Debug, Clone, Copy)]
pub enum Sort {
    Relevance,
    PaidDesc,
    PaidAsc,
    ClaimsDesc,
    ClaimsAsc,
}

#[derive(Debug, Clone)]
pub struct HcpcsSearchQuery {
    pub q: Option<String>,
    pub sort: Sort,
    pub page: usize,
    pub page_size: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct HcpcsHit {
    pub hcpcs_code: String,
    pub short_desc: Option<String>,
    pub long_desc: Option<String>,
    pub paid_total: f64,
    pub claims_total: i64,
    pub bene_total: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HcpcsSearchResponse {
    pub total_hits: usize,
    pub hits: Vec<HcpcsHit>,
}

#[derive(Clone)]
pub struct HcpcsEngine {
    reader: IndexReader,
    fields: HcpcsFields,
    query_parser: QueryParser,
}

#[derive(Debug, Clone)]
struct HcpcsFields {
    hcpcs_code: Field,
    short_desc: Field,
    long_desc: Field,
    paid_total: Field,
    claims_total: Field,
    bene_total: Field,
}

impl HcpcsEngine {
    pub fn open(index_dir: &Path) -> anyhow::Result<Self> {
        let dir = MmapDirectory::open(index_dir)
            .with_context(|| format!("open index dir {}", index_dir.display()))?;
        let index = Index::open(dir).context("open tantivy index")?;
        let schema = index.schema();
        let fields = hcpcs_fields(&schema)?;

        let reader = index.reader().context("create index reader")?;
        let query_parser = QueryParser::for_index(
            &index,
            vec![fields.hcpcs_code, fields.short_desc, fields.long_desc],
        );

        Ok(Self {
            reader,
            fields,
            query_parser,
        })
    }

    pub fn search(&self, q: HcpcsSearchQuery) -> anyhow::Result<HcpcsSearchResponse> {
        let searcher = self.reader.searcher();
        let query = self.build_query(&q)?;
        let total_hits = searcher
            .search(&query, &tantivy::collector::Count)
            .context("count hits")?;

        let page_size = q.page_size.clamp(1, 200);
        let offset = q.page.saturating_mul(page_size);

        let hits: Vec<HcpcsHit> = match q.sort {
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
                let doc_addrs: Vec<DocAddress> = match q.sort {
                    Sort::PaidDesc => {
                        let top_docs: Vec<(f64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<f64>("paid_total", Order::Desc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    Sort::PaidAsc => {
                        let top_docs: Vec<(f64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<f64>("paid_total", Order::Asc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    Sort::ClaimsDesc => {
                        let top_docs: Vec<(i64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<i64>("claims_total", Order::Desc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    Sort::ClaimsAsc => {
                        let top_docs: Vec<(i64, DocAddress)> = searcher.search(
                            &query,
                            &TopDocs::with_limit(page_size)
                                .and_offset(offset)
                                .order_by_fast_field::<i64>("claims_total", Order::Asc),
                        )?;
                        top_docs.into_iter().map(|(_, a)| a).collect()
                    }
                    _ => unreachable!(),
                };

                doc_addrs
                    .into_iter()
                    .map(|addr| self.doc_to_hit(&searcher, addr))
                    .collect::<anyhow::Result<Vec<_>>>()?
            }
        };

        Ok(HcpcsSearchResponse { total_hits, hits })
    }

    pub fn search_simple(&self, q: &str, limit: usize) -> anyhow::Result<Vec<HcpcsHit>> {
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
        Ok(self.query_parser.parse_query(q).context("parse query")?)
    }

    fn build_query(&self, q: &HcpcsSearchQuery) -> anyhow::Result<Box<dyn Query>> {
        let mut clauses: Vec<(tantivy::query::Occur, Box<dyn Query>)> = Vec::new();
        let base: Box<dyn Query> = match q.q.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            None => Box::new(AllQuery),
            Some(s) => self.build_simple_query(s)?,
        };
        clauses.push((tantivy::query::Occur::Must, base));
        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    fn doc_to_hit(
        &self,
        searcher: &tantivy::Searcher,
        addr: DocAddress,
    ) -> anyhow::Result<HcpcsHit> {
        let doc: TantivyDocument = searcher.doc(addr)?;
        let hcpcs_code = doc
            .get_first(self.fields.hcpcs_code)
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("missing hcpcs_code in doc"))?
            .to_string();

        Ok(HcpcsHit {
            hcpcs_code,
            short_desc: doc
                .get_first(self.fields.short_desc)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            long_desc: doc
                .get_first(self.fields.long_desc)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
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

pub fn build_hcpcs_index(conn: &Connection, index_dir: &Path, rebuild: bool) -> anyhow::Result<()> {
    let success_marker = index_dir.join("_SUCCESS");
    if index_dir.exists() && !rebuild {
        if success_marker.exists() {
            tracing::info!(
                "HCPCS index already exists at {}; skipping",
                index_dir.display()
            );
            return Ok(());
        }
        tracing::info!(
            "HCPCS index dir exists but is missing {} (previous build likely failed); rebuilding",
            success_marker.display()
        );
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("remove {}", index_dir.display()))?;
    } else if rebuild && index_dir.exists() {
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("remove {}", index_dir.display()))?;
    }
    std::fs::create_dir_all(index_dir).with_context(|| format!("mkdir {}", index_dir.display()))?;

    let schema = hcpcs_schema();
    let index = Index::create_in_dir(index_dir, schema).context("create hcpcs index")?;
    let mut writer = index
        .writer_with_num_threads(2, 256_000_000)
        .context("create index writer")?;

    let schema = index.schema();
    let fields = hcpcs_fields(&schema)?;

    let sql = r#"
        SELECT
          hcpcs_code,
          short_desc,
          long_desc,
          paid_total,
          claims_total,
          bene_total
        FROM hcpcs_search
    "#;
    let mut stmt = conn.prepare(sql).context("prepare hcpcs_search scan")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<usize, Option<String>>(0)?,
            row.get::<usize, Option<String>>(1)?,
            row.get::<usize, Option<String>>(2)?,
            row.get::<usize, Option<f64>>(3)?,
            row.get::<usize, Option<i64>>(4)?,
            row.get::<usize, Option<i64>>(5)?,
        ))
    })?;

    let mut count: u64 = 0;
    let mut skipped: u64 = 0;
    for r in rows {
        let (code, short, long, paid, claims, bene) = r?;
        let Some(code) = code.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
            skipped += 1;
            continue;
        };
        let code = code.to_string();

        let mut doc = tantivy::doc!();
        doc.add_text(fields.hcpcs_code, &code);
        if let Some(v) = short.as_deref().filter(|s| !s.trim().is_empty()) {
            doc.add_text(fields.short_desc, v);
        }
        if let Some(v) = long.as_deref().filter(|s| !s.trim().is_empty()) {
            doc.add_text(fields.long_desc, v);
        }
        doc.add_f64(fields.paid_total, paid.unwrap_or(0.0));
        doc.add_i64(fields.claims_total, claims.unwrap_or(0));
        doc.add_i64(fields.bene_total, bene.unwrap_or(0));

        writer.add_document(doc)?;
        count += 1;
    }

    if skipped > 0 {
        tracing::info!("Skipped {} rows with NULL/empty hcpcs_code", skipped);
    }
    tracing::info!("Committing hcpcs index ({} docs)...", count);
    writer.commit().context("commit hcpcs index")?;

    let _ = std::fs::write(&success_marker, "ok\n");
    Ok(())
}

fn hcpcs_schema() -> Schema {
    let mut b = Schema::builder();
    b.add_text_field("hcpcs_code", STRING | STORED);
    b.add_text_field("short_desc", TEXT | STORED);
    b.add_text_field("long_desc", TEXT | STORED);

    let f64o = NumericOptions::default()
        .set_fast()
        .set_indexed()
        .set_stored();
    let i64o = NumericOptions::default()
        .set_fast()
        .set_indexed()
        .set_stored();

    b.add_f64_field("paid_total", f64o);
    b.add_i64_field("claims_total", i64o.clone());
    b.add_i64_field("bene_total", i64o);

    b.build()
}

fn hcpcs_fields(schema: &Schema) -> anyhow::Result<HcpcsFields> {
    Ok(HcpcsFields {
        hcpcs_code: schema.get_field("hcpcs_code")?,
        short_desc: schema.get_field("short_desc")?,
        long_desc: schema.get_field("long_desc")?,
        paid_total: schema.get_field("paid_total")?,
        claims_total: schema.get_field("claims_total")?,
        bene_total: schema.get_field("bene_total")?,
    })
}
