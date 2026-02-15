# Medicaid Provider Spending Pipeline

## Data source

- Source dataset: [HHS Medicaid Provider Spending](https://opendata.hhs.gov/datasets/medicaid-provider-spending/)

## Dataset schema

| Name | Type | Description |
| --- | --- | --- |
| `BILLING_PROVIDER_NPI_NUM` | string | National Provider Identifier of the billing provider |
| `SERVICING_PROVIDER_NPI_NUM` | string | National Provider Identifier of the servicing provider |
| `HCPCS_CODE` | string | Healthcare Common Procedure Coding System code for the service |
| `CLAIM_FROM_MONTH` | date | Month for which claims are aggregated (`YYYY-MM-01` format) |
| `TOTAL_UNIQUE_BENEFICIARIES` | integer | Count of unique beneficiaries for this provider/procedure/month |
| `TOTAL_CLAIMS` | integer | Total number of claims for this provider/procedure/month |
| `TOTAL_PAID` | float | Total amount paid by Medicaid (in USD) |

## Data directory layout

```text
data/
  unresolved_identifiers.csv
  README.md                  # detailed provenance and usage notes for data folders
  raw/
    medicaid/                 # downloaded source dataset
    nppes/
      monthly/                # extracted monthly NPPES bundle(s)
      weekly/                 # extracted weekly incremental NPPES bundle(s)
    cpt/
      archives/               # optional CPT/HCPCS source zips downloaded via --cpt-zip-url
      cpt_hcpcs_fallback.csv  # derived local fallback used for unresolved HCPCS/CPT lookups
  manually_cleaned/           # version-controlled manual CSV/MD corrections used as overlays
  cache/
    npi/npi_provider_cache.sqlite
    hcpcs/hcpcs_code_cache.sqlite
  mappings/
    npi/npi_provider_mapping.csv
    hcpcs/hcpcs_code_mapping.csv
  output/
    npi.parquet
    hcpcs.parquet
```

## 1) Download raw data files

```bash
./download.sh
```

Default Medicaid dataset URL:
- `https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet`

`download.sh` will:
- create organized subfolders under `data/`
- download the Medicaid source file into `data/raw/medicaid/`
- download latest NPPES **monthly V2** and **weekly V2** bundles into `data/raw/nppes/`
- extract NPPES zips and keep the raw bundles
- recursively extract any zip archives under `data/raw/cpt/` (including nested zips)
- normalize CPT extracted folder names when source archives include trailing spaces
- apply manual overlays from `data/manually_cleaned/PFREV26AR_nonQP/{csv,md}` into extracted CPT folders
- build `data/raw/cpt/cpt_hcpcs_fallback.csv` from extracted CPT/HCPCS files when parsable
- reuse existing downloaded archives/files when present (downloads only missing/empty targets)
- optionally run Rust pipeline if `--run-rust` is passed

Useful options:
- `--skip-nppes` (skip monthly/weekly NPPES downloads)
- `--nppes-monthly-url <url>` / `--nppes-weekly-url <url>` (pin exact bundle URLs)
- `--cpt-index-url <url>` (override CPT page used for fallback zip auto-discovery)
- `--cpt-zip-url <url>` (repeatable; downloads CPT/HCPCS source archives into `data/raw/cpt/archives/`)

If `data/raw/cpt/` has no local CPT/HCPCS source data and no `--cpt-zip-url` is provided,
`download.sh` auto-discovers a default CMS CPT archive URL from the configured `--cpt-index-url`.

## 2) Build mappings + resolved identifier datasets (resumable, cached)

```bash
./build_datasets.sh
```

Default outputs:
- NPI mapping CSV: `data/mappings/npi/npi_provider_mapping.csv`
- NPI lookup cache DB: `data/cache/npi/npi_provider_cache.sqlite`
- HCPCS mapping CSV: `data/mappings/hcpcs/hcpcs_code_mapping.csv`
- HCPCS lookup cache DB: `data/cache/hcpcs/hcpcs_code_cache.sqlite`
- NPI resolved identifier dataset (bulk+API): `data/output/npi.parquet`
- HCPCS resolved identifier dataset (cache+fallback+API): `data/output/hcpcs.parquet`
- Unresolved identifiers report: `data/unresolved_identifiers.csv`

Behavior:
- each dataset build step is skipped only when outputs exist and cache coverage is complete (no unresolved IDs/codes for the current input)
- use `--rebuild-map` to rebuild using existing cache/API
- use `--reset-map` to delete mappings + caches and start fresh
- interrupted runs resume from cache state
- NPI mapping preloads from the newest local NPPES monthly + weekly CSV first, then uses API only for unresolved NPIs
- pass `--skip-nppes-bulk` to disable local NPPES preload
- HCPCS API lookups are batched with OR queries (`q=code:(... OR ...)`) to reduce request count
- tune HCPCS batch size with `--hcpcs-batch-size` (default `100`)
- on HCPCS batch request failure, the pipeline falls back to single-code HCPCS requests for that batch
- local CPT/HCPCS fallback rows are loaded from `data/raw/cpt/cpt_hcpcs_fallback.csv` (or `--hcpcs-fallback-csv`) and seeded into cache before API lookup
- if an HCPCS API lookup is `not_found`, the resolver checks local CPT fallback rows before writing `not_found`
- local fallback logic applies to HCPCS/CPT only; NPI resolution still uses NPPES bulk + NPI API
- NPI + HCPCS map-building runs in parallel, with one live progress bar per API
- progress bars include elapsed time, throughput, and ETA during API lookups
- pressing Ctrl-C triggers a graceful stop: current in-flight work finishes, caches/maps are saved, then process exits
- resolved identifier datasets capture full API payloads when requests occurred, otherwise a synthetic payload derived from bulk/fallback sources (plus URL/params/errors) and are written as deduped one-row-per-identifier tables
- resolved identifier dataset columns are ordered for readability: primary fields first, metadata/raw payload fields last
- end-of-run unresolved report includes unresolved NPIs/HCPCS with status (`not_found`, `error`, `missing_cache`) and last fetch timestamp
- override unresolved report path with `--unresolved-report-csv`

Optional log file output:

```bash
./build_datasets.sh \
  --log-file ./logs/build_map.log
```

Note:
- in an interactive terminal, `--log-file` preserves live progress bars while also writing logs

## API response datasets

Two additional datasets are produced during map building (and are safe to re-export from the cache DB without re-querying the APIs):

- `data/output/npi.parquet`
  - one row per NPI API lookup (deduped)
  - includes nested response structures as JSON-string columns (`addresses`, `taxonomies`, etc.)
  - includes `url` and full payload in `response_json`
  - column order:
    - `npi`
    - `basic`, `addresses`, `practice_locations`, `taxonomies`, `identifiers`, `other_names`, `endpoints`
    - `url`, `error_message`, `api_run_id`, `requested_at_utc`, `request_params`, `results`, `response_json`

- `data/output/hcpcs.parquet`
  - one row per HCPCS API lookup (deduped)
  - includes columns derived from `response_json` plus `url`
  - includes full payload in `response_json`
  - column order:
    - `hcpcs_code`
    - `ef_short_desc`, `ef_long_desc`, `ef_add_dt`, `ef_act_eff_dt`, `ef_term_dt`, `ef_obsolete`, `ef_is_noc`
    - `response_codes`, `response_display`, `response_extra_fields`
    - `url`, `error_message`, `api_run_id`, `requested_at_utc`, `request_params`, `response_json`

Note:
- if `--skip-api` is set, these response datasets will not gain new rows (existing cached rows are still exported)

## 3) One-command download + Rust pipeline

```bash
./download.sh --run-rust
```

Forward extra args to Rust after `--`:

```bash
./download.sh --run-rust -- \
  --reset-map \
  --requests-per-second 2 \
  --concurrency 2 \
  --hcpcs-batch-size 100
```

## 4) Optional Hugging Face upload

Upload is opt-in in Rust and only happens when upload flags are provided:

```bash
./build_datasets.sh \
  --hf-token "hf_..." \
  --hf-repo-id "mkieffer/Medicaid-Provider-Spending" \
  --hf-upload-mapping \
  --hf-upload-hcpcs-mapping \
  --hf-upload-npi \
  --hf-upload-hcpcs
```

Optional destination overrides for API response datasets:
- `--hf-npi-path-in-repo`
- `--hf-hcpcs-path-in-repo`

You can still use the standalone upload helper:

```bash
./upload_medicaid_to_hf.sh \
  --token "hf_..." \
  --repo-id "mkieffer/Medicaid-Provider-Spending" \
  --file "./data/raw/medicaid/medicaid-provider-spending.parquet" \
  --path-in-repo "data/spending.parquet"
```

Or with the split/config dataset upload mode:

```bash
./upload_medicaid_to_hf.sh \
  --token "hf_..." \
  --repo-id "mkieffer/Medicaid-Provider-Spending" \
  --upload-split-dataset
```

To update just the API response splits + dataset card without re-uploading the large spending parquet:

```bash
./upload_medicaid_to_hf.sh \
  --token "hf_..." \
  --repo-id "mkieffer/Medicaid-Provider-Spending" \
  --upload-split-dataset \
  --skip-spending-upload
```

Hugging Face config usage:

```python
from datasets import load_dataset

spending = load_dataset("mkieffer/Medicaid-Provider-Spending", "spending")["spending"]
npi = load_dataset("mkieffer/Medicaid-Provider-Spending", "npi")["npi"]
hcpcs = load_dataset("mkieffer/Medicaid-Provider-Spending", "hcpcs")["hcpcs"]
```

Dataset URL:

- https://huggingface.co/datasets/mkieffer/Medicaid-Provider-Spending

## API docs (rate-limit guidance)

- NPPES registry homepage notice about hourly query limits:
  - `https://npiregistry.cms.hhs.gov/`
- NPPES API documentation:
  - `https://npiregistry.cms.hhs.gov/api-page`
- CMS NPPES Data Dissemination overview:
  - `https://www.cms.gov/medicare/regulations-guidance/administrative-simplification/data-dissemination`
- CMS NPPES downloadable files index:
  - `http://download.cms.gov/nppes/NPI_Files.html`
- HCPCS API documentation (Clinical Tables):
  - `https://clinicaltables.nlm.nih.gov/apidoc/hcpcs/v3/doc.html`
- Clinical Tables FAQ (rate limits and usage guidance):
  - `https://clinicaltables.nlm.nih.gov/faq.html`

The API does not publish a numeric per-hour cap, so defaults are conservative:
- `--requests-per-second 2`
- `--concurrency 2`

HCPCS API batching notes:
- uses `count=500` (the documented maximum per request)
- default `--hcpcs-batch-size` is `100` to keep query URLs manageable while significantly reducing request count

For large bulk workloads, CMS recommends NPPES dissemination files instead of high-volume NPI API querying.
