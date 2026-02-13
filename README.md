# Medicaid Provider Spending Pipeline

## Data source

- Source dataset: [Medicaid Provider Spending | Medicaid Open Data Portal](https://opendata.hhs.gov/datasets/medicaid-provider-spending/)

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

## API references (rate-limit guidance)

- NPPES registry homepage notice about hourly query limits:
  - `https://npiregistry.cms.hhs.gov/`
- NPPES API documentation:
  - `https://npiregistry.cms.hhs.gov/api-page`
- HCPCS API documentation (Clinical Tables):
  - `https://clinicaltables.nlm.nih.gov/apidoc/hcpcs/v3/doc.html`
- Clinical Tables FAQ (rate limits and usage guidance):
  - `https://clinicaltables.nlm.nih.gov/faq.html`

The API does not publish a numeric per-hour cap, so defaults are conservative:
- `--requests-per-second 2`
- `--concurrency 2`

For large bulk workloads, CMS recommends DDS files instead of high-volume NPI API querying.

## 1) Download dataset to `data/`

```bash
./download.sh
```

Default URL:
- `https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet`

`download.sh` will:
- create `data/` if missing
- download the file into `data/`
- decompress if the file is `.zip` or `.gz`
- optionally run Rust pipeline if `--run-rust` is passed

## 2) Build mapping files (resumable, cached)

```bash
./enrich_with_rust.sh \
  --build-map-only
```

Default outputs:
- NPI mapping CSV: `data/npi_provider_mapping.csv`
- NPI lookup cache DB: `data/npi_provider_cache.sqlite`
- HCPCS mapping CSV: `data/hcpcs_code_mapping.csv`
- HCPCS lookup cache DB: `data/hcpcs_code_cache.sqlite`

Behavior:
- step is skipped if mapping file already exists
- use `--rebuild-map` to rebuild using existing cache/API
- use `--reset-map` to delete mappings + caches and start fresh
- interrupted runs resume from cache state
- terminal shows a progress bar during API lookups

## 3) Enrich dataset with mapped provider names

```bash
./enrich_with_rust.sh
```

Adds two columns to each row:
- `BILLING_PROVIDER`
- `SERVICING_PROVIDER`

Adds HCPCS enrichment columns:
- `HCPCS_SHORT_DESC`
- `HCPCS_LONG_DESC`
- `HCPCS_ADD_DATE`
- `HCPCS_ACT_EFF_DATE`
- `HCPCS_TERM_DATE`
- `HCPCS_OBSOLETE`
- `HCPCS_IS_NOC`

HCPCS temporal logic:
- enrichment uses `CLAIM_FROM_MONTH` to select HCPCS metadata valid at the claim month
- matching requires `claim_date >= COALESCE(act_eff_date, add_date)` and `claim_date <= term_date` when termination exists
- if no temporally valid row exists, enrichment falls back to the best available record for that code

NOC/non-NOC selection policy:
- HCPCS `is_noc=true` means “Not Otherwise Classified” (catch-all entries)
- when code metadata is ambiguous, the pipeline prefers non-NOC rows (`is_noc=false`) because they are more specific to the billed service
- if only NOC rows are available for a code/date, the pipeline uses NOC so coverage is not lost

Default output:
- `data/medicaid-provider-spending-enriched.parquet`

## 4) One-command download + Rust pipeline

```bash
./download.sh --run-rust
```

Forward extra args to Rust after `--`:

```bash
./download.sh --run-rust -- \
  --reset-map \
  --requests-per-second 2 \
  --concurrency 2
```

## 5) Optional Hugging Face upload

Upload is opt-in in Rust and only happens when upload flags are provided:

```bash
./enrich_with_rust.sh \
  --hf-token "hf_..." \
  --hf-repo-id "MedicaidProviderSpending" \
  --hf-upload-mapping \
  --hf-upload-hcpcs-mapping \
  --hf-upload-enriched
```

You can still use the standalone upload helper:

```bash
./upload_medicaid_to_hf.sh \
  --token "hf_..." \
  --repo-id "MedicaidProviderSpending" \
  --file "./data/medicaid-provider-spending-enriched.parquet"
```
