---
pretty_name: Medicaid Provider Spending
language:
- en
license: other
tags:
- medicaid
- healthcare
- claims
- providers
- npi
- hcpcs
configs:
- config_name: spending
  default: true
  data_files:
  - split: spending
    path: data/spending.parquet
- config_name: npi
  data_files:
  - split: npi
    path: data/npi.parquet
- config_name: hcpcs
  data_files:
  - split: hcpcs
    path: data/hcpcs.parquet
---

# Medicaid Provider Spending

This dataset packages:

- the raw **Medicaid Provider Spending** dataset (as published on HHS Open Data), and
- the **derived identifier-resolution artifacts** produced by this repository when extracting and resolving the unique NPIs and HCPCS/CPT codes found in the raw spending data.

## Source Data

- Medicaid Provider Spending (public dataset published by HHS/Open Data): [opendata.hhs.gov/datasets/medicaid-provider-spending](https://opendata.hhs.gov/datasets/medicaid-provider-spending/)
- CMS NPPES bulk dissemination files + CMS NPI Registry API (for NPI/provider metadata)
- NLM Clinical Tables HCPCS API (for HCPCS/CPT code metadata)



## Data Splits

These are exposed on Hugging Face as **three dataset configurations** (`spending`, `npi`, `hcpcs`)
because each has a different schema (the Hub viewer and `datasets` expect a consistent schema within a single config).

| split | contents |
| --- | --- |
| `spending` | Raw Medicaid Provider Spending parquet (as downloaded; unmodified) from [HHS Open Data](https://opendata.hhs.gov/datasets/medicaid-provider-spending/). |
| `npi` | **Derived** NPI resolution artifacts: one row per *unique NPI in the spending dataset* (deduped). Populated primarily from bulk NPPES dissemination files; the CMS NPI Registry API is used only as fallback for bulk misses. Includes provenance (URL/params), error message, and a raw-or-synthetic JSON payload matching the NPI Registry API response shape. |
| `hcpcs` | **Derived** HCPCS/CPT resolution artifacts: one row per *unique code in the spending dataset* (deduped). Populated from the resolved mapping cache (Clinical Tables API and/or local CPT/HCPCS fallback). Includes provenance (URL/params), error message, and a raw-or-synthetic JSON payload matching the Clinical Tables response shape. |


## Usage

```python
from datasets import load_dataset

spending = load_dataset("mkieffer/Medicaid-Provider-Spending", "spending")["spending"]
npi = load_dataset("mkieffer/Medicaid-Provider-Spending", "npi")["npi"]
hcpcs = load_dataset("mkieffer/Medicaid-Provider-Spending", "hcpcs")["hcpcs"]
```

## Split Schemas

### `spending`

| column | type | description |
| --- | --- | --- |
| `BILLING_PROVIDER_NPI_NUM` | string | Billing provider NPI |
| `SERVICING_PROVIDER_NPI_NUM` | string | Servicing provider NPI |
| `HCPCS_CODE` | string | HCPCS/CPT code |
| `CLAIM_FROM_MONTH` | string | Claim month (`YYYY-MM-01`) |
| `TOTAL_UNIQUE_BENEFICIARIES` | int64 | Unique beneficiaries |
| `TOTAL_CLAIMS` | int64 | Total claims |
| `TOTAL_PAID` | float64 | Total paid (USD) |

### `npi`

All columns in this split are `string` (some are JSON-encoded strings).

| column | type | description |
| --- | --- | --- |
| `npi` | string | NPI |
| `basic` | string (JSON) | `results[0].basic` |
| `addresses` | string (JSON) | `results[0].addresses` |
| `practice_locations` | string (JSON) | `results[0].practiceLocations` |
| `taxonomies` | string (JSON) | `results[0].taxonomies` |
| `identifiers` | string (JSON) | `results[0].identifiers` |
| `other_names` | string (JSON) | `results[0].other_names` |
| `endpoints` | string (JSON) | `results[0].endpoints` |
| `url` | string | Request URL when an API call happened; otherwise a bulk sentinel like `nppes_bulk:*` |
| `error_message` | string (nullable) | Error message if lookup failed |
| `api_run_id` | string | Local pipeline run id |
| `requested_at_utc` | string | Request timestamp (UTC) or bulk-export generation timestamp |
| `request_params` | string (JSON) | Request params / provenance captured by the pipeline |
| `results` | string (JSON) | Full `results` array |
| `response_json` | string (JSON) | Full raw API JSON payload when an API call happened; otherwise a synthetic payload in the same shape |

### `hcpcs`

All columns in this split are `string` (some are JSON-encoded strings).

| column | type | description |
| --- | --- | --- |
| `hcpcs_code` | string | HCPCS/CPT code |
| `ef_short_desc` | string (JSON) | `extra_fields.short_desc` (list) |
| `ef_long_desc` | string (JSON) | `extra_fields.long_desc` (list) |
| `ef_add_dt` | string (JSON) | `extra_fields.add_dt` (list) |
| `ef_act_eff_dt` | string (JSON) | `extra_fields.act_eff_dt` (list) |
| `ef_term_dt` | string (JSON) | `extra_fields.term_dt` (list) |
| `ef_obsolete` | string (JSON) | `extra_fields.obsolete` (list) |
| `ef_is_noc` | string (JSON) | `extra_fields.is_noc` (list) |
| `response_codes` | string (JSON) | Codes array returned by the API |
| `response_display` | string (JSON) | Display array returned by the API |
| `response_extra_fields` | string (JSON) | Extra fields object returned by the API |
| `url` | string | Request URL when an API call happened; otherwise a cache/fallback sentinel like `hcpcs_cache:*` |
| `error_message` | string (nullable) | Error message if lookup failed |
| `api_run_id` | string | Local pipeline run id |
| `requested_at_utc` | string | Request timestamp (UTC) or cache-export generation timestamp |
| `request_params` | string (JSON) | Request params / provenance captured by the pipeline |
| `response_json` | string (JSON) | Full raw API JSON payload when an API call happened; otherwise a synthetic payload in the same shape |


## Parquet Null / Empty-List Audit

<!-- BEGIN PARQUET_NULL_AUDIT -->
_Auto-generated by `./build_datasets.sh` (or `cargo run --release --manifest-path build_datasets/Cargo.toml -- --null-check`)._

- Generated at (unix seconds): 1771173117
- NPI parquet: `data/output/npi.parquet`
- HCPCS parquet: `data/output/hcpcs.parquet`

Notes:
- `null_count` counts actual Parquet nulls.
- `empty_list_count` counts the literal string value `"[]"` (JSON-encoded empty list).

### NPI (`data/output/npi.parquet`)

| column | rows_total | null_count | null_pct | empty_list_count | empty_list_pct |
| --- | ---: | ---: | ---: | ---: | ---: |
| error_message | 1802136 | 1799569 | 99.86% | 0 | 0.00% |
| other_names | 1802136 | 4176 | 0.23% | 1657485 | 91.97% |
| endpoints | 1802136 | 4176 | 0.23% | 1591727 | 88.32% |
| practice_locations | 1802136 | 4176 | 0.23% | 1480277 | 82.14% |
| identifiers | 1802136 | 4176 | 0.23% | 1208848 | 67.08% |
| addresses | 1802136 | 4176 | 0.23% | 20097 | 1.12% |
| taxonomies | 1802136 | 4176 | 0.23% | 20097 | 1.12% |
| basic | 1802136 | 4176 | 0.23% | 0 | 0.00% |
| results | 1802136 | 4065 | 0.23% | 111 | 0.01% |
| response_json | 1802136 | 2567 | 0.14% | 0 | 0.00% |
| api_run_id | 1802136 | 0 | 0.00% | 0 | 0.00% |
| npi | 1802136 | 0 | 0.00% | 0 | 0.00% |
| request_params | 1802136 | 0 | 0.00% | 0 | 0.00% |
| requested_at_utc | 1802136 | 0 | 0.00% | 0 | 0.00% |
| url | 1802136 | 0 | 0.00% | 0 | 0.00% |

### HCPCS (`data/output/hcpcs.parquet`)

| column | rows_total | null_count | null_pct | empty_list_count | empty_list_pct |
| --- | ---: | ---: | ---: | ---: | ---: |
| error_message | 10881 | 6271 | 57.63% | 0 | 0.00% |
| ef_act_eff_dt | 10881 | 0 | 0.00% | 4610 | 42.37% |
| ef_add_dt | 10881 | 0 | 0.00% | 4610 | 42.37% |
| ef_is_noc | 10881 | 0 | 0.00% | 4610 | 42.37% |
| ef_long_desc | 10881 | 0 | 0.00% | 4610 | 42.37% |
| ef_obsolete | 10881 | 0 | 0.00% | 4610 | 42.37% |
| ef_short_desc | 10881 | 0 | 0.00% | 4610 | 42.37% |
| ef_term_dt | 10881 | 0 | 0.00% | 4610 | 42.37% |
| response_codes | 10881 | 0 | 0.00% | 4610 | 42.37% |
| response_display | 10881 | 0 | 0.00% | 4610 | 42.37% |
| api_run_id | 10881 | 0 | 0.00% | 0 | 0.00% |
| hcpcs_code | 10881 | 0 | 0.00% | 0 | 0.00% |
| request_params | 10881 | 0 | 0.00% | 0 | 0.00% |
| requested_at_utc | 10881 | 0 | 0.00% | 0 | 0.00% |
| response_extra_fields | 10881 | 0 | 0.00% | 0 | 0.00% |
| response_json | 10881 | 0 | 0.00% | 0 | 0.00% |
| url | 10881 | 0 | 0.00% | 0 | 0.00% |
<!-- END PARQUET_NULL_AUDIT -->

## Unmapped / Unresolved Identifier Counts

For the build that produced this dataset:

- **NPI**: 24,272 unique NPIs were not successfully mapped/resolved (15,012 request errors; 9,260 `not_found`).
  - Many of these were invalid NPI identifiers (4,111 unique): 2,928 non-numeric, 971 wrong-length, 205 checksum-invalid (Luhn), and 7 placeholders.
  - Some NPIs have been **inactive/deactivated** in [NPPES/NPI Registry](https://npiregistry.cms.hhs.gov/search) (status can change over time); when present, check the registry payload for an active/inactive status (and/or NPPES deactivation fields) before treating an NPI as current.
  - Full format breakdown from triage:

    | inferred type | count | % | meaning |
    | --- | ---: | ---: | --- |
    | `npi_luhn_valid` | 20,161 | 83.06% | looks like a well-formed NPI (10 digits + valid check digit) |
    | `non_numeric` | 2,928 | 12.06% | contains non-digits (often needs upstream cleaning) |
    | `numeric_wrong_len` | 971 | 4.00% | all digits but not 10 digits long |
    | `npi_luhn_invalid` | 205 | 0.84% | 10 digits, but invalid Luhn check digit |
    | `placeholder_or_invalid` | 7 | 0.03% | placeholder-like values (e.g., all zeros) |
- **HCPCS/CPT**: 4,610 unique codes were not successfully mapped/resolved (4,608 `not_found`; 2 request errors).
  - Full resolved/unresolved pattern breakdown from triage:

    | pattern family | count | % |
    | --- | ---: | ---: |
    | `CPT_or_HCPCS_L1_5digit` (5-digit CPT/HCPCS) | 2,169 | 47.05% |
    | `HCPCS_level_II` (`A0000` style) | 474 | 10.28% |
    | `CDT` (`D####`) | 449 | 9.74% |
    | `CPT_category_II` (`####F`) | 358 | 7.77% |
    | `alphanum_5char_unknown` | 310 | 6.72% |
    | `revenue_code_4digit` | 272 | 5.90% |
    | `4digit_plus_letter_other` | 124 | 2.69% |
    | `CPT_PLA` (`####U`) | 110 | 2.39% |
    | `CPT_category_III` (`####T`) | 96 | 2.08% |
    | `word_or_flag` | 78 | 1.69% |
    | `unknown` | 57 | 1.24% |
    | `HCPCS_L2_plus_modifier` | 39 | 0.85% |
    | `CPT_5digit_plus_modifier` | 24 | 0.52% |
    | `drg_like_3digit` | 15 | 0.33% |
    | `icd10pcs_like_7char` | 11 | 0.24% |
    | `modifier_2char` | 7 | 0.15% |
    | `numeric_6to8_unknown` | 7 | 0.15% |
    | `placeholder_or_invalid` | 6 | 0.13% |
    | `CDT_plus_suffix` | 4 | 0.09% |
  - 458 unique codes were flagged by triage as "needs review" (e.g., flags/words, placeholders, or unknown alphanumeric patterns).
  - The 2 request errors are included in `unknown` (for example `H)015`, `NT/WKN`).
