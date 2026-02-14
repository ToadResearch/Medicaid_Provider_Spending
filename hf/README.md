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

| split | contents |
| --- | --- |
| `spending` | Raw Medicaid Provider Spending parquet (as downloaded; unmodified) from [HHS Open Data](https://opendata.hhs.gov/datasets/medicaid-provider-spending/). |
| `npi` | **Derived** NPI resolution artifacts: one row per *unique NPI that was looked up via the CMS NPI Registry API* (deduped). Includes request URL/params, error message, and raw JSON payload. (Many NPIs are resolved from bulk NPPES files instead; those successful bulk matches will not necessarily appear in this API-response split.) |
| `hcpcs` | **Derived** HCPCS/CPT resolution artifacts: one row per *unique code that was looked up via the Clinical Tables HCPCS API* (deduped). Includes request URL/params, error message, and raw JSON payload. (Some codes may be resolved from local CPT/HCPCS fallback data instead; those successful fallback matches will not necessarily appear in this API-response split.) |


## Usage

```python
from datasets import load_dataset

ds = load_dataset("mkieffer/Medicaid-Provider-Spending")

spending = ds["spending"]
npi = ds["npi"]
hcpcs = ds["hcpcs"]
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
| `npi` | string | NPI looked up |
| `basic` | string (JSON) | `results[0].basic` |
| `addresses` | string (JSON) | `results[0].addresses` |
| `practice_locations` | string (JSON) | `results[0].practiceLocations` |
| `taxonomies` | string (JSON) | `results[0].taxonomies` |
| `identifiers` | string (JSON) | `results[0].identifiers` |
| `other_names` | string (JSON) | `results[0].other_names` |
| `endpoints` | string (JSON) | `results[0].endpoints` |
| `url` | string | Request URL |
| `error_message` | string (nullable) | Error message if lookup failed |
| `api_run_id` | string | Local pipeline run id |
| `requested_at_utc` | string | Request timestamp (UTC) |
| `request_params` | string (JSON) | Request params captured by the pipeline |
| `results` | string (JSON) | Full `results` array |
| `response_json` | string (JSON) | Full raw API JSON payload |

### `hcpcs`

All columns in this split are `string` (some are JSON-encoded strings).

| column | type | description |
| --- | --- | --- |
| `hcpcs_code` | string | HCPCS/CPT code looked up |
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
| `url` | string | Request URL |
| `error_message` | string (nullable) | Error message if lookup failed |
| `api_run_id` | string | Local pipeline run id |
| `requested_at_utc` | string | Request timestamp (UTC) |
| `request_params` | string (JSON) | Request params captured by the pipeline |
| `response_json` | string (JSON) | Full raw API JSON payload |


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
