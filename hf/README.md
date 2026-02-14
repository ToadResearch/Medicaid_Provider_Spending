---
pretty_name: Medicaid Provider Spending (Enriched)
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
- config_name: default
  data_files:
  - split: provider
    path: data/provider.parquet
  - split: npi_api_reference
    path: data/npi_api_reference.parquet
  - split: hcpcs_api_reference
    path: data/hcpcs_api_reference.parquet
---

# Medicaid Provider Spending (Enriched)

This dataset packages the public Medicaid Provider Spending dataset and derived enrichment artifacts produced by this repository.

## Data Splits

| split | contents |
| --- | --- |
| `provider` | The main provider spending dataset (enriched). Contains all source rows; rows with unresolved identifiers keep original IDs and have null enrichment fields. |
| `npi_api_reference` | One row per NPI lookup request (includes request parameters, HTTP status, errors, and raw JSON payloads). |
| `hcpcs_api_reference` | One row per HCPCS lookup request (includes request parameters, HTTP status, errors, and raw JSON payloads). |

## Enrichment Summary

- NPI enrichment is primarily sourced from local CMS NPPES dissemination files when available, with API fallback for missing NPIs.
- HCPCS enrichment uses a date-aware lookup keyed by `CLAIM_FROM_MONTH` and prefers non-NOC records when multiple records match.

## Source Data

- Medicaid Provider Spending (public dataset published by HHS/Open Data).
- CMS NPPES (bulk dissemination + registry API) for NPI metadata.
- Clinical Tables HCPCS API for code metadata.

## Usage

```python
from datasets import load_dataset

ds = load_dataset("YOUR_ORG_OR_USER/YOUR_DATASET_REPO")

provider = ds["provider"]
npi_ref = ds["npi_api_reference"]
hcpcs_ref = ds["hcpcs_api_reference"]
```

## Notes / Caveats

- This repository produces derived artifacts; this dataset is not an official publication by CMS/HHS.
- Enrichment quality depends on upstream reference data and identifier cleanliness; see `provider` split for null enrichment fields where resolution was not possible.

