# Parquet Null / Empty-List Audit

- Generated at (unix seconds): 1771173117
- NPI parquet: `/Users/mkieffer/programming/ToadResearch/medicaid_provider_spending/data/output/npi.parquet`
- HCPCS parquet: `/Users/mkieffer/programming/ToadResearch/medicaid_provider_spending/data/output/hcpcs.parquet`

Notes:
- `null_count` counts actual Parquet nulls.
- `empty_list_count` counts the literal string value `"[]"` (JSON-encoded empty list).

## NPI (`data/output/npi.parquet`)

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

## HCPCS (`data/output/hcpcs.parquet`)

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
