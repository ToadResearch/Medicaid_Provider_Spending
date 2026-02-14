# Parquet Null / Empty-List Audit

- Generated: 2026-02-14 19:31:16 UTC
- NPI parquet: `/Users/mkieffer/programming/ToadResearch/medicaid_provider_spending/data/output/npi.parquet`
- HCPCS parquet: `/Users/mkieffer/programming/ToadResearch/medicaid_provider_spending/data/output/hcpcs.parquet`

Notes:
- `null_count` counts actual Parquet nulls.
- `empty_list_count` counts the literal string value `"[]"` (JSON-encoded empty list).

## NPI (`data/output/npi.parquet`)

| column             | rows_total | null_count | null_pct | empty_list_count | empty_list_pct |
| ------------------ | ---------- | ---------- | -------- | ---------------- | -------------- |
| other_names        | 24274      | 24271      | 99.99%   | 3                | 0.01%          |
| practice_locations | 24274      | 24271      | 99.99%   | 3                | 0.01%          |
| endpoints          | 24274      | 24271      | 99.99%   | 2                | 0.01%          |
| identifiers        | 24274      | 24271      | 99.99%   | 2                | 0.01%          |
| addresses          | 24274      | 24271      | 99.99%   | 0                | 0.00%          |
| basic              | 24274      | 24271      | 99.99%   | 0                | 0.00%          |
| taxonomies         | 24274      | 24271      | 99.99%   | 0                | 0.00%          |
| results            | 24274      | 16510      | 68.02%   | 7761             | 31.97%         |
| response_json      | 24274      | 15012      | 61.84%   | 0                | 0.00%          |
| error_message      | 24274      | 9262       | 38.16%   | 0                | 0.00%          |
| api_run_id         | 24274      | 0          | 0.00%    | 0                | 0.00%          |
| npi                | 24274      | 0          | 0.00%    | 0                | 0.00%          |
| request_params     | 24274      | 0          | 0.00%    | 0                | 0.00%          |
| requested_at_utc   | 24274      | 0          | 0.00%    | 0                | 0.00%          |
| url                | 24274      | 0          | 0.00%    | 0                | 0.00%          |

## HCPCS (`data/output/hcpcs.parquet`)

| column                | rows_total | null_count | null_pct | empty_list_count | empty_list_pct |
| --------------------- | ---------- | ---------- | -------- | ---------------- | -------------- |
| error_message         | 8280       | 8278       | 99.98%   | 0                | 0.00%          |
| ef_act_eff_dt         | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| ef_add_dt             | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| ef_is_noc             | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| ef_long_desc          | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| ef_obsolete           | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| ef_short_desc         | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| ef_term_dt            | 8280       | 183        | 2.21%    | 4427             | 53.47%         |
| response_codes        | 8280       | 2          | 0.02%    | 4608             | 55.65%         |
| response_display      | 8280       | 2          | 0.02%    | 4608             | 55.65%         |
| response_extra_fields | 8280       | 2          | 0.02%    | 0                | 0.00%          |
| response_json         | 8280       | 2          | 0.02%    | 0                | 0.00%          |
| api_run_id            | 8280       | 0          | 0.00%    | 0                | 0.00%          |
| hcpcs_code            | 8280       | 0          | 0.00%    | 0                | 0.00%          |
| request_params        | 8280       | 0          | 0.00%    | 0                | 0.00%          |
| requested_at_utc      | 8280       | 0          | 0.00%    | 0                | 0.00%          |
| url                   | 8280       | 0          | 0.00%    | 0                | 0.00%          |

