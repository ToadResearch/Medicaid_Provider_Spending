# AGENTS

Guidance for coding agents working in this repository.

## Project layout

- Rust pipeline crate: `build_datasets/`
- Wrapper script (preferred entrypoint): `build_datasets.sh`
- Download/orchestration script: `download.sh`
- Optional HF upload helper: `upload_medicaid_to_hf.sh`
- Main docs: `README.md`
- Data folders:
  - `data/manually_cleaned/`
  - `data/raw/medicaid/`
  - `data/raw/nppes/monthly/`
  - `data/raw/nppes/weekly/`
  - `data/raw/cpt/`
  - `data/cache/{npi,hcpcs}/`
  - `data/mappings/{npi,hcpcs}/`
  - `data/reference/{npi,hcpcs}/`
  - `data/output/`

## Primary workflow

1. Download dataset:
   - `./download.sh`
2. Build mappings only (optional):
   - `./build_datasets.sh --build-map-only`
3. Full enrichment:
   - `./build_datasets.sh`

## Mapping/cache behavior

- NPI and HCPCS mapping outputs are cached on disk under `data/cache/` and `data/mappings/`.
- API reference datasets are exported under `data/reference/`.
- Unresolved identifiers report is exported at end of run to `data/unresolved_identifiers.csv` (override with `--unresolved-report-csv`).
- If mapping/reference outputs exist and cache coverage is complete for the current input, default runs do not rebuild them.
- If enriched output already exists and mappings were unchanged in that run, default runs do not rebuild enrichment output.
- Rebuild mappings with:
  - `--rebuild-map`
- Reset and rebuild with:
  - `--reset-map`
- NPI and HCPCS map-building run in parallel when both need rebuilding.
- NPI mapping preloads from the newest local NPPES monthly and weekly CSV before API fallback.
- HCPCS lookups use batched OR queries by default; tune with `--hcpcs-batch-size`.
- If an HCPCS batch request fails, resolver falls back to single-code requests for that batch.
- `download.sh` normalizes trailing-space CPT directory names and applies manual overlays from `data/manually_cleaned/PFREV26AR_nonQP` into extracted CPT folders before building fallback CSV.
- Local CPT/HCPCS fallback rows can be loaded from `data/raw/cpt/cpt_hcpcs_fallback.csv` (or `--hcpcs-fallback-csv`) before API lookup.
- HCPCS API `not_found` results should still check local fallback rows before persisting `not_found`.
- API request failures are retried again in deferred rounds within the same run.
- Tune deferred retries with `--failure-retry-rounds` and `--failure-retry-delay-seconds`.
- On first retry-eligible request error in a round, stop scheduling new requests immediately, drain in-flight work, then begin retry cooldown.
- Ctrl-C triggers graceful shutdown: complete in-flight work, save cache/maps, then exit.
- `build_datasets.sh --log-file` should keep progress bars visible in interactive terminals.
- During retry cooldowns, progress bars temporarily switch to a seconds countdown before resuming lookup progress.
- Reference dataset column order should stay human-readable: primary business fields first, metadata/raw JSON fields last.

## Data semantics to preserve

- HCPCS enrichment is date-aware using `CLAIM_FROM_MONTH`.
- Prefer non-NOC HCPCS records when multiple records match.
- If only NOC is available for a code/date, use NOC as fallback.
- For unresolved NPI/HCPCS codes, keep source identifiers and leave enrichment fields null; use unresolved report for manual follow-up.
- The enriched upload (`--hf-upload-enriched`) must include all source rows, including unresolved rows with null enrichment fields.

## Development expectations

- Keep changes focused; avoid unrelated refactors.
- Preserve current CLI flags and defaults unless explicitly requested.
- After Rust changes, run:
  - `cargo fmt`
  - `cargo check`
- Use `README.md` as the user-facing source of truth; update it when behavior changes.

## Performance and API safety

- Respect API throttling controls:
  - `--requests-per-second`
  - `--concurrency`
  - `--failure-retry-rounds`
  - `--failure-retry-delay-seconds`
- HCPCS batching should stay below API payload/query-size risk:
  - keep `count=500` max for HCPCS requests
  - prefer moderate `--hcpcs-batch-size` values (default `100`)
- Do not remove retry/backoff behavior for transient API failures.
- Prefer local NPPES data dissemination files for bulk NPI resolution whenever available.

