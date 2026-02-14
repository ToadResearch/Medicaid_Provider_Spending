# Spending Explorer (site/)

This folder contains a self-bootstrapping web app for exploring the Medicaid Provider Spending dataset with fast search, filters, and a ZIP-centroid map.

It is designed to work even if you have **not** run the local data pipeline first: the backend will download the required Parquet files from Hugging Face automatically.
If you *have* already run the pipeline, the backend will automatically reuse local artifacts from this repo’s `data/` folder to avoid re-downloading.

## Layout

- `site/backend/`: Rust backend (Axum + DuckDB + Tantivy)
- `site/frontend/`: Svelte 5 + Tailwind frontend (MapLibre map)

## Backend

### Build (downloads + rollups + indices)

```bash
cd site/backend
cargo run --release -- build
```

This will download (if missing):

- `spending.parquet` from:
  - `https://huggingface.co/datasets/mkieffer/Medicaid-Provider-Spending/resolve/main/data/spending.parquet`
- `npi.parquet` from:
  - `https://huggingface.co/datasets/mkieffer/Medicaid-Provider-Spending/resolve/main/data/npi.parquet`
- `hcpcs.parquet` from:
  - `https://huggingface.co/datasets/mkieffer/Medicaid-Provider-Spending/resolve/main/data/hcpcs.parquet`
- GeoNames ZIP centroids (`US.zip`) from:
  - `https://download.geonames.org/export/zip/US.zip`

If present locally, it will prefer reusing:

- `data/raw/medicaid/medicaid-provider-spending.parquet` (spending)
- `data/output/npi.parquet` (NPI API responses)
- `data/output/hcpcs.parquet` (HCPCS API responses)

Outputs are written under `site/backend/data/`:

- `site.duckdb` (rollups / serving tables)
- `index/providers/` and `index/hcpcs/` (Tantivy search indices)

### Serve API

```bash
cd site/backend
cargo run --release -- serve --host 127.0.0.1 --port 8787
```

API base URL: `http://127.0.0.1:8787`

## Frontend

```bash
cd site/frontend
npm install
npm run dev
```

By default, the frontend expects the backend at `http://127.0.0.1:8787`.

Override with:

```bash
export VITE_API_BASE_URL="http://127.0.0.1:8787"
```

## Geocoding note (important)

The map uses **ZIP centroids** derived from GeoNames. These coordinates are approximate and should not be interpreted as exact provider locations.

GeoNames data source:

- `https://download.geonames.org/export/zip/`
