#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  download.sh [options] [-- <args forwarded to enrich_with_rust.sh>]

Options:
  --url <dataset-url>   Source URL to download
  --data-dir <path>     Data directory (default: ./data)
  --run-rust            Run Rust pipeline after download/decompress
  -h, --help            Show this help

Examples:
  ./download.sh
  ./download.sh --run-rust
  ./download.sh --run-rust -- --build-map-only --reset-map
EOF
}

DATASET_URL="https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
RUN_RUST=0
RUST_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      DATASET_URL="${2:-}"
      shift 2
      ;;
    --data-dir)
      DATA_DIR="${2:-}"
      shift 2
      ;;
    --run-rust)
      RUN_RUST=1
      shift
      ;;
    --)
      shift
      RUST_ARGS=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

mkdir -p "${DATA_DIR}"

DOWNLOAD_PATH="${DATA_DIR}/$(basename "${DATASET_URL}")"
echo "Downloading dataset to ${DOWNLOAD_PATH}"
curl -L --fail --retry 3 "${DATASET_URL}" -o "${DOWNLOAD_PATH}"

FINAL_PATH="${DOWNLOAD_PATH}"
case "${DOWNLOAD_PATH}" in
  *.zip)
    EXTRACT_DIR="${DATA_DIR}/extracted"
    mkdir -p "${EXTRACT_DIR}"
    echo "Unzipping archive..."
    unzip -o "${DOWNLOAD_PATH}" -d "${EXTRACT_DIR}" >/dev/null
    FINAL_PATH="$(python3 - <<'PY' "${EXTRACT_DIR}"
import pathlib
import sys

extract_dir = pathlib.Path(sys.argv[1])
parquet_files = sorted(extract_dir.glob("*.parquet"))
if not parquet_files:
    raise SystemExit(1)
print(parquet_files[0])
PY
)"
    if [[ -z "${FINAL_PATH}" ]]; then
      echo "No parquet file found after unzip." >&2
      exit 1
    fi
    ;;
  *.gz)
    FINAL_PATH="${DOWNLOAD_PATH%.gz}"
    echo "Decompressing gzip to ${FINAL_PATH}"
    gzip -dc "${DOWNLOAD_PATH}" > "${FINAL_PATH}"
    ;;
  *.parquet)
    echo "Downloaded parquet directly; no decompression needed."
    ;;
  *)
    echo "Unknown extension; using downloaded file directly."
    ;;
esac

echo "Ready file: ${FINAL_PATH}"

if [[ "${RUN_RUST}" -eq 1 ]]; then
  ENRICH_SCRIPT="${SCRIPT_DIR}/enrich_with_rust.sh"
  if [[ ! -x "${ENRICH_SCRIPT}" ]]; then
    echo "Rust wrapper script not executable: ${ENRICH_SCRIPT}" >&2
    exit 1
  fi

  echo "Running Rust pipeline..."
  "${ENRICH_SCRIPT}" --input-path "${FINAL_PATH}" "${RUST_ARGS[@]}"
fi
