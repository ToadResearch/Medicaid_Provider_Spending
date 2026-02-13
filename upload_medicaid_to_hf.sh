#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  upload_medicaid_to_hf.sh [args]

Args:
  --token <HF_TOKEN>               Hugging Face token (or set HF_TOKEN)
  --repo-id <repo-id>              Repo id (default: MedicaidProviderSpending)
  --repo-type <dataset|model>      Repo type (default: dataset)
  --path-in-repo <path>            Destination path in repo
  --file <local-path>              Upload a local file directly (skip download)
  --url <dataset-url>              Source file URL
  --tmp-dir <path>                 Temp directory
  -h, --help                       Show this help
EOF
}

DATASET_URL="${HF_DATASET_URL:-https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet}"
WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="${HF_TMP_DIR:-${WORK_DIR}/tmp}"

HF_TOKEN="${HF_TOKEN:-}"
HF_REPO_ID="${HF_REPO_ID:-MedicaidProviderSpending}"
HF_REPO_TYPE="${HF_REPO_TYPE:-dataset}"
HF_PATH_IN_REPO="${HF_PATH_IN_REPO:-medicaid-provider-spending.parquet}"
LOCAL_FILE="${HF_LOCAL_FILE:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)
      HF_TOKEN="${2:-}"
      shift 2
      ;;
    --repo-id)
      HF_REPO_ID="${2:-}"
      shift 2
      ;;
    --repo-type)
      HF_REPO_TYPE="${2:-}"
      shift 2
      ;;
    --path-in-repo)
      HF_PATH_IN_REPO="${2:-}"
      shift 2
      ;;
    --file)
      LOCAL_FILE="${2:-}"
      shift 2
      ;;
    --url)
      DATASET_URL="${2:-}"
      shift 2
      ;;
    --tmp-dir)
      TMP_DIR="${2:-}"
      shift 2
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

: "${HF_TOKEN:?HF_TOKEN is required (use --token or env var)}"

mkdir -p "${TMP_DIR}"

if [[ -n "${LOCAL_FILE}" ]]; then
  if [[ ! -f "${LOCAL_FILE}" ]]; then
    echo "Local file not found: ${LOCAL_FILE}"
    exit 1
  fi
  UPLOAD_FILE="${LOCAL_FILE}"
else
  DOWNLOADED_FILE="${TMP_DIR}/$(basename "${DATASET_URL}")"
  EXTRACT_DIR="${TMP_DIR}/extracted"
  mkdir -p "${EXTRACT_DIR}"

  echo "Downloading dataset..."
  curl -L --fail --retry 3 "${DATASET_URL}" -o "${DOWNLOADED_FILE}"

  # Unzip/decompress if needed. If already a parquet file, use as-is.
  UPLOAD_FILE="${DOWNLOADED_FILE}"
  case "${DOWNLOADED_FILE}" in
    *.zip)
      echo "Unzipping zip archive..."
      unzip -o "${DOWNLOADED_FILE}" -d "${EXTRACT_DIR}"
      PARQUET_IN_ZIP="$(ls "${EXTRACT_DIR}"/*.parquet 2>/dev/null | head -n 1 || true)"
      if [[ -z "${PARQUET_IN_ZIP}" ]]; then
        echo "No .parquet file found after unzip."
        exit 1
      fi
      UPLOAD_FILE="${PARQUET_IN_ZIP}"
      ;;
    *.gz)
      echo "Decompressing gzip..."
      GUNZIPPED_FILE="${EXTRACT_DIR}/$(basename "${DOWNLOADED_FILE%.gz}")"
      gzip -dc "${DOWNLOADED_FILE}" > "${GUNZIPPED_FILE}"
      UPLOAD_FILE="${GUNZIPPED_FILE}"
      ;;
    *.parquet)
      echo "Downloaded parquet directly; no unzip needed."
      ;;
    *)
      echo "Unknown extension; attempting to upload downloaded file directly."
      ;;
  esac
fi

echo "Uploading ${UPLOAD_FILE} to hf://${HF_REPO_ID}/${HF_PATH_IN_REPO} (${HF_REPO_TYPE})..."
export UPLOAD_FILE HF_PATH_IN_REPO HF_REPO_ID HF_REPO_TYPE HF_TOKEN

python3 - <<'PY'
import os
import sys

try:
    from huggingface_hub import HfApi
except ImportError:
    print("huggingface_hub is required. Install with: pip install huggingface_hub", file=sys.stderr)
    sys.exit(1)

api = HfApi(token=os.environ["HF_TOKEN"])
api.upload_file(
    path_or_fileobj=os.environ["UPLOAD_FILE"],
    path_in_repo=os.environ["HF_PATH_IN_REPO"],
    repo_id=os.environ["HF_REPO_ID"],
    repo_type=os.environ["HF_REPO_TYPE"],
)
print("Upload complete.")
PY

echo "Done."
