#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  upload_medicaid_to_hf.sh [args]

Args:
  --token <HF_TOKEN>               Hugging Face token (or set HF_TOKEN)
  --repo-id <repo-id>              Repo id (default: mkieffer/Medicaid-Provider-Spending)
  --repo-type <dataset|model>      Repo type (default: dataset)
  --upload-split-dataset           Upload a 3-config dataset (spending + resolved NPI + resolved HCPCS identifier datasets)
  --skip-spending-upload           In split mode, do not upload spending parquet (assumes it already exists in the HF repo)
  --spending-file <local-path>     Spending dataset parquet (default: data/raw/medicaid/medicaid-provider-spending.parquet)
  --npi-file <path>                Resolved NPI identifier parquet (default: data/output/npi.parquet)
  --hcpcs-file <path>              Resolved HCPCS identifier parquet (default: data/output/hcpcs.parquet)
  --readme-file <path>             Dataset README to upload as hf://<repo>/README.md (default: hf/README.md)
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
HF_REPO_ID="${HF_REPO_ID:-mkieffer/Medicaid-Provider-Spending}"
HF_REPO_TYPE="${HF_REPO_TYPE:-dataset}"
HF_PATH_IN_REPO="${HF_PATH_IN_REPO:-medicaid-provider-spending.parquet}"
LOCAL_FILE="${HF_LOCAL_FILE:-}"
UPLOAD_SPLIT_DATASET=0
SKIP_SPENDING_UPLOAD=0
SPENDING_FILE=""
NPI_FILE=""
HCPCS_FILE=""
README_FILE=""

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
    --upload-split-dataset)
      UPLOAD_SPLIT_DATASET=1
      shift
      ;;
    --skip-spending-upload|--skip-provider-upload)
      SKIP_SPENDING_UPLOAD=1
      shift
      ;;
    --spending-file|--provider-file)
      SPENDING_FILE="${2:-}"
      shift 2
      ;;
    --npi-file|--npi-reference-file)
      NPI_FILE="${2:-}"
      shift 2
      ;;
    --hcpcs-file|--hcpcs-reference-file)
      HCPCS_FILE="${2:-}"
      shift 2
      ;;
    --readme-file)
      README_FILE="${2:-}"
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
case "${HF_REPO_TYPE}" in
  dataset|model)
    ;;
  *)
    echo "Unsupported --repo-type: ${HF_REPO_TYPE} (expected dataset|model)." >&2
    exit 1
    ;;
esac

if [[ "${HF_REPO_TYPE}" == "dataset" ]]; then
  HF_REPO_URL="https://huggingface.co/datasets/${HF_REPO_ID}"
else
  HF_REPO_URL="https://huggingface.co/${HF_REPO_ID}"
fi

mkdir -p "${TMP_DIR}"

PYTHON_RUN=(python3)
if ! python3 -c "import huggingface_hub" >/dev/null 2>&1; then
  if command -v uvx >/dev/null 2>&1; then
    # Use an ephemeral environment; no global install needed.
    PYTHON_RUN=(uvx --from huggingface_hub python3)
  elif command -v uv >/dev/null 2>&1; then
    # Use an ephemeral environment; no global install needed.
    PYTHON_RUN=(uv run --with huggingface_hub python3)
  else
    echo "huggingface_hub is required. Install with: pip install huggingface_hub" >&2
    echo "Or install uv and re-run (recommended): https://github.com/astral-sh/uv" >&2
    exit 1
  fi
fi

if [[ "${UPLOAD_SPLIT_DATASET}" -eq 1 ]]; then
  # Default to the raw spending parquet (as downloaded; unmodified).
  DEFAULT_SPENDING_RAW="${WORK_DIR}/data/raw/medicaid/medicaid-provider-spending.parquet"
  if [[ -z "${SPENDING_FILE}" ]]; then
    if [[ -f "${DEFAULT_SPENDING_RAW}" ]]; then
      SPENDING_FILE="${DEFAULT_SPENDING_RAW}"
    else
      echo "Spending file not found. Pass --spending-file, or build the dataset first." >&2
      exit 1
    fi
  fi
  if [[ -z "${NPI_FILE}" ]]; then
    NPI_FILE="${WORK_DIR}/data/output/npi.parquet"
  fi
  if [[ -z "${HCPCS_FILE}" ]]; then
    HCPCS_FILE="${WORK_DIR}/data/output/hcpcs.parquet"
  fi
  if [[ -z "${README_FILE}" ]]; then
    README_FILE="${WORK_DIR}/hf/README.md"
  fi

  required_files=("${NPI_FILE}" "${HCPCS_FILE}" "${README_FILE}")
  if [[ "${SKIP_SPENDING_UPLOAD}" -eq 0 ]]; then
    required_files+=("${SPENDING_FILE}")
  fi
  for f in "${required_files[@]}"; do
    if [[ ! -f "${f}" ]]; then
      echo "Required file not found: ${f}" >&2
      exit 1
    fi
  done

  echo "Uploading split dataset to hf://${HF_REPO_ID} (${HF_REPO_TYPE})..."
  export HF_TOKEN HF_REPO_ID HF_REPO_TYPE SPENDING_FILE NPI_FILE HCPCS_FILE README_FILE SKIP_SPENDING_UPLOAD

  ${PYTHON_RUN[@]} - <<'PY'
import os
import sys

try:
    from huggingface_hub import HfApi
except ImportError:
    print("huggingface_hub is required. Install with: pip install huggingface_hub", file=sys.stderr)
    sys.exit(1)

repo_id = os.environ["HF_REPO_ID"]
repo_type = os.environ["HF_REPO_TYPE"]
api = HfApi(token=os.environ["HF_TOKEN"])

# Ensure repo exists
api.create_repo(repo_id=repo_id, repo_type=repo_type, exist_ok=True)

uploads = [
    (os.environ["README_FILE"], "README.md", "Upload dataset card (README.md)"),
    (os.environ["NPI_FILE"], "data/npi.parquet", "Upload npi split"),
    (os.environ["HCPCS_FILE"], "data/hcpcs.parquet", "Upload hcpcs split"),
]

if os.environ.get("SKIP_SPENDING_UPLOAD", "0") != "1":
    uploads.insert(1, (os.environ["SPENDING_FILE"], "data/spending.parquet", "Upload spending split"))

for local_path, path_in_repo, msg in uploads:
    print(f"Uploading {local_path} -> hf://{repo_id}/{path_in_repo} ({repo_type})...")
    api.upload_file(
        path_or_fileobj=local_path,
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type=repo_type,
        commit_message=msg,
    )

print("Split dataset upload complete.")
print(f"Repo URL: { 'https://huggingface.co/datasets/' if repo_type == 'dataset' else 'https://huggingface.co/' }{repo_id}")
PY

  echo "Done."
  echo "Repo URL: ${HF_REPO_URL}"
  exit 0
fi

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

${PYTHON_RUN[@]} - <<'PY'
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
echo "Repo URL: ${HF_REPO_URL}"
