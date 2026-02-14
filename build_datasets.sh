#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  build_datasets.sh [script-options] [--] [build_datasets args]

Script options:
  --log-file <path>    Write stdout/stderr to a log file (keeps TTY progress bars)
  --append-log         Append to log file instead of overwriting
  -h, --help           Show this help

Examples:
  ./build_datasets.sh --build-map-only
  ./build_datasets.sh --log-file ./logs/run.log --build-map-only --skip-api
  ./build_datasets.sh --log-file ./logs/run.log -- --build-map-only --skip-api
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_PATH="${SCRIPT_DIR}/build_datasets/Cargo.toml"

LOG_FILE=""
APPEND_LOG=0
RUST_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --log-file)
      LOG_FILE="${2:-}"
      if [[ -z "${LOG_FILE}" ]]; then
        echo "Error: --log-file requires a path." >&2
        exit 1
      fi
      shift 2
      ;;
    --append-log)
      APPEND_LOG=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      RUST_ARGS+=("$@")
      break
      ;;
    *)
      RUST_ARGS+=("$1")
      shift
      ;;
  esac
done

CMD=(cargo run --release --manifest-path "${MANIFEST_PATH}" --)
if [[ ${#RUST_ARGS[@]} -gt 0 ]]; then
  CMD+=("${RUST_ARGS[@]}")
fi

if [[ -n "${LOG_FILE}" ]]; then
  mkdir -p "$(dirname "${LOG_FILE}")"
  if [[ -t 1 && -t 2 ]] && command -v script >/dev/null 2>&1; then
    SCRIPT_FLAGS=(-q -e)
    if [[ "${APPEND_LOG}" -eq 1 ]]; then
      SCRIPT_FLAGS+=(-a)
    fi
    script "${SCRIPT_FLAGS[@]}" "${LOG_FILE}" "${CMD[@]}"
  else
    if [[ "${APPEND_LOG}" -eq 1 ]]; then
      "${CMD[@]}" 2>&1 | tee -a "${LOG_FILE}"
    else
      "${CMD[@]}" 2>&1 | tee "${LOG_FILE}"
    fi
  fi
else
  "${CMD[@]}"
fi
