#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_PATH="${SCRIPT_DIR}/data_enricher/Cargo.toml"

cargo run --release --manifest-path "${MANIFEST_PATH}" -- "$@"
