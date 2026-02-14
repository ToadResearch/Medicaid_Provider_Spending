#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  download.sh [options] [-- <args forwarded to build_datasets.sh>]

Options:
  --url <dataset-url>         Medicaid source URL to download
  --nppes-index-url <url>     NPPES index page (default: CMS NPI files page)
  --nppes-monthly-url <url>   Explicit monthly NPPES V2 zip URL
  --nppes-weekly-url <url>    Explicit weekly NPPES V2 zip URL
  --cpt-index-url <url>       CPT index/page URL used for auto-discovery
  --cpt-zip-url <url>         CPT/HCPCS source zip URL (repeatable)
  --skip-nppes                Skip NPPES monthly/weekly downloads
  --data-dir <path>           Data directory (default: ./data)
  --run-rust                  Run Rust pipeline after downloads
  -h, --help                  Show this help

Examples:
  ./download.sh
  ./download.sh --run-rust
  ./download.sh --cpt-zip-url "https://example.org/pfrev26a.zip"
  ./download.sh --run-rust -- --skip-api
  ./download.sh --run-rust -- --reset-map
EOF
}

DATASET_URL="https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
NPPES_INDEX_URL="http://download.cms.gov/nppes/NPI_Files.html"
NPPES_MONTHLY_URL=""
NPPES_WEEKLY_URL=""
CPT_INDEX_URL="https://www.cms.gov/medicare/payment/fee-schedules/physician/national-payment-amount-file/pfrev26a"
CPT_ZIP_URLS=()
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
RUN_RUST=0
SKIP_NPPES=0
RUST_ARGS=()

download_if_missing() {
  local file_url="$1"
  local target_path="$2"
  local label="$3"

  mkdir -p "$(dirname "${target_path}")"
  if [[ -s "${target_path}" ]]; then
    echo "Using existing ${label} file at ${target_path}; skipping download." >&2
    return
  fi

  if [[ -e "${target_path}" ]]; then
    echo "Found empty/incomplete ${label} file at ${target_path}; re-downloading." >&2
    rm -f "${target_path}"
  fi

  echo "Downloading ${label} to ${target_path}" >&2
  if [[ -t 2 ]]; then
    curl -L --fail --retry 3 --progress-bar "${file_url}" -o "${target_path}"
  else
    curl -L --fail --retry 3 "${file_url}" -o "${target_path}"
  fi
}

discover_nppes_urls() {
  python3 - <<'PY' "${NPPES_INDEX_URL}"
import re
import sys
import urllib.parse
import urllib.request

index_url = sys.argv[1]
html = urllib.request.urlopen(index_url, timeout=30).read().decode("utf-8", "ignore")
links = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)

monthly = []
weekly = []
for link in links:
    full = urllib.parse.urljoin(index_url, link)
    lower = full.lower()
    if not lower.endswith(".zip"):
        continue
    if "nppes_data_dissemination" not in lower:
        continue
    if "_v2.zip" not in lower:
        continue
    if "weekly" in lower:
        weekly.append(full)
    elif "deactivated" not in lower:
        monthly.append(full)

if not monthly or not weekly:
    raise SystemExit("Could not discover monthly/weekly NPPES V2 URLs from index page.")

# The page lists newest first.
print(monthly[0])
print(weekly[0])
PY
}

discover_cpt_zip_url() {
  python3 - <<'PY' "${CPT_INDEX_URL}"
import re
import sys
import urllib.parse
import urllib.request

index_url = sys.argv[1]
html = urllib.request.urlopen(index_url, timeout=30).read().decode("utf-8", "ignore")
links = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)

candidates = []
for link in links:
    full = urllib.parse.urljoin(index_url, link)
    lower = full.lower()
    if not lower.endswith(".zip"):
        continue
    # Prefer physician fee schedule rev files first, but allow generic zip fallback.
    score = 0
    if "pfrev" in lower:
        score += 10
    if "updated" in lower:
        score += 2
    if "/files/zip/" in lower:
        score += 1
    candidates.append((score, full))

if not candidates:
    raise SystemExit(1)

candidates.sort(key=lambda x: x[0], reverse=True)
print(candidates[0][1])
PY
}

has_local_cpt_source_data() {
  local cpt_root="$1"
  local fallback_csv="$2"

  if [[ -s "${fallback_csv}" ]]; then
    return 0
  fi
  if [[ ! -d "${cpt_root}" ]]; then
    return 1
  fi

  local found=""
  found="$(find "${cpt_root}" -type f \( -name '*.zip' -o -name '*.csv' -o -name '*.txt' -o -name '*.tsv' \) ! -path "${fallback_csv}" -print -quit)"
  [[ -n "${found}" ]]
}

normalize_trailing_space_dirs() {
  local root_dir="$1"
  local dir=""
  local parent=""
  local base=""
  local trimmed=""
  local target=""

  if [[ ! -d "${root_dir}" ]]; then
    return
  fi

  while IFS= read -r -d '' dir; do
    if [[ "${dir}" == "${root_dir}" ]]; then
      continue
    fi

    parent="$(dirname "${dir}")"
    base="$(basename "${dir}")"
    trimmed="$(printf '%s' "${base}" | sed -E 's/[[:space:]]+$//')"
    if [[ -z "${trimmed}" || "${trimmed}" == "${base}" ]]; then
      continue
    fi

    target="${parent}/${trimmed}"
    echo "Normalizing folder name: ${dir#${DATA_DIR}/} -> ${target#${DATA_DIR}/}" >&2
    if [[ -d "${target}" ]]; then
      if command -v rsync >/dev/null 2>&1; then
        rsync -a "${dir}/" "${target}/"
      else
        cp -R "${dir}/." "${target}/"
      fi
      rm -rf "${dir}"
    else
      mv "${dir}" "${target}"
    fi
  done < <(find "${root_dir}" -depth -type d -print0)
}

apply_manual_clean_overrides() {
  local manual_root="$1"
  local cpt_root="$2"
  local source_nonqp=""
  local target_nonqp=""
  local subdir=""
  local source_dir=""
  local target_dir=""

  source_nonqp="${manual_root}/PFREV26AR_nonQP"
  if [[ ! -d "${source_nonqp}" ]]; then
    return
  fi

  target_nonqp="$(find "${cpt_root}" -type d -name 'PFREV26AR_nonQP' -print -quit || true)"
  if [[ -z "${target_nonqp}" ]]; then
    target_nonqp="${cpt_root}/archives/PFREV26A/PFREV26AR_nonQP"
  fi
  mkdir -p "${target_nonqp}"

  for subdir in csv md; do
    source_dir="${source_nonqp}/${subdir}"
    target_dir="${target_nonqp}/${subdir}"
    if [[ ! -d "${source_dir}" ]]; then
      continue
    fi

    mkdir -p "${target_dir}"
    echo "Applying manually cleaned ${subdir}: ${source_dir#${DATA_DIR}/} -> ${target_dir#${DATA_DIR}/}" >&2
    if command -v rsync >/dev/null 2>&1; then
      rsync -a --delete "${source_dir}/" "${target_dir}/"
    else
      cp -R "${source_dir}/." "${target_dir}/"
    fi
  done

  if [[ -f "${source_nonqp}/PF26PAR_QP.md" ]]; then
    cp -f "${source_nonqp}/PF26PAR_QP.md" "${target_nonqp}/PF26PAR_QP.md"
  fi
}

find_primary_nppes_csv() {
  python3 - <<'PY' "$1"
import csv
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
best = None
for path in root.rglob("*.csv"):
    try:
        with path.open("r", newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            headers = next(reader, [])
    except Exception:
        continue
    header_set = {h.strip() for h in headers}
    if "NPI" not in header_set or "Entity Type Code" not in header_set:
        continue
    if (
        "Provider Organization Name (Legal Business Name)" not in header_set
        and not (
            "Provider First Name" in header_set
            and "Provider Last Name (Legal Name)" in header_set
        )
    ):
        continue
    size = path.stat().st_size
    if best is None or size > best[0]:
        best = (size, path)

if best is None:
    raise SystemExit(1)
print(best[1])
PY
}

download_and_extract_nppes_zip() {
  local file_url="$1"
  local target_dir="$2"
  local label="$3"

  mkdir -p "${target_dir}"
  local zip_path="${target_dir}/$(basename "${file_url}")"
  download_if_missing "${file_url}" "${zip_path}" "${label} NPPES archive"

  local extract_dir="${target_dir}/$(basename "${zip_path%.zip}")"
  mkdir -p "${extract_dir}"
  local csv_path
  csv_path="$(find_primary_nppes_csv "${extract_dir}" || true)"
  if [[ -n "${csv_path}" && -s "${csv_path}" ]]; then
    echo "Using existing extracted ${label} NPPES CSV at ${csv_path}" >&2
    echo "${csv_path}"
    return
  fi

  echo "Extracting ${label} NPPES zip..." >&2
  unzip -o "${zip_path}" -d "${extract_dir}" >/dev/null

  csv_path="$(find_primary_nppes_csv "${extract_dir}")"
  if [[ -z "${csv_path}" ]]; then
    echo "Failed to locate primary NPPES CSV in ${extract_dir}" >&2
    exit 1
  fi
  echo "${csv_path}"
}

download_zip_to_dir() {
  local file_url="$1"
  local target_dir="$2"

  mkdir -p "${target_dir}"
  local zip_path="${target_dir}/$(basename "${file_url}")"
  download_if_missing "${file_url}" "${zip_path}" "CPT/HCPCS archive"
}

extract_nested_zips() {
  local root_dir="$1"
  python3 - <<'PY' "${root_dir}"
import pathlib
import sys
import zipfile

root = pathlib.Path(sys.argv[1])
if not root.exists():
    print(f"Skip extracting zips: missing directory {root}", file=sys.stderr)
    sys.exit(0)

processed = set()
total = 0
pass_number = 1

def should_extract_to_parent(members):
    files = []
    for name in members:
        name = name.strip().lstrip("./")
        if not name or name.startswith("__MACOSX/") or name.endswith("/"):
            continue
        files.append(name)
    if not files:
        return True
    if any("/" not in name for name in files):
        return False
    top_levels = {name.split("/", 1)[0] for name in files}
    return len(top_levels) == 1

while True:
    found_new = False
    for zip_path in sorted(root.rglob("*.zip")):
        zip_path = zip_path.resolve()
        if zip_path in processed:
            continue
        processed.add(zip_path)
        found_new = True
        total += 1

        try:
            with zipfile.ZipFile(zip_path) as zf:
                names = zf.namelist()
                if should_extract_to_parent(names):
                    target = zip_path.parent
                else:
                    target = zip_path.parent / zip_path.stem
                    target.mkdir(parents=True, exist_ok=True)
                print(f"[{pass_number}] extracting: {zip_path}", file=sys.stderr)
                zf.extractall(target)
        except Exception as exc:
            raise SystemExit(f"Failed extracting {zip_path}: {exc}")

    if not found_new:
        break
    pass_number += 1

print(f"Done. Extracted {total} archive(s) under {root}.", file=sys.stderr)
PY
}

build_cpt_fallback_csv() {
  local source_root="$1"
  local output_csv="$2"
  python3 - <<'PY' "${source_root}" "${output_csv}"
import csv
import pathlib
import re
import sys

source_root = pathlib.Path(sys.argv[1])
output_csv = pathlib.Path(sys.argv[2])

if not source_root.exists():
    print("", end="")
    sys.exit(0)

def normalize_header(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())

def pick_index(headers, aliases):
    normalized = [normalize_header(h) for h in headers]
    for alias in aliases:
        target = normalize_header(alias)
        if target in normalized:
            return normalized.index(target)
    return None

def detect_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",|\t;").delimiter
    except Exception:
        counts = {",": sample.count(","), "|": sample.count("|"), "\t": sample.count("\t"), ";": sample.count(";")}
        return max(counts, key=counts.get)

def normalize_code(raw: str):
    value = "".join((raw or "").strip().split())
    if value.endswith(".0"):
        value = value[:-2]
    value = value.upper()
    if len(value) == 5 and re.fullmatch(r"[A-Z0-9]{5}", value):
        return value
    return None

def parse_boolish(value: str) -> str:
    lowered = (value or "").strip().lower()
    return "true" if lowered in {"1", "true", "t", "yes", "y"} else "false"

def get_field(row, idx):
    if idx is None or idx >= len(row):
        return ""
    return row[idx].strip()

def score(entry):
    return (
        1 if entry["long_desc"] else 0,
        len(entry["long_desc"]),
        1 if entry["short_desc"] else 0,
        len(entry["short_desc"]),
    )

def add_candidate(records, candidate):
    existing = records.get(candidate["hcpcs_code"])
    if existing is None or score(candidate) > score(existing):
        records[candidate["hcpcs_code"]] = candidate

records = {}
parsed_files = 0
candidate_files = sorted(source_root.rglob("*.csv")) + sorted(source_root.rglob("*.txt")) + sorted(source_root.rglob("*.tsv"))
status_descriptions = {}

for status_file in source_root.rglob("status_code.csv"):
    try:
        with status_file.open("r", encoding="utf-8", errors="ignore", newline="") as sfh:
            reader = csv.reader(sfh)
            header = next(reader, None)
            if not header:
                continue
            status_idx = pick_index(header, ["status_code", "status code", "code"])
            desc_idx = pick_index(header, ["description", "long_desc", "long_description"])
            if status_idx is None or desc_idx is None:
                continue
            for row in reader:
                code = get_field(row, status_idx).upper()
                desc = get_field(row, desc_idx)
                if code and desc:
                    status_descriptions[code] = desc
    except Exception:
        continue

for path in candidate_files:
    if path.resolve() == output_csv.resolve():
        continue
    if path.suffix.lower() == ".zip":
        continue
    upper_name = path.name.upper()
    if upper_name.startswith("PFALL") and path.suffix.lower() == ".txt":
        try:
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as fh:
                reader = csv.reader(fh)
                parsed_files += 1
                for row in reader:
                    if len(row) < 4:
                        continue
                    code = normalize_code(get_field(row, 3))
                    if not code:
                        continue
                    status_code = get_field(row, 9).upper() if len(row) > 9 else ""
                    status_desc = status_descriptions.get(status_code, "")
                    short_desc = (
                        f"CPT/HCPCS code from CMS PFS file (status {status_code})"
                        if status_code
                        else "CPT/HCPCS code from CMS PFS file"
                    )
                    long_desc = status_desc or short_desc
                    add_candidate(
                        records,
                        {
                            "hcpcs_code": code,
                            "short_desc": short_desc,
                            "long_desc": long_desc,
                            "add_dt": "",
                            "act_eff_dt": "",
                            "term_dt": "",
                            "obsolete": "false",
                            "is_noc": "false",
                            "source_file": str(path.relative_to(source_root)),
                        },
                    )
            continue
        except Exception:
            pass
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as fh:
            sample = fh.read(4096)
            fh.seek(0)
            delim = detect_delimiter(sample)
            reader = csv.reader(fh, delimiter=delim)
            headers = next(reader, None)
            if not headers:
                continue
            code_idx = pick_index(headers, ["hcpcs_code", "cpt_code", "procedure_code", "billing_code", "code", "hcpcs", "cpt"])
            short_idx = pick_index(headers, ["short_desc", "short_description", "desc_short", "display"])
            long_idx = pick_index(headers, ["long_desc", "long_description", "description", "desc_long"])
            add_dt_idx = pick_index(headers, ["add_dt", "add_date"])
            act_eff_idx = pick_index(headers, ["act_eff_dt", "act_eff_date", "effective_date", "effective_dt"])
            term_dt_idx = pick_index(headers, ["term_dt", "term_date", "end_date"])
            obsolete_idx = pick_index(headers, ["obsolete", "is_obsolete"])
            is_noc_idx = pick_index(headers, ["is_noc", "noc"])

            if code_idx is None or (short_idx is None and long_idx is None):
                continue

            parsed_files += 1
            for row in reader:
                code = normalize_code(get_field(row, code_idx))
                if not code:
                    continue
                short_desc = get_field(row, short_idx)
                long_desc = get_field(row, long_idx)
                if not short_desc and not long_desc:
                    continue
                if not short_desc:
                    short_desc = long_desc
                if not long_desc:
                    long_desc = short_desc

                candidate = {
                    "hcpcs_code": code,
                    "short_desc": short_desc,
                    "long_desc": long_desc,
                    "add_dt": get_field(row, add_dt_idx),
                    "act_eff_dt": get_field(row, act_eff_idx),
                    "term_dt": get_field(row, term_dt_idx),
                    "obsolete": parse_boolish(get_field(row, obsolete_idx)),
                    "is_noc": parse_boolish(get_field(row, is_noc_idx)),
                    "source_file": str(path.relative_to(source_root)),
                }
                add_candidate(records, candidate)
    except Exception:
        continue

if not records:
    if output_csv.exists():
        output_csv.unlink()
    print("", end="")
    sys.exit(0)

output_csv.parent.mkdir(parents=True, exist_ok=True)
with output_csv.open("w", encoding="utf-8", newline="") as fh:
    writer = csv.writer(fh)
    writer.writerow(
        [
            "hcpcs_code",
            "short_desc",
            "long_desc",
            "add_dt",
            "act_eff_dt",
            "term_dt",
            "obsolete",
            "is_noc",
            "source_file",
        ]
    )
    for code in sorted(records):
        entry = records[code]
        writer.writerow(
            [
                entry["hcpcs_code"],
                entry["short_desc"],
                entry["long_desc"],
                entry["add_dt"],
                entry["act_eff_dt"],
                entry["term_dt"],
                entry["obsolete"],
                entry["is_noc"],
                entry["source_file"],
            ]
        )

print(
    f"Built CPT/HCPCS fallback CSV at {output_csv} (rows={len(records)}, parsed_files={parsed_files})",
    file=sys.stderr,
)
print(str(output_csv), end="")
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      DATASET_URL="${2:-}"
      shift 2
      ;;
    --nppes-index-url)
      NPPES_INDEX_URL="${2:-}"
      shift 2
      ;;
    --nppes-monthly-url)
      NPPES_MONTHLY_URL="${2:-}"
      shift 2
      ;;
    --nppes-weekly-url)
      NPPES_WEEKLY_URL="${2:-}"
      shift 2
      ;;
    --cpt-index-url)
      CPT_INDEX_URL="${2:-}"
      shift 2
      ;;
    --cpt-zip-url)
      CPT_ZIP_URLS+=("${2:-}")
      shift 2
      ;;
    --skip-nppes)
      SKIP_NPPES=1
      shift
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

RAW_DIR="${DATA_DIR}/raw"
MANUAL_CLEAN_ROOT="${DATA_DIR}/manually_cleaned"
MEDICAID_RAW_DIR="${RAW_DIR}/medicaid"
NPPES_RAW_DIR="${RAW_DIR}/nppes"
NPPES_MONTHLY_DIR="${NPPES_RAW_DIR}/monthly"
NPPES_WEEKLY_DIR="${NPPES_RAW_DIR}/weekly"
CPT_RAW_DIR="${RAW_DIR}/cpt"
CPT_ARCHIVE_DIR="${CPT_RAW_DIR}/archives"
CPT_FALLBACK_CSV="${CPT_RAW_DIR}/cpt_hcpcs_fallback.csv"

mkdir -p "${MEDICAID_RAW_DIR}" "${NPPES_MONTHLY_DIR}" "${NPPES_WEEKLY_DIR}" "${CPT_ARCHIVE_DIR}"

DOWNLOAD_PATH="${MEDICAID_RAW_DIR}/$(basename "${DATASET_URL}")"
download_if_missing "${DATASET_URL}" "${DOWNLOAD_PATH}" "Medicaid source dataset"

FINAL_PATH="${DOWNLOAD_PATH}"
case "${DOWNLOAD_PATH}" in
  *.zip)
    EXTRACT_DIR="${MEDICAID_RAW_DIR}/extracted"
    mkdir -p "${EXTRACT_DIR}"
    EXISTING_PARQUET="$(python3 - <<'PY' "${EXTRACT_DIR}"
import pathlib
import sys

extract_dir = pathlib.Path(sys.argv[1])
parquet_files = sorted(extract_dir.rglob("*.parquet"))
print(parquet_files[0] if parquet_files else "", end="")
PY
)"
    if [[ -n "${EXISTING_PARQUET}" && -s "${EXISTING_PARQUET}" ]]; then
      echo "Using existing extracted parquet: ${EXISTING_PARQUET}"
      FINAL_PATH="${EXISTING_PARQUET}"
    else
      echo "Unzipping archive..."
      unzip -o "${DOWNLOAD_PATH}" -d "${EXTRACT_DIR}" >/dev/null
      FINAL_PATH="$(python3 - <<'PY' "${EXTRACT_DIR}"
import pathlib
import sys

extract_dir = pathlib.Path(sys.argv[1])
parquet_files = sorted(extract_dir.rglob("*.parquet"))
if not parquet_files:
    raise SystemExit(1)
print(parquet_files[0])
PY
)"
      if [[ -z "${FINAL_PATH}" ]]; then
        echo "No parquet file found after unzip." >&2
        exit 1
      fi
    fi
    ;;
  *.gz)
    FINAL_PATH="${DOWNLOAD_PATH%.gz}"
    if [[ -s "${FINAL_PATH}" ]]; then
      echo "Using existing decompressed file: ${FINAL_PATH}"
    else
      echo "Decompressing gzip to ${FINAL_PATH}"
      gzip -dc "${DOWNLOAD_PATH}" > "${FINAL_PATH}"
    fi
    ;;
  *.parquet)
    echo "Downloaded parquet directly; no decompression needed."
    ;;
  *)
    echo "Unknown extension; using downloaded file directly."
    ;;
esac

echo "Ready file: ${FINAL_PATH}"

NPPES_MONTHLY_CSV=""
NPPES_WEEKLY_CSV=""
if [[ "${SKIP_NPPES}" -eq 0 ]]; then
  if [[ -z "${NPPES_MONTHLY_URL}" || -z "${NPPES_WEEKLY_URL}" ]]; then
    echo "Discovering latest NPPES monthly/weekly V2 URLs..."
    DISCOVERED="$(discover_nppes_urls)"
    DISCOVERED_MONTHLY="$(printf '%s\n' "${DISCOVERED}" | sed -n '1p')"
    DISCOVERED_WEEKLY="$(printf '%s\n' "${DISCOVERED}" | sed -n '2p')"
    if [[ -z "${NPPES_MONTHLY_URL}" ]]; then
      NPPES_MONTHLY_URL="${DISCOVERED_MONTHLY}"
    fi
    if [[ -z "${NPPES_WEEKLY_URL}" ]]; then
      NPPES_WEEKLY_URL="${DISCOVERED_WEEKLY}"
    fi
  fi

  NPPES_MONTHLY_CSV="$(download_and_extract_nppes_zip "${NPPES_MONTHLY_URL}" "${NPPES_MONTHLY_DIR}" "monthly")"
  NPPES_WEEKLY_CSV="$(download_and_extract_nppes_zip "${NPPES_WEEKLY_URL}" "${NPPES_WEEKLY_DIR}" "weekly")"
  echo "Monthly NPPES CSV: ${NPPES_MONTHLY_CSV}"
  echo "Weekly NPPES CSV:  ${NPPES_WEEKLY_CSV}"
else
  echo "Skipping NPPES downloads (--skip-nppes)."
fi

if [[ "${#CPT_ZIP_URLS[@]}" -gt 0 ]]; then
  for cpt_url in "${CPT_ZIP_URLS[@]}"; do
    if [[ -z "${cpt_url}" ]]; then
      continue
    fi
    download_zip_to_dir "${cpt_url}" "${CPT_ARCHIVE_DIR}"
  done
elif ! has_local_cpt_source_data "${CPT_RAW_DIR}" "${CPT_FALLBACK_CSV}"; then
  echo "No local CPT/HCPCS source data found under ${CPT_RAW_DIR}; discovering a default CPT archive..."
  DISCOVERED_CPT_URL="$(discover_cpt_zip_url || true)"
  if [[ -n "${DISCOVERED_CPT_URL}" ]]; then
    echo "Discovered CPT archive URL: ${DISCOVERED_CPT_URL}"
    download_zip_to_dir "${DISCOVERED_CPT_URL}" "${CPT_ARCHIVE_DIR}"
  else
    echo "Could not auto-discover a CPT archive URL from ${CPT_INDEX_URL}." >&2
    echo "Tip: pass one or more --cpt-zip-url values explicitly." >&2
  fi
fi

echo "Preparing local CPT/HCPCS fallback data under ${CPT_RAW_DIR}..."
extract_nested_zips "${CPT_RAW_DIR}"
normalize_trailing_space_dirs "${CPT_RAW_DIR}"
apply_manual_clean_overrides "${MANUAL_CLEAN_ROOT}" "${CPT_RAW_DIR}"
CPT_FALLBACK_READY="$(build_cpt_fallback_csv "${CPT_RAW_DIR}" "${CPT_FALLBACK_CSV}")"
if [[ -n "${CPT_FALLBACK_READY}" ]]; then
  echo "Local CPT/HCPCS fallback CSV: ${CPT_FALLBACK_READY}"
else
  echo "No local CPT/HCPCS fallback CSV built. HCPCS API will remain primary source."
fi

if [[ "${RUN_RUST}" -eq 1 ]]; then
  BUILD_SCRIPT="${SCRIPT_DIR}/build_datasets.sh"
  if [[ ! -x "${BUILD_SCRIPT}" ]]; then
    echo "Rust wrapper script not executable: ${BUILD_SCRIPT}" >&2
    exit 1
  fi

  echo "Running Rust pipeline..."
  RUST_CMD=(
    "${BUILD_SCRIPT}"
    --input-path "${FINAL_PATH}"
    --nppes-monthly-dir "${NPPES_MONTHLY_DIR}"
    --nppes-weekly-dir "${NPPES_WEEKLY_DIR}"
  )
  if [[ "${SKIP_NPPES}" -eq 1 ]]; then
    RUST_CMD+=(--skip-nppes-bulk)
  fi
  if [[ -f "${CPT_FALLBACK_CSV}" ]]; then
    RUST_CMD+=(--hcpcs-fallback-csv "${CPT_FALLBACK_CSV}")
  fi
  if [[ ${#RUST_ARGS[@]} -gt 0 ]]; then
    RUST_CMD+=("${RUST_ARGS[@]}")
  fi
  "${RUST_CMD[@]}"
fi
