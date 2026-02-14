use anyhow::{Context, Result};
use csv::Writer;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    path::Path,
};

#[derive(Debug, Deserialize)]
struct UnresolvedRow {
    identifier_type: String,
    identifier: String,
    status: String,
    #[serde(default)]
    error_message: String,
    #[serde(default)]
    fetched_at_unix: String,
}

#[derive(Debug, Clone)]
struct TriageRow {
    identifier_type: String,
    identifier: String,
    status: String,
    error_message: String,
    fetched_at_unix: String,
    inferred_code_type: String,
    base_code: Option<String>,
    suffix_or_modifier: Option<String>,
    identifier_norm: String,
}

#[derive(Debug, Clone)]
pub struct IdentifierTriageSummary {
    pub hcpcs_rows: usize,
    pub hcpcs_needs_review_rows: usize,
    pub npi_rows: usize,
    pub npi_needs_review_rows: usize,
}

fn normalize_identifier(raw: &str) -> String {
    raw.trim().to_ascii_uppercase()
}

fn is_placeholder(u: &str) -> bool {
    matches!(
        u,
        "" | "-" | "0" | "00" | "000" | "0000" | "00000" | "000000" | "0000000" | "NONE"
            | "NULL"
            | "N/A"
            | "NA"
    )
}

fn is_ascii_upper_alpha(bytes: &[u8]) -> bool {
    bytes.iter().all(|b| b.is_ascii_uppercase())
}

fn is_ascii_digit(bytes: &[u8]) -> bool {
    bytes.iter().all(|b| b.is_ascii_digit())
}

fn is_ascii_upper_alphanum(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
}

fn is_icd10pcs_like_7char(u: &str) -> bool {
    if u.len() != 7 {
        return false;
    }
    u.as_bytes().iter().all(|b| match *b {
        b'0'..=b'9' => true,
        b'A'..=b'H' => true,
        b'J'..=b'N' => true,
        b'P'..=b'Z' => true,
        _ => false,
    })
}

fn classify_hcpcs_identifier(raw: &str) -> (String, Option<String>, Option<String>) {
    let s = raw.trim();
    let u = s.to_ascii_uppercase();

    if is_placeholder(&u) {
        return ("placeholder_or_invalid".to_string(), None, None);
    }

    let bytes = u.as_bytes();

    // flags/words like LTFUP, VOID, etc.
    if bytes.len() >= 3 && is_ascii_upper_alpha(bytes) {
        return ("word_or_flag".to_string(), None, None);
    }

    // two-character modifier (e.g., 25, GT, QW, U3, etc.)
    if bytes.len() == 2 && is_ascii_upper_alphanum(bytes) {
        return (
            "modifier_2char".to_string(),
            Some(u.clone()),
            Some(u.clone()),
        );
    }

    // CDT dental + suffix (e.g., D0123A, D0123AB)
    if (bytes.len() == 6 || bytes.len() == 7)
        && bytes.first().copied() == Some(b'D')
        && is_ascii_digit(&bytes[1..5])
        && is_ascii_upper_alphanum(&bytes[5..])
    {
        return (
            "CDT_plus_suffix".to_string(),
            Some(u[..5].to_string()),
            Some(u[5..].to_string()),
        );
    }
    if bytes.len() == 5 && bytes.first().copied() == Some(b'D') && is_ascii_digit(&bytes[1..]) {
        return ("CDT".to_string(), Some(u), None);
    }

    // CPT 5-digit + modifier (e.g., 99213GT)
    if bytes.len() == 7 && is_ascii_digit(&bytes[..5]) && is_ascii_upper_alphanum(&bytes[5..]) {
        return (
            "CPT_5digit_plus_modifier".to_string(),
            Some(u[..5].to_string()),
            Some(u[5..].to_string()),
        );
    }

    // HCPCS Level II + modifier (e.g., Q3014GT, H0020U3)
    if bytes.len() == 7
        && bytes[0].is_ascii_uppercase()
        && is_ascii_digit(&bytes[1..5])
        && is_ascii_upper_alphanum(&bytes[5..])
    {
        return (
            "HCPCS_L2_plus_modifier".to_string(),
            Some(u[..5].to_string()),
            Some(u[5..].to_string()),
        );
    }

    // CPT Category II + modifier (e.g., 0001F25)
    if bytes.len() == 7
        && is_ascii_digit(&bytes[..4])
        && bytes[4] == b'F'
        && is_ascii_upper_alphanum(&bytes[5..])
    {
        return (
            "CPT_catII_plus_modifier".to_string(),
            Some(u[..5].to_string()),
            Some(u[5..].to_string()),
        );
    }

    // CPT Category II/III/PLA
    if bytes.len() == 5 && is_ascii_digit(&bytes[..4]) && bytes[4] == b'F' {
        return ("CPT_category_II".to_string(), Some(u), None);
    }
    if bytes.len() == 5 && is_ascii_digit(&bytes[..4]) && bytes[4] == b'T' {
        return ("CPT_category_III".to_string(), Some(u), None);
    }
    if bytes.len() == 5 && is_ascii_digit(&bytes[..4]) && bytes[4] == b'U' {
        return ("CPT_PLA".to_string(), Some(u), None);
    }

    // HCPCS Level II (A0000..Z9999 style)
    if bytes.len() == 5 && bytes[0].is_ascii_uppercase() && is_ascii_digit(&bytes[1..]) {
        return ("HCPCS_level_II".to_string(), Some(u), None);
    }

    // CPT / HCPCS Level I 5-digit numeric
    if bytes.len() == 5 && is_ascii_digit(bytes) {
        return ("CPT_or_HCPCS_L1_5digit".to_string(), Some(u), None);
    }

    // revenue codes (UB-04)
    if bytes.len() == 4 && is_ascii_digit(bytes) {
        return ("revenue_code_4digit".to_string(), Some(u), None);
    }

    // DRG-like (3 digits)
    if bytes.len() == 3 && is_ascii_digit(bytes) {
        return ("drg_like_3digit".to_string(), Some(u), None);
    }

    // ICD-10-PCS-like (7 chars, excluding I/O)
    if is_icd10pcs_like_7char(&u) {
        return ("icd10pcs_like_7char".to_string(), Some(u), None);
    }

    // 4 digits + letter (non F/T/U)
    if bytes.len() == 5 && is_ascii_digit(&bytes[..4]) && bytes[4].is_ascii_uppercase() {
        return ("4digit_plus_letter_other".to_string(), Some(u), None);
    }

    // other numeric lengths (6-8) that show up
    if (6..=8).contains(&bytes.len()) && is_ascii_digit(bytes) {
        return ("numeric_6to8_unknown".to_string(), Some(u), None);
    }

    // catch common 5-char alphanumeric unknowns
    if bytes.len() == 5 && is_ascii_upper_alphanum(bytes) {
        return ("alphanum_5char_unknown".to_string(), Some(u), None);
    }

    ("unknown".to_string(), Some(u), None)
}

fn luhn_mod10(digits: &[u32]) -> u32 {
    let mut sum: u32 = 0;
    let mut double = false;
    for d in digits.iter().rev() {
        let mut v = *d;
        if double {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
        double = !double;
    }
    sum % 10
}

fn npi_luhn_valid(npi_digits: &str) -> bool {
    if npi_digits.len() != 10 || !npi_digits.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return false;
    }

    // NPI validation uses Luhn with the fixed "80840" prefix applied to the full 10 digits.
    let mut digits: [u32; 15] = [0; 15];
    digits[..5].copy_from_slice(&[8, 0, 8, 4, 0]);
    for (idx, b) in npi_digits.as_bytes().iter().enumerate() {
        digits[5 + idx] = (b - b'0') as u32;
    }
    luhn_mod10(&digits) == 0
}

fn classify_npi_identifier(raw: &str) -> (String, Option<String>, Option<String>) {
    let s = raw.trim();
    let u = s.to_ascii_uppercase();

    if is_placeholder(&u) || (!u.is_empty() && u.as_bytes().iter().all(|b| *b == b'0')) {
        return ("placeholder_or_invalid".to_string(), None, None);
    }

    if !u.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return ("non_numeric".to_string(), None, None);
    }

    if u.len() != 10 {
        return ("numeric_wrong_len".to_string(), Some(u), None);
    }

    if npi_luhn_valid(&u) {
        ("npi_luhn_valid".to_string(), Some(u), None)
    } else {
        ("npi_luhn_invalid".to_string(), Some(u), None)
    }
}

fn write_triage_rows(path: &Path, rows: &[TriageRow]) -> Result<()> {
    let mut writer = Writer::from_path(path)
        .with_context(|| format!("Failed creating output CSV {}", path.display()))?;
    writer
        .write_record([
            "identifier_type",
            "identifier",
            "status",
            "error_message",
            "fetched_at_unix",
            "inferred_code_type",
            "base_code",
            "suffix_or_modifier",
            "identifier_norm",
        ])
        .context("Failed writing output header")?;

    for row in rows {
        writer
            .write_record([
                row.identifier_type.as_str(),
                row.identifier.as_str(),
                row.status.as_str(),
                row.error_message.as_str(),
                row.fetched_at_unix.as_str(),
                row.inferred_code_type.as_str(),
                row.base_code.as_deref().unwrap_or(""),
                row.suffix_or_modifier.as_deref().unwrap_or(""),
                row.identifier_norm.as_str(),
            ])
            .context("Failed writing output row")?;
    }
    writer.flush().context("Failed flushing output writer")?;
    Ok(())
}

fn count_by_key<T, K>(rows: &[T], mut key_fn: impl FnMut(&T) -> K) -> HashMap<K, usize>
where
    K: std::hash::Hash + Eq,
{
    let mut counts: HashMap<K, usize> = HashMap::new();
    for row in rows {
        *counts.entry(key_fn(row)).or_insert(0) += 1;
    }
    counts
}

fn sorted_counts(mut counts: Vec<(String, usize)>) -> Vec<(String, usize)> {
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    counts
}

pub fn write_unresolved_identifier_triage(
    input_csv: &Path,
    out_dir: &Path,
) -> Result<IdentifierTriageSummary> {
    fs::create_dir_all(out_dir)
        .with_context(|| format!("Failed creating triage output dir {}", out_dir.display()))?;

    let mut reader = csv::Reader::from_path(input_csv)
        .with_context(|| format!("Failed opening unresolved identifiers CSV {}", input_csv.display()))?;

    let mut hcpcs_rows: Vec<TriageRow> = Vec::new();
    let mut npi_rows: Vec<TriageRow> = Vec::new();
    for result in reader.deserialize::<UnresolvedRow>() {
        let row = result.with_context(|| {
            format!(
                "Failed reading row from unresolved identifiers CSV {}",
                input_csv.display()
            )
        })?;

        let id_type = row.identifier_type.trim();
        if id_type.eq_ignore_ascii_case("hcpcs") {
            let (inferred, base, suffix) = classify_hcpcs_identifier(&row.identifier);
            let identifier_norm = normalize_identifier(&row.identifier);
            hcpcs_rows.push(TriageRow {
                identifier_type: row.identifier_type,
                identifier: row.identifier,
                status: row.status,
                error_message: row.error_message,
                fetched_at_unix: row.fetched_at_unix,
                inferred_code_type: inferred,
                base_code: base,
                suffix_or_modifier: suffix,
                identifier_norm,
            });
        } else if id_type.eq_ignore_ascii_case("npi") {
            let (inferred, base, suffix) = classify_npi_identifier(&row.identifier);
            let identifier_norm = normalize_identifier(&row.identifier);
            npi_rows.push(TriageRow {
                identifier_type: row.identifier_type,
                identifier: row.identifier,
                status: row.status,
                error_message: row.error_message,
                fetched_at_unix: row.fetched_at_unix,
                inferred_code_type: inferred,
                base_code: base,
                suffix_or_modifier: suffix,
                identifier_norm,
            });
        }
    }

    // --- HCPCS outputs ---
    let hcpcs_out_a = out_dir.join("hcpcs_identifiers_with_type.csv");
    let hcpcs_out_b = out_dir.join("hcpcs_identifiers_with_inferred_types.csv");
    write_triage_rows(&hcpcs_out_a, &hcpcs_rows)?;
    write_triage_rows(&hcpcs_out_b, &hcpcs_rows)?;

    let hcpcs_needs_review = |t: &str| {
        matches!(
            t,
            "unknown"
                | "word_or_flag"
                | "placeholder_or_invalid"
                | "numeric_6to8_unknown"
                | "alphanum_5char_unknown"
        )
    };
    let hcpcs_concat_type = |t: &str| {
        matches!(
            t,
            "HCPCS_L2_plus_modifier"
                | "CPT_5digit_plus_modifier"
                | "CDT_plus_suffix"
                | "CPT_catII_plus_modifier"
        )
    };

    let hcpcs_unmapped: Vec<TriageRow> = hcpcs_rows
        .iter()
        .cloned()
        .filter(|r| hcpcs_needs_review(&r.inferred_code_type))
        .collect();
    write_triage_rows(&out_dir.join("hcpcs_unmapped_rows.csv"), &hcpcs_unmapped)?;

    let mut unmapped_unique: HashMap<(String, String), usize> = HashMap::new();
    for row in &hcpcs_unmapped {
        *unmapped_unique
            .entry((row.identifier_norm.clone(), row.inferred_code_type.clone()))
            .or_insert(0) += 1;
    }
    let mut unmapped_unique_items: Vec<((String, String), usize)> = unmapped_unique.into_iter().collect();
    unmapped_unique_items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)));
    {
        let path = out_dir.join("hcpcs_unmapped_unique_counts.csv");
        let mut writer = Writer::from_path(&path)
            .with_context(|| format!("Failed creating output CSV {}", path.display()))?;
        writer
            .write_record(["identifier_norm", "inferred_code_type", "count"])
            .context("Failed writing header")?;
        for ((id_norm, inferred), count) in unmapped_unique_items {
            writer
                .write_record([id_norm, inferred, count.to_string()])
                .context("Failed writing row")?;
        }
        writer.flush().context("Failed flushing writer")?;
    }

    let unknown_only: Vec<&TriageRow> = hcpcs_rows
        .iter()
        .filter(|r| matches!(r.inferred_code_type.as_str(), "unknown" | "alphanum_5char_unknown"))
        .collect();
    let unknown_counts = count_by_key(&unknown_only, |r| r.identifier_norm.clone());
    let mut unknown_items: Vec<(String, usize)> =
        unknown_counts.into_iter().map(|(k, v)| (k, v)).collect();
    unknown_items = sorted_counts(unknown_items);
    {
        let path = out_dir.join("hcpcs_unknown_unique_with_prefixes.csv");
        let mut writer = Writer::from_path(&path)
            .with_context(|| format!("Failed creating output CSV {}", path.display()))?;
        writer
            .write_record(["identifier_norm", "count", "len", "prefix2", "prefix3"])
            .context("Failed writing header")?;
        for (id_norm, count) in &unknown_items {
            let len = id_norm.chars().count();
            let prefix2: String = id_norm.chars().take(2).collect();
            let prefix3: String = id_norm.chars().take(3).collect();
            writer
                .write_record([
                    id_norm.as_str(),
                    count.to_string().as_str(),
                    len.to_string().as_str(),
                    prefix2.as_str(),
                    prefix3.as_str(),
                ])
                .context("Failed writing row")?;
        }
        writer.flush().context("Failed flushing writer")?;
    }

    let mut prefix2_counts: HashMap<String, usize> = HashMap::new();
    for (id_norm, count) in &unknown_items {
        let prefix2: String = id_norm.chars().take(2).collect();
        *prefix2_counts.entry(prefix2).or_insert(0) += *count;
    }
    let mut prefix2_items: Vec<(String, usize)> =
        prefix2_counts.into_iter().map(|(k, v)| (k, v)).collect();
    prefix2_items = sorted_counts(prefix2_items);
    {
        let path = out_dir.join("hcpcs_unknown_prefix2_counts.csv");
        let mut writer = Writer::from_path(&path)
            .with_context(|| format!("Failed creating output CSV {}", path.display()))?;
        writer
            .write_record(["prefix2", "count"])
            .context("Failed writing header")?;
        for (prefix2, count) in prefix2_items {
            writer
                .write_record([prefix2, count.to_string()])
                .context("Failed writing row")?;
        }
        writer.flush().context("Failed flushing writer")?;
    }

    let concat_rows: Vec<&TriageRow> = hcpcs_rows
        .iter()
        .filter(|r| hcpcs_concat_type(&r.inferred_code_type))
        .collect();
    let mut concat_counts: HashMap<(String, String, String, String), usize> = HashMap::new();
    for row in &concat_rows {
        let base = row.base_code.clone().unwrap_or_default();
        let suffix = row.suffix_or_modifier.clone().unwrap_or_default();
        *concat_counts
            .entry((
                row.identifier_norm.clone(),
                row.inferred_code_type.clone(),
                base,
                suffix,
            ))
            .or_insert(0) += 1;
    }
    let mut concat_items: Vec<((String, String, String, String), usize)> = concat_counts.into_iter().collect();
    concat_items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)));
    {
        let path = out_dir.join("hcpcs_concat_unique_counts.csv");
        let mut writer = Writer::from_path(&path)
            .with_context(|| format!("Failed creating output CSV {}", path.display()))?;
        writer
            .write_record([
                "identifier_norm",
                "inferred_code_type",
                "base_code",
                "suffix_or_modifier",
                "count",
            ])
            .context("Failed writing header")?;
        for ((id_norm, inferred, base, suffix), count) in concat_items {
            writer
                .write_record([id_norm, inferred, base, suffix, count.to_string()])
                .context("Failed writing row")?;
        }
        writer.flush().context("Failed flushing writer")?;
    }

    // --- NPI outputs ---
    write_triage_rows(
        &out_dir.join("npi_identifiers_with_inferred_types.csv"),
        &npi_rows,
    )?;
    let npi_needs_review = |t: &str| {
        matches!(
            t,
            "placeholder_or_invalid" | "non_numeric" | "numeric_wrong_len" | "npi_luhn_invalid"
        )
    };
    let npi_unmapped: Vec<TriageRow> = npi_rows
        .iter()
        .cloned()
        .filter(|r| npi_needs_review(&r.inferred_code_type))
        .collect();
    write_triage_rows(&out_dir.join("npi_unmapped_rows.csv"), &npi_unmapped)?;

    let mut npi_unmapped_unique: HashMap<(String, String), usize> = HashMap::new();
    for row in &npi_unmapped {
        *npi_unmapped_unique
            .entry((row.identifier_norm.clone(), row.inferred_code_type.clone()))
            .or_insert(0) += 1;
    }
    let mut npi_unmapped_unique_items: Vec<((String, String), usize)> =
        npi_unmapped_unique.into_iter().collect();
    npi_unmapped_unique_items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)));
    {
        let path = out_dir.join("npi_unmapped_unique_counts.csv");
        let mut writer = Writer::from_path(&path)
            .with_context(|| format!("Failed creating output CSV {}", path.display()))?;
        writer
            .write_record(["identifier_norm", "inferred_code_type", "count"])
            .context("Failed writing header")?;
        for ((id_norm, inferred), count) in npi_unmapped_unique_items {
            writer
                .write_record([id_norm, inferred, count.to_string()])
                .context("Failed writing row")?;
        }
        writer.flush().context("Failed flushing writer")?;
    }

    Ok(IdentifierTriageSummary {
        hcpcs_rows: hcpcs_rows.len(),
        hcpcs_needs_review_rows: hcpcs_unmapped.len(),
        npi_rows: npi_rows.len(),
        npi_needs_review_rows: npi_unmapped.len(),
    })
}

