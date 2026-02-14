use std::path::Path;

use anyhow::{Context, anyhow};

#[derive(Debug, Clone)]
pub struct ZipCentroid {
    pub zip5: String,
    pub lat: f64,
    pub lon: f64,
}

pub fn normalize_zip5(s: &str) -> Option<String> {
    let mut digits = String::with_capacity(5);
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            if digits.len() == 5 {
                break;
            }
        } else if !digits.is_empty() {
            break;
        }
    }
    if digits.len() == 5 {
        Some(digits)
    } else {
        None
    }
}

pub fn parse_geonames_us_txt(path: &Path) -> anyhow::Result<Vec<ZipCentroid>> {
    let data = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut out = Vec::new();
    for (lineno, line) in data.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 11 {
            return Err(anyhow!(
                "GeoNames line {} has too few columns ({}): {}",
                lineno + 1,
                parts.len(),
                line
            ));
        }
        let zip_raw = parts[1];
        let Some(zip5) = normalize_zip5(zip_raw) else {
            continue;
        };
        let lat: f64 = parts[9].parse().context("parse lat")?;
        let lon: f64 = parts[10].parse().context("parse lon")?;
        out.push(ZipCentroid { zip5, lat, lon });
    }
    Ok(out)
}
