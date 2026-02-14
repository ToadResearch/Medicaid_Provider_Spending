use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use tokio::io::AsyncWriteExt;

use crate::cli::BuildArgs;
use crate::storage::{SourceFiles, StoragePaths, file_present_nonempty};

const HF_BASE: &str = "https://huggingface.co/datasets";
const GEONAMES_US_ZIP_URL: &str = "https://download.geonames.org/export/zip/US.zip";

pub async fn ensure_inputs(
    paths: &StoragePaths,
    opts: &BuildArgs,
) -> anyhow::Result<(SourceFiles, PathBuf)> {
    tracing::info!("Ensuring input datasets (HF downloads with local reuse when available)...");
    paths.ensure_dirs().context("create data directories")?;

    let sources = ensure_hf_parquets(paths, opts).await?;
    let geonames_txt = ensure_geonames_zip_centroids(paths, opts).await?;
    Ok((sources, geonames_txt))
}

async fn ensure_hf_parquets(paths: &StoragePaths, opts: &BuildArgs) -> anyhow::Result<SourceFiles> {
    let sources = paths.source_files();
    tracing::info!("Inputs will be stored under {}", paths.source_dir.display());

    let spending_url = hf_resolve_url(&opts.hf_repo, &opts.hf_revision, "data/spending.parquet");
    let npi_url = hf_resolve_url(&opts.hf_repo, &opts.hf_revision, "data/npi.parquet");
    let hcpcs_url = hf_resolve_url(&opts.hf_repo, &opts.hf_revision, "data/hcpcs.parquet");

    ensure_one_file(&spending_url, &sources.spending, opts).await?;
    ensure_one_file(&npi_url, &sources.npi, opts).await?;
    ensure_one_file(&hcpcs_url, &sources.hcpcs, opts).await?;

    Ok(sources)
}

async fn ensure_geonames_zip_centroids(
    paths: &StoragePaths,
    opts: &BuildArgs,
) -> anyhow::Result<PathBuf> {
    if let Some(p) = opts.zip_centroids_file.as_ref() {
        return Ok(PathBuf::from(p));
    }

    let out_txt = paths.geonames_us_txt();
    if !opts.force_download && file_present_nonempty(&out_txt) {
        return Ok(out_txt);
    }

    if opts.offline {
        return Err(anyhow!(
            "Missing ZIP centroid file at {} (use --zip-centroids-file or run without --offline).",
            out_txt.display()
        ));
    }

    let zip_path = paths.geonames_us_zip();
    ensure_download(GEONAMES_US_ZIP_URL, &zip_path, opts.force_download).await?;
    extract_first_txt_from_zip(&zip_path, &out_txt).context("extract US.zip")?;
    Ok(out_txt)
}

async fn ensure_one_file(url: &str, dest: &Path, opts: &BuildArgs) -> anyhow::Result<()> {
    if !opts.force_download && file_present_nonempty(dest) {
        return Ok(());
    }

    if !opts.force_download {
        // If the user already ran the pipeline, prefer reusing existing local artifacts rather
        // than downloading large files again.
        if try_seed_from_repo_data(dest).context("seed from local repo data")? {
            return Ok(());
        }
    }

    if opts.offline {
        return Err(anyhow!(
            "Missing required input at {} (run without --offline to auto-download from {}).",
            dest.display(),
            url
        ));
    }
    ensure_download(url, dest, opts.force_download).await
}

fn try_seed_from_repo_data(dest: &Path) -> anyhow::Result<bool> {
    if file_present_nonempty(dest) {
        return Ok(true);
    }

    // If an empty file was left behind, replace it.
    if std::fs::metadata(dest)
        .map(|m| m.is_file() && m.len() == 0)
        .unwrap_or(false)
    {
        let _ = std::fs::remove_file(dest);
    }

    let fname = dest.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo_root = repo_root.canonicalize().unwrap_or(repo_root);

    let candidates: Vec<PathBuf> = match fname {
        "spending.parquet" => {
            vec![repo_root.join("data/raw/medicaid/medicaid-provider-spending.parquet")]
        }
        "npi.parquet" => vec![repo_root.join("data/output/npi.parquet")],
        "hcpcs.parquet" => vec![repo_root.join("data/output/hcpcs.parquet")],
        _ => vec![],
    };

    for src in candidates {
        if !file_present_nonempty(&src) {
            continue;
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        // Prefer hardlink (no duplication), then symlink, then copy.
        if std::fs::hard_link(&src, dest).is_ok() {
            tracing::info!(
                "Reused local artifact via hardlink: {} -> {}",
                dest.display(),
                src.display()
            );
            return Ok(true);
        }
        if symlink_file(&src, dest).is_ok() {
            tracing::info!(
                "Reused local artifact via symlink: {} -> {}",
                dest.display(),
                src.display()
            );
            return Ok(true);
        }
        if std::fs::copy(&src, dest).is_ok() {
            tracing::info!(
                "Reused local artifact via copy: {} -> {}",
                dest.display(),
                src.display()
            );
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(unix)]
fn symlink_file(src: &Path, dest: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(src, dest)
}

#[cfg(windows)]
fn symlink_file(src: &Path, dest: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(src, dest)
}

fn hf_resolve_url(repo: &str, rev: &str, path_in_repo: &str) -> String {
    format!("{HF_BASE}/{repo}/resolve/{rev}/{path_in_repo}")
}

async fn ensure_download(url: &str, dest: &Path, force: bool) -> anyhow::Result<()> {
    if !force && file_present_nonempty(dest) {
        return Ok(());
    }

    let tmp = tmp_path(dest);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    tracing::info!("Downloading {} -> {}", url, dest.display());

    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;

    if !resp.status().is_success() {
        return Err(anyhow!("Download failed ({}): {}", resp.status(), url));
    }

    let mut file = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("create {}", tmp.display()))?;

    let mut downloaded: u64 = 0;
    let mut stream = resp.bytes_stream();
    use futures_util::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("read body chunk from {url}"))?;
        downloaded += chunk.len() as u64;
        file.write_all(&chunk).await?;

        if downloaded % (50 * 1024 * 1024) < chunk.len() as u64 {
            tracing::info!("... downloaded {} MB", downloaded / (1024 * 1024));
        }
    }

    file.flush().await?;
    drop(file);

    tokio::fs::rename(&tmp, dest)
        .await
        .with_context(|| format!("rename {} -> {}", tmp.display(), dest.display()))?;

    Ok(())
}

fn tmp_path(dest: &Path) -> PathBuf {
    let fname = dest
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("download");
    dest.with_file_name(format!("{fname}.part"))
}

fn extract_first_txt_from_zip(zip_path: &Path, out_txt: &Path) -> anyhow::Result<()> {
    use std::io::{Read, Write};

    let f =
        std::fs::File::open(zip_path).with_context(|| format!("open {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(f).context("read zip archive")?;

    let mut chosen_index: Option<usize> = None;
    for i in 0..archive.len() {
        let name = archive.by_index(i)?.name().to_string();
        if name.ends_with("US.txt") {
            chosen_index = Some(i);
            break;
        }
        if chosen_index.is_none() && name.ends_with(".txt") {
            chosen_index = Some(i);
        }
    }
    let idx =
        chosen_index.ok_or_else(|| anyhow!("no .txt file found in {}", zip_path.display()))?;

    let mut zf = archive.by_index(idx)?;
    let mut buf = Vec::new();
    zf.read_to_end(&mut buf)?;

    if let Some(parent) = out_txt.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = std::fs::File::create(out_txt)?;
    out.write_all(&buf)?;
    out.flush()?;

    Ok(())
}
