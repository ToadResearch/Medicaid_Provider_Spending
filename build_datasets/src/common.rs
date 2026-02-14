use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

pub fn delete_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("Failed deleting {}", path.display()))?;
    }
    Ok(())
}

pub fn project_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or(manifest_dir)
}

pub fn default_enriched_output_path(input_path: &Path, data_dir: &Path) -> PathBuf {
    let ext = input_path
        .extension()
        .and_then(|x| x.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "parquet".to_string());
    let stem = input_path
        .file_stem()
        .and_then(|x| x.to_str())
        .unwrap_or("medicaid-provider-spending");
    data_dir.join(format!("{stem}-enriched.{ext}"))
}

pub fn file_name_from_url(url: &str) -> Result<String> {
    let trimmed = url.trim().trim_end_matches('/');
    let file_name = trimmed
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .context("Could not derive filename from URL")?;
    Ok(file_name.to_string())
}

pub async fn download_file(client: &Client, url: &str, output_path: &Path) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed creating {}", parent.display()))?;
    }

    let mut response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("Download request failed for {url}"))?
        .error_for_status()
        .with_context(|| format!("Download failed for {url}"))?;

    let mut file = File::create(output_path)
        .with_context(|| format!("Failed creating {}", output_path.display()))?;

    let mut downloaded: u64 = 0;
    while let Some(chunk) = response
        .chunk()
        .await
        .context("Failed reading download stream")?
    {
        file.write_all(&chunk)
            .with_context(|| format!("Failed writing {}", output_path.display()))?;
        downloaded += chunk.len() as u64;
        if downloaded % (512 * 1024 * 1024) < chunk.len() as u64 {
            println!("Downloaded ~{} MiB", downloaded / (1024 * 1024));
        }
    }

    println!("Download complete: {}", output_path.display());
    Ok(())
}

pub fn sql_escape_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

pub fn source_expr(input_path: &Path) -> Result<String> {
    let escaped = sql_escape_path(input_path);
    let extension = input_path
        .extension()
        .and_then(|x| x.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match extension.as_str() {
        "parquet" => Ok(format!("read_parquet('{escaped}')")),
        "csv" => Ok(format!("read_csv_auto('{escaped}', header=true)")),
        _ => bail!(
            "Unsupported input extension for {}. Use .csv or .parquet",
            input_path.display()
        ),
    }
}

pub fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

pub fn parse_retry_after(value: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    let value = value?.to_str().ok()?.trim();
    let secs = value.parse::<u64>().ok()?;
    Some(Duration::from_secs(secs))
}

pub fn truncate_for_log(text: &str) -> String {
    let trimmed = text.trim();
    let max_len = 300usize;
    if trimmed.len() <= max_len {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..max_len])
    }
}

pub async fn wait_for_rate_slot(next_slot: &Arc<Mutex<Instant>>, min_interval: Duration) {
    if min_interval.is_zero() {
        return;
    }
    let mut guard = next_slot.lock().await;
    let now = Instant::now();
    if *guard > now {
        sleep(*guard - now).await;
    }
    *guard = Instant::now() + min_interval;
}

pub fn install_ctrlc_handler(shutdown_requested: Arc<AtomicBool>) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let was_set = shutdown_requested.swap(true, Ordering::SeqCst);
            if !was_set {
                eprintln!(
                    "\nReceived Ctrl-C. Finishing in-flight work, saving progress, and exiting safely..."
                );
            }
        }
    });
}

pub fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

pub fn now_unix_millis() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i128)
        .unwrap_or_default()
}

pub fn new_api_run_id() -> String {
    format!("api-run-{}", now_unix_millis())
}
