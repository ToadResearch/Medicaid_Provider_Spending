use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct StoragePaths {
    pub source_dir: PathBuf,
    pub geo_dir: PathBuf,
    pub index_dir: PathBuf,
    pub duckdb_path: PathBuf,
    pub provider_index_dir: PathBuf,
    pub hcpcs_index_dir: PathBuf,
    pub meta_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SourceFiles {
    pub spending: PathBuf,
    pub npi: PathBuf,
    pub hcpcs: PathBuf,
}

impl StoragePaths {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir: PathBuf = data_dir.into();
        let source_dir = data_dir.join("source");
        let geo_dir = data_dir.join("geo");
        let index_dir = data_dir.join("index");
        let duckdb_path = data_dir.join("site.duckdb");
        let provider_index_dir = index_dir.join("providers");
        let hcpcs_index_dir = index_dir.join("hcpcs");
        let meta_path = data_dir.join("meta.json");

        Self {
            source_dir,
            geo_dir,
            index_dir,
            duckdb_path,
            provider_index_dir,
            hcpcs_index_dir,
            meta_path,
        }
    }

    pub fn source_files(&self) -> SourceFiles {
        SourceFiles {
            spending: self.source_dir.join("spending.parquet"),
            npi: self.source_dir.join("npi.parquet"),
            hcpcs: self.source_dir.join("hcpcs.parquet"),
        }
    }

    pub fn geonames_us_txt(&self) -> PathBuf {
        self.geo_dir.join("US.txt")
    }

    pub fn geonames_us_zip(&self) -> PathBuf {
        self.geo_dir.join("US.zip")
    }

    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.source_dir)?;
        std::fs::create_dir_all(&self.geo_dir)?;
        std::fs::create_dir_all(&self.index_dir)?;
        Ok(())
    }
}

pub fn file_present_nonempty(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(m) => m.is_file() && m.len() > 0,
        Err(_) => false,
    }
}
