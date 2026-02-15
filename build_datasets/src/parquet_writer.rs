use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::{basic::Compression, file::properties::WriterProperties};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Streaming Parquet writer for "all-string" datasets.
///
/// We use this for large exports (e.g. NPPES-derived NPI rows) to avoid writing
/// huge intermediate CSVs.
pub struct StringParquetWriter {
    output_path: PathBuf,
    tmp_path: PathBuf,
    schema: Arc<Schema>,
    writer: ArrowWriter<File>,
    builders: Vec<StringBuilder>,
    rows_in_batch: usize,
    batch_size: usize,
}

impl StringParquetWriter {
    pub fn try_new(output_path: &Path, columns: &[&str], batch_size: usize) -> Result<Self> {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed creating {}", parent.display()))?;
        }

        let file_name = output_path
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("output.parquet");
        let tmp_path = output_path.with_file_name(format!("{file_name}.tmp"));

        let fields: Vec<Field> = columns
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(&tmp_path)
            .with_context(|| format!("Failed creating {}", tmp_path.display()))?;
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
            .context("Failed creating Parquet ArrowWriter")?;

        let builders = (0..columns.len()).map(|_| StringBuilder::new()).collect();

        Ok(Self {
            output_path: output_path.to_path_buf(),
            tmp_path,
            schema,
            writer,
            builders,
            rows_in_batch: 0,
            batch_size: batch_size.max(1),
        })
    }

    pub fn push_row(&mut self, values: &[Option<&str>]) -> Result<()> {
        anyhow::ensure!(
            values.len() == self.builders.len(),
            "push_row expected {} columns, got {}",
            self.builders.len(),
            values.len()
        );

        for (idx, value) in values.iter().enumerate() {
            match value {
                Some(v) => self.builders[idx].append_value(v),
                None => self.builders[idx].append_null(),
            }
        }
        self.rows_in_batch += 1;
        if self.rows_in_batch >= self.batch_size {
            self.flush_batch()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.flush_batch()?;
        self.writer
            .close()
            .context("Failed closing Parquet writer")?;
        fs::rename(&self.tmp_path, &self.output_path).with_context(|| {
            format!(
                "Failed moving temp parquet {} to {}",
                self.tmp_path.display(),
                self.output_path.display()
            )
        })?;
        Ok(())
    }

    pub fn abort(self) -> Result<()> {
        // Best-effort cleanup: don't replace the output parquet with a partial tmp file.
        let _ = self.writer.close();
        let _ = fs::remove_file(&self.tmp_path);
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.rows_in_batch == 0 {
            return Ok(());
        }

        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .context("Failed creating RecordBatch for Parquet write")?;
        self.writer
            .write(&batch)
            .context("Failed writing Parquet RecordBatch")?;
        self.rows_in_batch = 0;
        Ok(())
    }
}
