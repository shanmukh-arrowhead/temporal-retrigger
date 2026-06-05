use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct CallRecord {
    id: String,
}

pub fn read_call_ids(path: &Path) -> Result<Vec<String>> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("Failed to open CSV: {}", path.display()))?;

    let mut ids = Vec::new();
    for result in reader.deserialize() {
        let record: CallRecord = result.context("Failed to parse CSV row")?;
        ids.push(record.id);
    }

    Ok(ids)
}
