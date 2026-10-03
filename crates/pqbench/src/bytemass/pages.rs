//! Optional bounded page-header scanning, independent of page indexes.
use crate::third_party::{object_store, parquet};
use parquet::api::{Error, PageHeader};
use serde::Serialize;
use std::collections::BTreeMap;

/// One page in physical file order within a column chunk.
#[derive(Debug, Clone, Serialize)]
pub struct PageRecord {
    pub file: String,
    pub column: String,
    pub row_group: u32,
    pub page: u64,
    /// Absolute byte offset of the page header.
    pub offset: u64,
    #[serde(flatten)]
    pub header: PageHeader,
}

/// Read page headers using bounded ranges and skip compressed payloads.
///
/// Read-ahead may fetch up to 4 KiB of payload along with a small header, but
/// no values are decoded. Large headers are limited to 1 MiB. A v1 page's
/// value count is not its row count for repeated columns.
///
/// # Errors
/// Invalid chunk/header bounds, unsupported encryption, or storage errors.
pub async fn scan_pages(
    input: &str,
    env: &BTreeMap<String, String>,
) -> Result<Vec<PageRecord>, Error> {
    let uri = if input.contains("://") {
        input.to_owned()
    } else {
        let path = std::fs::canonicalize(input).map_err(|e| Error(e.to_string()))?;
        url::Url::from_file_path(path)
            .map_err(|_| Error("invalid file path".into()))?
            .to_string()
    };
    let options: Vec<_> = env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let (stat, mass) = super::remote::read_remote(&uri, options.clone(), false).await?;
    let reader = object_store::open(&uri, &options).map_err(|e| Error(e.to_string()))?;
    let mut pages = Vec::new();
    for column in mass.columns {
        let mut offset = column.chunk_offset;
        let end = offset
            .checked_add(column.compressed_bytes)
            .filter(|end| *end <= stat.size_bytes.saturating_sub(8))
            .ok_or_else(|| Error("column chunk outside file".into()))?;
        let mut page = 0;
        while offset < end {
            let mut window = 4096u64.min(end - offset);
            let header = loop {
                let bytes = reader
                    .read_range(offset..offset + window, stat.identity.as_deref())
                    .await
                    .map_err(|e| Error(e.to_string()))?;
                match parquet::api::read_page_header(&bytes) {
                    Ok(header) => break header,
                    Err(error) if window >= 1_048_576 || window == end - offset => {
                        return Err(Error(format!(
                            "{} {} at {offset}: {error}",
                            input, column.column
                        )))
                    }
                    Err(_) => window = (window * 2).min(1_048_576).min(end - offset),
                }
            };
            let next = offset
                .checked_add(header.header_bytes)
                .and_then(|n| n.checked_add(header.compressed_bytes))
                .filter(|next| *next > offset && *next <= end)
                .ok_or_else(|| Error("page outside column chunk".into()))?;
            pages.push(PageRecord {
                file: input.to_owned(),
                column: column.column.clone(),
                row_group: column.row_group,
                page,
                offset,
                header,
            });
            offset = next;
            page += 1;
        }
    }
    Ok(pages)
}
