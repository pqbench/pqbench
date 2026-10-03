use crate::CliError;
use clap::Args;
use pqbench::table::{FileSelection, TableInfo};

#[derive(Args, Default)]
pub(crate) struct FileSelectionArgs {
    /// Keep active files matching a table-relative path glob (repeatable).
    #[arg(long)]
    include: Vec<String>,
    /// Exclude active files matching a table-relative path glob (repeatable).
    #[arg(long)]
    exclude: Vec<String>,
    /// Match an exact partition value: COLUMN=VALUE (repeatable).
    #[arg(long = "partition", value_name = "COLUMN=VALUE")]
    partitions: Vec<String>,
    /// After filtering: all, first:N, every:N, or median:N (per table).
    #[arg(long, default_value = "all")]
    sample: String,
}
impl FileSelectionArgs {
    pub(crate) fn parse(&self) -> Result<FileSelection, CliError> {
        let mut partitions = std::collections::BTreeMap::new();
        for value in &self.partitions {
            let (key, value) = value
                .split_once('=')
                .filter(|(key, _)| !key.is_empty())
                .ok_or("partition must be COLUMN=VALUE")?;
            if partitions
                .insert(key.to_owned(), Some(value.to_owned()))
                .is_some()
            {
                return Err(format!("duplicate partition selector: {key}").into());
            }
        }
        let selection = FileSelection {
            include: self.include.clone(),
            exclude: self.exclude.clone(),
            partitions,
            sample: self.sample.clone(),
        };
        selection.select_files(Vec::new())?;
        Ok(selection)
    }
}

/// Apply `selection` to one loaded table, recomputing its partition totals.
pub(crate) fn apply(selection: &FileSelection, info: &mut TableInfo) -> Result<(), CliError> {
    if selection.unrestricted() {
        return Ok(());
    }
    info.files = selection.select_files(std::mem::take(&mut info.files))?;
    info.partitions = pqbench::table::partition_masses(&info.files)?;
    info.file_selection = Some(selection.clone());
    Ok(())
}
