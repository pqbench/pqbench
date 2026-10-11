//! `metastore info`: the endpoint's metastore record.

use pqbench::metastore::info;
use serde::Serialize;

use super::OutputArgs;
use crate::emit::{Align, Emitter, Row};
use crate::CliError;

/// The document `metastore info` writes.
#[derive(Serialize)]
struct MetastoreRecord<'a> {
    kind: &'static str,
    version: u32,
    name: &'a str,
    id: &'a str,
    cloud: &'a str,
    region: &'a str,
}

impl Row for MetastoreRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "id", "cloud", "region"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.to_string(),
            self.id.to_string(),
            self.cloud.to_string(),
            self.region.to_string(),
        ]
    }
}

pub(super) async fn run_info(
    workspace: &pqbench::Workspace,
    args: &OutputArgs,
) -> Result<(), CliError> {
    let metastore = info::read(workspace).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    emit.write_row(&MetastoreRecord {
        kind: "pqbench.metastore",
        version: 1,
        name: &metastore.name,
        id: &metastore.id,
        cloud: &metastore.cloud,
        region: &metastore.region,
    })
    .await?;
    emit.finish("metastores: 1\n").await
}
