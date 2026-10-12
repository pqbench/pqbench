//! `credentials get`: put vended read credentials on each table-ref:
//! `list tables | credentials get`.

use std::collections::BTreeMap;

use futures_util::stream::StreamExt;
use serde::Serialize;
use serde_json::Value;

use super::StageArgs;
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input, table_ref, vend};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let input = read_input("credentials get").await?;
    if !input.piped {
        return Err("credentials get reads table refs on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let records = source::records("credentials get", input.first, input.lines);
    let mut tables = 0;
    let mut vends = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("credentials get", &record)?;
            let credentials = vend(workspace, &catalog, &schema, &name).await?;
            Ok::<_, CliError>((record, credentials))
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(result) = vends.next().await {
        let (record, credentials) = result?;
        let vended = credentials.is_some();
        emit.write_row(&VendedRef {
            record: attach_credentials(record, credentials),
            vended,
        })
        .await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

/// The ref with the vended lease written onto it as its `credentials` value;
/// the older `env` bag, if any, is dropped, so a table's lease lives in one
/// place. A table the catalog vends nothing for is passed through unchanged.
fn attach_credentials(mut record: Value, env: Option<BTreeMap<String, String>>) -> Value {
    let Some(env) = env else {
        return record;
    };
    let credentials = pqbench::v1::Credentials::from_env(&env);
    if credentials.is_empty() {
        return record;
    }
    let Some(object) = record.as_object_mut() else {
        return record;
    };
    object.remove("env");
    object.insert(
        "credentials".to_string(),
        serde_json::to_value(credentials).unwrap_or(Value::Null),
    );
    record
}

/// The row `credentials get` writes: the ref, plus whether creds were vended.
/// The serialized form is the ref itself.
struct VendedRef {
    record: Value,
    vended: bool,
}

impl Serialize for VendedRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.record.serialize(serializer)
    }
}

impl Row for VendedRef {
    const HEADER: &'static [&'static str] = &["name", "credentials"];
    const ALIGN: &[Align] = &[Align::Left, Align::Left];

    fn cells(&self) -> Vec<String> {
        let name = self.record["name"]
            .as_str()
            .or_else(|| self.record["id"].as_str())
            .unwrap_or_default();
        vec![
            name.to_string(),
            if self.vended { "vended" } else { "-" }.to_string(),
        ]
    }
}
