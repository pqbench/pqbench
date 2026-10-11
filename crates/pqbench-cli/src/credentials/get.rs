//! `credentials get`: put vended read credentials on each table-ref:
//! `schema ls | credentials get`.

use std::collections::BTreeMap;

use futures_util::stream::StreamExt;
use serde::Serialize;
use serde_json::Value;

use super::StageArgs;
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input, ref_env, table_ref, vend};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let input = read_input("credentials get").await?;
    if !input.piped {
        return Err("credentials get reads pqbench.table-ref v2 refs on standard input".into());
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
            record: add_env(record, credentials),
            vended,
        })
        .await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

/// The table's env: the ref's own, then the vended credentials. The vended keys
/// win, so a table-scoped lease replaces any static key for that table.
fn table_env(
    record: &Value,
    credentials: Option<BTreeMap<String, String>>,
) -> BTreeMap<String, String> {
    let mut env = ref_env(record);
    if let Some(credentials) = credentials {
        env.extend(credentials);
    }
    env
}

/// The ref's `env` with the vended credentials written over it.
fn add_env(mut record: Value, credentials: Option<BTreeMap<String, String>>) -> Value {
    let Some(credentials) = credentials else {
        return record;
    };
    let env = table_env(&record, Some(credentials));
    let Some(object) = record.as_object_mut() else {
        return record;
    };
    let mut values = serde_json::Map::new();
    for (key, value) in env {
        values.insert(key, Value::String(value));
    }
    object.insert("env".to_string(), Value::Object(values));
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
        vec![
            self.record["id"].as_str().unwrap_or_default().to_string(),
            if self.vended { "vended" } else { "-" }.to_string(),
        ]
    }
}
