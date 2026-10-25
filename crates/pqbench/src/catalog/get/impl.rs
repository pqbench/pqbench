//! The Unity REST call behind [`super::api::read`].
//!
//! The URL and the JSON shape are this command's; the transport is the
//! third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Catalog, Error};
use crate::dialect;
use crate::v1::name;
use crate::Workspace;

/// The subset of `GET /catalogs/{name}` this command reports.
#[derive(Deserialize)]
struct Record {
    name: String,
    #[serde(default)]
    catalog_type: Option<String>,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    owner: Option<String>,
}

pub(crate) async fn get(workspace: &Workspace, name: &str) -> Result<Catalog, Error> {
    let catalog = Catalog::parse_name(name)
        .ok_or_else(|| Error::Name(format!("{name} is not a catalogs/… name")))?;
    let url = format!(
        "{}/catalogs/{}",
        dialect::api_root(workspace.config().endpoint.as_str()),
        catalog
    );
    let record: Record =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
    Ok(Catalog {
        name: name::catalog(&record.name),
        catalog_type: record
            .catalog_type
            .as_deref()
            .and_then(dialect::catalog_type),
        comment: record.comment.filter(|comment| !comment.is_empty()),
        owner: record.owner.filter(|owner| !owner.is_empty()),
        ..Catalog::default()
    })
}
