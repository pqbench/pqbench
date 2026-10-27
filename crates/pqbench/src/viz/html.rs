//! Static HTML: one treemap level at a time, clicked down the hierarchy.
//!
//! The page is one file with inline styles and data; it draws with the three
//! d3 modules it imports from a CDN, so it opens without a server. A table's
//! files are counted, not drawn: a table can carry thousands of them.

use crate::viz::{Error, Report};

/// Self-contained page that embeds the report and draws it as clickable
/// treemaps: catalogs, a catalog's schemas, a schema's tables, and a table's
/// columns.
///
/// # Errors
/// Fails when the report holds no catalogs or cannot be encoded into the page.
pub fn render_html(report: &Report, title: &str) -> Result<String, Error> {
    if report.catalogs.is_empty() {
        return Err(Error::Empty);
    }
    let title = html_escape(title);
    let data = encode(report)?;
    let mut out = String::new();
    out.push_str("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>");
    out.push_str(&title);
    out.push_str("</title>\n<style>\n");
    out.push_str("body { font-family: sans-serif; margin: 0; background: #eeece9; }\n");
    out.push_str(
        "#title { text-align: center; font-size: 26px; padding: 12px; color: #332f2a; }\n",
    );
    out.push_str("#crumbs { padding: 0 16px 12px; color: #6b655c; }\n");
    out.push_str("#crumbs a { color: #1f5e2a; }\n");
    out.push_str("#crumbs span { margin: 0 6px; }\n");
    out.push_str("section { padding: 0 16px 16px; }\n");
    out.push_str("h2 { font-size: 16px; color: #332f2a; margin: 8px 0; }\n");
    out.push_str("svg { display: block; }\n");
    out.push_str(".cell rect { stroke: #a9a49d; stroke-width: 0.8px; }\n");
    out.push_str(".cell:hover rect { stroke: #332f2a; stroke-width: 1.6px; }\n");
    out.push_str(".label { fill: #332f2a; font-size: 13px; pointer-events: none; }\n");
    out.push_str("</style>\n</head>\n<body>\n<div id=\"title\">");
    out.push_str(&title);
    out.push_str("</div>\n<nav id=\"crumbs\"></nav>\n<main>\n");
    out.push_str("<section><h2 id=\"heading\"></h2><svg id=\"map\"></svg></section>\n</main>\n");
    out.push_str("<script type=\"module\">\n");
    out.push_str(
        "import {hierarchy, treemap} from \"https://cdn.jsdelivr.net/npm/d3-hierarchy@3/+esm\";\n",
    );
    out.push_str("import {scaleLinear} from \"https://cdn.jsdelivr.net/npm/d3-scale@4/+esm\";\n");
    out.push_str("import {select} from \"https://cdn.jsdelivr.net/npm/d3-selection@3/+esm\";\n");
    out.push_str("const REPORT = ");
    out.push_str(&data);
    out.push_str(";\n");
    out.push_str(PAGE_SCRIPT);
    out.push_str("</script>\n</body>\n</html>\n");
    Ok(out)
}

/// JSON for the page, with `<` escaped so the value cannot close the script.
fn encode(value: &(impl serde::Serialize + ?Sized)) -> Result<String, Error> {
    Ok(serde_json::to_string(value)
        .map_err(Error::Render)?
        .replace('<', "\\u003c"))
}

const PAGE_SCRIPT: &str = r##"
function dollars(value) {
  if (value >= 1000) return "$" + (value / 1000).toFixed(1) + "k";
  if (value >= 1) return "$" + value.toFixed(2);
  if (value >= 0.01) return "$" + value.toFixed(3);
  return "$" + value.toFixed(4);
}
function money(cost) {
  if (cost == null) return "";
  const first = dollars(cost.first), last = dollars(cost.last);
  return first === last ? first : first + "–" + last;
}
function costNote(node) {
  return node.storage_cost_per_month == null ? "" : " · " + money(node.storage_cost_per_month) + "/mo";
}
function humanBytes(value) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = value, unit = 0;
  while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit += 1; }
  return (unit === 0 ? size.toFixed(0) : size.toFixed(1)) + " " + units[unit];
}
function readPath() {
  const raw = window.location.hash.slice(1);
  if (!raw) return [];
  const path = [];
  for (const part of raw.split("/")) {
    if (!/^\d+$/.test(part)) return [];
    path.push(Number(part));
  }
  return path;
}
function locate(want) {
  const catalog = REPORT.catalogs[want[0]];
  if (!catalog) return { path: [] };
  if (want.length === 1) return { path: [want[0]], catalog };
  const schema = catalog.schemas[want[1]];
  if (!schema) return { path: [want[0]], catalog };
  if (want.length === 2) return { path: want.slice(0, 2), catalog, schema };
  const table = schema.tables[want[2]];
  if (!table) return { path: want.slice(0, 2), catalog, schema };
  return { path: want.slice(0, 3), catalog, schema, table };
}
function crumbs(at) {
  const nav = document.getElementById("crumbs");
  nav.replaceChildren();
  const items = [{ label: "catalogs", href: "#" }];
  if (at.catalog) items.push({ label: at.catalog.display_name, href: "#" + at.path[0] });
  if (at.schema) {
    items.push({ label: at.schema.display_name, href: "#" + at.path.slice(0, 2).join("/") });
  }
  if (at.table) items.push({ label: at.table.display_name, href: "#" + at.path.join("/") });
  items.forEach((item, index) => {
    if (index > 0) {
      const separator = document.createElement("span");
      separator.textContent = "›";
      nav.append(separator);
    }
    if (index === items.length - 1) {
      const current = document.createElement("span");
      current.textContent = item.label;
      nav.append(current);
    } else {
      const link = document.createElement("a");
      link.href = item.href;
      link.textContent = item.label;
      nav.append(link);
    }
  });
}
function count(number, singular, plural) {
  return number + " " + (number === 1 ? singular : plural);
}
function ratioNote(node) {
  return node.compression_ratio == null ? "" : " · " + node.compression_ratio.toFixed(1) + "×";
}
function detail(node, depth) {
  const head = humanBytes(node.compressed_bytes) + costNote(node);
  switch (depth) {
    case 0: return head + " · " + count(node.schemas.length, "schema", "schemas");
    case 1: return head + " · " + count(node.tables.length, "table", "tables");
    case 2: return head + " · " + count(node.columns.length, "column", "columns");
    default: return head + " · " + node.bytes_per_value.toFixed(1) + " B/row" + ratioNote(node);
  }
}
function describe(node, depth) {
  const head = humanBytes(node.compressed_bytes) + costNote(node);
  switch (depth) {
    case 0: return node.display_name + " — " + head + " over " +
      count(node.schemas.reduce((sum, schema) => sum + schema.tables.length, 0), "table", "tables");
    case 1: return node.display_name + " — " + head + " over " +
      count(node.tables.length, "table", "tables");
    case 2: return node.display_name + " — " + head + " over " +
      count(node.columns.length, "column", "columns") + " in " +
      count(node.file_count, "file", "files") +
      (node.storage_classes.length ? " [" + node.storage_classes.join(", ") + "]" : "");
    default: return node.column + " — " + head + " (" +
      node.bytes_per_value.toFixed(1) + " bytes per row" + ratioNote(node) + ")";
  }
}
function draw(element, nodes, depth, path) {
  const width = 1600, height = 460;
  const svg = select(element).attr("width", width).attr("height", height);
  svg.selectAll("*").remove();
  const items = nodes.map((node, index) => ({
    index,
    node,
    name: depth === 3 ? node.column : node.display_name,
    value: node.compressed_bytes,
    href: depth < 3 ? "#" + path.concat(index).join("/") : null,
  }));
  const root = hierarchy({ children: items }).sum(item => item.children ? 0 : item.value);
  treemap().size([width, height]).paddingInner(1).paddingOuter(0)(root);
  const color = scaleLinear().domain([0, Math.max(root.value, 1)]).range(["#eef3ea", "#1f5e2a"]);
  const cell = svg.selectAll("g.cell").data(root.leaves()).join("g").attr("class", "cell")
    .attr("transform", item => `translate(${item.x0},${item.y0})`);
  cell.append("rect")
    .attr("width", item => Math.max(item.x1 - item.x0, 0))
    .attr("height", item => Math.max(item.y1 - item.y0, 0))
    .attr("fill", item => color(item.value));
  cell.append("title").text(item => describe(item.data.node, depth));
  cell.filter(item => (item.x1 - item.x0) > 60 && (item.y1 - item.y0) > 24)
    .append("text")
    .selectAll("tspan")
    .data(item => [item.data.name, detail(item.data.node, depth)])
    .join("tspan")
    .attr("x", 4)
    .attr("y", (text, index) => 15 + index * 14)
    .attr("class", "label")
    .text(text => text);
  cell.filter(item => item.data.href !== null)
    .style("cursor", "pointer")
    .on("click", (event, item) => { window.location.hash = item.data.href; });
}
function render() {
  const at = locate(readPath());
  const depth = at.path.length;
  const nodes = depth === 0 ? REPORT.catalogs
    : depth === 1 ? at.catalog.schemas
    : depth === 2 ? at.schema.tables
    : at.table.columns;
  crumbs(at);
  const heading = document.getElementById("heading");
  heading.textContent =
    depth === 0 ? `Catalogs (${nodes.length})`
    : depth === 1 ? `Schemas in ${at.catalog.display_name} (${nodes.length})`
    : depth === 2 ? `Tables in ${at.catalog.display_name}.${at.schema.display_name} (${nodes.length})`
    : `Columns in ${at.table.display_name} (${nodes.length})`;
  draw(document.getElementById("map"), nodes, depth, at.path);
}
window.addEventListener("hashchange", render);
render();
"##;

fn html_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
    out
}
