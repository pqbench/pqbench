//! Static HTML: the embedded bytemass rows are drawn as a d3 treemap.

use crate::bytemass::FileStat;
use crate::third_party::parquet::api::Error;
use crate::viz::MassRecord;

/// Self-contained page that groups the embedded rows and draws a d3 treemap.
///
/// # Errors
/// Fails when there are no rows or the rows cannot be encoded into the page.
pub fn render_html(rows: &[MassRecord], files: &[FileStat], title: &str) -> Result<String, Error> {
    if rows.is_empty() {
        return Err(Error("html needs bytemass rows".into()));
    }
    let title = html_escape(title);
    let data = serde_json::to_string(rows)
        .map_err(|error| Error(format!("cannot encode rows: {error}")))?
        .replace('<', "\\u003c");
    let files = serde_json::to_string(files)
        .map_err(|error| Error(format!("cannot encode files: {error}")))?
        .replace('<', "\\u003c");
    let mut out = String::new();
    out.push_str("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>");
    out.push_str(&title);
    out.push_str("</title>\n<style>\n");
    out.push_str("body { font-family: sans-serif; margin: 0; background: #eeece9; }\n");
    out.push_str(
        "#title { text-align: center; font-size: 26px; padding: 12px; color: #332f2a; }\n",
    );
    out.push_str("nav { display: flex; flex-wrap: wrap; gap: 8px; padding: 0 16px 8px; }\n");
    out.push_str("nav a { color: #1f5e2a; }\n");
    out.push_str("section { padding: 0 16px 16px; }\n");
    out.push_str("h2 { font-size: 16px; color: #332f2a; margin: 8px 0; }\n");
    out.push_str("svg { display: block; }\n");
    out.push_str(".cell { stroke: #a9a49d; stroke-width: 0.8px; }\n");
    out.push_str(".cell:hover { stroke: #332f2a; stroke-width: 1.6px; }\n");
    out.push_str(".label { fill: #332f2a; font-size: 13px; pointer-events: none; }\n");
    out.push_str("</style>\n</head>\n<body>\n<div id=\"title\">");
    out.push_str(&title);
    out.push_str("</div>\n<nav id=\"tables\"></nav>\n<main id=\"maps\"></main>\n");
    out.push_str("<script type=\"module\">\n");
    out.push_str(
        "import {hierarchy, treemap} from \"https://cdn.jsdelivr.net/npm/d3-hierarchy@3/+esm\";\n",
    );
    out.push_str("import {scaleLinear} from \"https://cdn.jsdelivr.net/npm/d3-scale@4/+esm\";\n");
    out.push_str("import {select} from \"https://cdn.jsdelivr.net/npm/d3-selection@3/+esm\";\n");
    out.push_str("const ROWS = ");
    out.push_str(&data);
    out.push_str(";\nconst FILES = ");
    out.push_str(&files);
    out.push_str(";\n");
    out.push_str(PAGE_SCRIPT);
    out.push_str("</script>\n</body>\n</html>\n");
    Ok(out)
}

const PAGE_SCRIPT: &str = r##"
function branch(name) { return { name, value: 0, children: [] }; }
function insert(node, parts, value) {
  if (parts.length === 0) { node.value = value; return; }
  let child = node.children.find(item => item.name === parts[0]);
  if (!child) { child = branch(parts[0]); node.children.push(child); }
  insert(child, parts.slice(1), value);
}
function recompute(node) {
  if (node.children.length === 0) return;
  for (const child of node.children) recompute(child);
  node.value = node.children.reduce((sum, child) => sum + child.value, 0);
}
function tree(name, columns) {
  const root = branch(name);
  for (const column of columns) {
    const rows = Math.max(column.row_count, 1);
    insert(root, column.column.split("."), column.compressed_bytes / rows);
  }
  recompute(root);
  return root;
}
function draw(svg, data) {
  const width = 1600, height = 420;
  svg.attr("width", width).attr("height", height);
  const root = hierarchy(data).sum(d => d.children && d.children.length ? 0 : d.value);
  treemap().size([width, height]).paddingInner(1).paddingOuter(0)(root);
  const color = scaleLinear().domain([0, root.value]).range(["#eef3ea", "#1f5e2a"]);
  const fmt = d => d.toFixed(1);
  const cell = svg.selectAll("g").data(root.leaves()).join("g")
    .attr("transform", d => `translate(${d.x0},${d.y0})`);
  cell.append("rect").attr("width", d => d.x1 - d.x0).attr("height", d => d.y1 - d.y0)
    .attr("fill", d => color(d.value)).attr("class", "cell");
  cell.append("title").text(d => `${d.data.name}: ${fmt(d.value)} bytes per row`);
  cell.filter(d => (d.x1 - d.x0) > 36 && (d.y1 - d.y0) > 16).append("text")
    .selectAll("tspan").data(d => [d.data.name, fmt(d.value) + " bpr"]).join("tspan")
    .attr("x", 4).attr("y", (d, i) => 15 + i * 14).attr("class", "label").text(d => d);
}
const groups = new Map();
for (const row of ROWS) {
  if (!groups.has(row.id)) groups.set(row.id, new Map());
  const columns = groups.get(row.id);
  const current = columns.get(row.column) || { compressed_bytes: 0, row_count: 0 };
  current.compressed_bytes += row.compressed_bytes;
  current.row_count = Math.max(current.row_count, row.row_count);
  columns.set(row.column, current);
}
const entries = [...groups.entries()];
const nav = document.getElementById("tables");
const maps = document.getElementById("maps");
entries.forEach(([id, columns], index) => {
  const name = id || document.getElementById("title").textContent;
  if (entries.length > 1) {
    const link = document.createElement("a");
    link.href = `#table-${index}`;
    link.textContent = name;
    nav.appendChild(link);
  }
  const section = document.createElement("section");
  section.id = `table-${index}`;
  const heading = document.createElement("h2");
  heading.textContent = name;
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.id = `map-${index}`;
  section.appendChild(heading);
  section.appendChild(svg);
  maps.appendChild(section);
  const rows = [...columns.entries()].map(([column, value]) => ({ column, ...value }));
  draw(select(svg), tree(name, rows));
  const fileRows = FILES.filter(file => (file.id || "") === id);
  if (fileRows.length === 0) return;
  const fileHeading = document.createElement("h2");
  fileHeading.textContent = name + " files";
  const fileSvg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  section.appendChild(fileHeading);
  section.appendChild(fileSvg);
  draw(select(fileSvg), fileTree(name, fileRows));
});
function fileTree(name, files) {
  const root = branch(name);
  for (const file of files) {
    const parts = [];
    const values = file.partition_values || {};
    for (const key of Object.keys(values)) parts.push(`${key}=${values[key] ?? "null"}`);
    let leaf = String(file.path || file.file).split("/").pop() || file.file;
    if (file.storage_class) leaf += ` [${file.storage_class}]`;
    parts.push(leaf);
    const stats = file.stats || {};
    const value = stats.bytes_per_row || file.size || 0;
    insert(root, parts, value);
  }
  recompute(root);
  return root;
}
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
