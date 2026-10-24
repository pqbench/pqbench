//! Static HTML: the embedded bytemass columns are drawn as a d3 treemap.

use crate::v1;
use crate::viz::{ColumnRow, Error};

/// Self-contained page that embeds the columns and blobs and draws a d3 treemap.
///
/// # Errors
/// Fails when there are no columns or the columns cannot be encoded into the page.
pub fn render_html(rows: &[ColumnRow], blobs: &[v1::Blob], title: &str) -> Result<String, Error> {
    if rows.is_empty() {
        return Err(Error::Empty);
    }
    let title = html_escape(title);
    let columns = encode(rows)?;
    let files = encode(blobs)?;
    let mut out = String::new();
    out.push_str("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>");
    out.push_str(&title);
    out.push_str("</title>\n<style>\n");
    out.push_str("body { font-family: sans-serif; margin: 0; background: #eeece9; }\n");
    out.push_str(
        "#title { text-align: center; font-size: 26px; padding: 12px; color: #332f2a; }\n",
    );
    out.push_str("section { padding: 0 16px 16px; }\n");
    out.push_str("h2 { font-size: 16px; color: #332f2a; margin: 8px 0; }\n");
    out.push_str("svg { display: block; }\n");
    out.push_str(".cell { stroke: #a9a49d; stroke-width: 0.8px; }\n");
    out.push_str(".cell:hover { stroke: #332f2a; stroke-width: 1.6px; }\n");
    out.push_str(".label { fill: #332f2a; font-size: 13px; pointer-events: none; }\n");
    out.push_str("</style>\n</head>\n<body>\n<div id=\"title\">");
    out.push_str(&title);
    out.push_str("</div>\n<main id=\"maps\"></main>\n");
    out.push_str("<script type=\"module\">\n");
    out.push_str(
        "import {hierarchy, treemap} from \"https://cdn.jsdelivr.net/npm/d3-hierarchy@3/+esm\";\n",
    );
    out.push_str("import {scaleLinear} from \"https://cdn.jsdelivr.net/npm/d3-scale@4/+esm\";\n");
    out.push_str("import {select} from \"https://cdn.jsdelivr.net/npm/d3-selection@3/+esm\";\n");
    out.push_str("const TITLE = ");
    out.push_str(&encode(&title)?);
    out.push_str(";\nconst COLUMNS = ");
    out.push_str(&columns);
    out.push_str(";\nconst FILES = ");
    out.push_str(&files);
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
    const values = Math.max(column.value_count, 1);
    insert(root, column.column.split("."), column.compressed_bytes / values);
  }
  recompute(root);
  return root;
}
function fileTree(name, files) {
  const root = branch(name);
  for (const file of files) {
    let leaf = String(file.name).split("/").pop() || file.name;
    if (file.storage_class) leaf += ` [${file.storage_class}]`;
    insert(root, [leaf], file.size_bytes || 0);
  }
  recompute(root);
  return root;
}
function draw(svg, data, unit) {
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
  cell.append("title").text(d => `${d.data.name}: ${fmt(d.value)} ${unit}`);
  cell.filter(d => (d.x1 - d.x0) > 36 && (d.y1 - d.y0) > 16).append("text")
    .selectAll("tspan").data(d => [d.data.name, fmt(d.value) + " " + unit]).join("tspan")
    .attr("x", 4).attr("y", (d, i) => 15 + i * 14).attr("class", "label").text(d => d);
}
const maps = document.getElementById("maps");
const section = document.createElement("section");
const heading = document.createElement("h2");
heading.textContent = TITLE;
const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
section.appendChild(heading);
section.appendChild(svg);
maps.appendChild(section);
draw(select(svg), tree(TITLE, COLUMNS), "bpr");
if (FILES.length > 0) {
  const fileHeading = document.createElement("h2");
  fileHeading.textContent = TITLE + " files";
  const fileSvg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  section.appendChild(fileHeading);
  section.appendChild(fileSvg);
  draw(select(fileSvg), fileTree(TITLE, FILES), "bytes");
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
