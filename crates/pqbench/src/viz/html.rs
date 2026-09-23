//! Static HTML: sql.js opens the embedded SQLite file; d3 draws the treemap.

use crate::parquet_helpers::Error;

const SQL_JS: &str = "https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.12.0";

/// Self-contained page that queries `sqlite` in the browser and draws d3.
///
/// # Errors
/// Fails when the SQLite bytes cannot be encoded into the page.
pub fn render_html(sqlite: &[u8], title: &str) -> Result<String, Error> {
    if sqlite.len() < 16 || !sqlite.starts_with(b"SQLite format 3") {
        return Err(Error("html needs a SQLite database".into()));
    }
    let title = html_escape(title);
    let payload = base64_encode(sqlite);
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
    out.push_str("</style>\n<script src=\"");
    out.push_str(SQL_JS);
    out.push_str("/sql-wasm.min.js\"></script>\n</head>\n<body>\n<div id=\"title\">");
    out.push_str(&title);
    out.push_str("</div>\n<nav id=\"tables\"></nav>\n<main id=\"maps\"></main>\n");
    out.push_str("<script type=\"module\">\n");
    out.push_str(
        "import {hierarchy, treemap} from \"https://cdn.jsdelivr.net/npm/d3-hierarchy@3/+esm\";\n",
    );
    out.push_str("import {scaleLinear} from \"https://cdn.jsdelivr.net/npm/d3-scale@4/+esm\";\n");
    out.push_str("import {select} from \"https://cdn.jsdelivr.net/npm/d3-selection@3/+esm\";\n");
    out.push_str("const SQL_JS = \"");
    out.push_str(SQL_JS);
    out.push_str("\";\nconst SQLITE_B64 = \"");
    out.push_str(&payload);
    out.push_str("\";\n");
    out.push_str(PAGE_SCRIPT);
    out.push_str("</script>\n</body>\n</html>\n");
    Ok(out)
}

const PAGE_SCRIPT: &str = r##"
function b64(text) {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}
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
    const rows = Math.max(column.num_rows, 1);
    insert(root, column.column_path.split("."), column.compressed_bytes / rows);
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
const SQL = await initSqlJs({ locateFile: file => `${SQL_JS}/${file}` });
const db = new SQL.Database(b64(SQLITE_B64));
const ids = db.exec("SELECT DISTINCT id FROM masses ORDER BY id")[0].values.map(row => row[0]);
const nav = document.getElementById("tables");
const maps = document.getElementById("maps");
ids.forEach((id, index) => {
  const stmt = db.prepare(
    "SELECT column_path, SUM(compressed_bytes) AS compressed_bytes, MAX(num_rows) AS num_rows FROM masses WHERE id = ? GROUP BY column_path"
  );
  stmt.bind([id]);
  const columns = [];
  while (stmt.step()) columns.push(stmt.getAsObject());
  stmt.free();
  const name = id || document.getElementById("title").textContent;
  if (ids.length > 1) {
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
  draw(select(svg), tree(name, columns));
});
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

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let n = ((a as u32) << 16) | ((b as u32) << 8) | c as u32;
        out.push(TABLE[(n >> 18) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
