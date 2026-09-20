//! Presentation: export a bytemass [`MassNode`] treemap as a self-contained
//! HTML page rendered by [d3.treemap](https://d3js.org) in the browser.
//!
//! Per Unix philosophy this layer folds the rows into a treemap hierarchy (via
//! [`super::aggregate::tree`]) and wraps its JSON in a thin page; d3 owns the
//! squarified layout and drawing. The page imports only the d3 modules it uses
//! (hierarchy, scale, selection) from a CDN — not the whole d3 bundle — and
//! embeds the tree as JSON, so it needs no installs, no server, and no other
//! setup: just open it.

use super::aggregate::{label, tree};
use super::api::MassRow;
use crate::parquet_helpers::Error;

/// The d3 treemap page, as a self-contained HTML string.
///
/// # Errors
/// Returns [`Error`] if aggregation overflows or the tree cannot be serialized.
pub fn render_html(rows: &[MassRow]) -> Result<String, Error> {
    let title = html_escape(&label(rows));
    let data = serde_json::to_string_pretty(&tree(rows)?)?.replace('<', "\\u003c");
    let mut out = head(&title);
    out.push_str(&treemap_script(&data));
    out.push_str("</body>\n</html>\n");
    Ok(out)
}

/// The page's `<script>`: the d3 treemap over the embedded `data`.
fn treemap_script(data: &str) -> String {
    let mut out = String::new();
    out.push_str("<script type=\"module\">\n");
    out.push_str(
        "import {hierarchy, treemap} from \"https://cdn.jsdelivr.net/npm/d3-hierarchy@3/+esm\";\n",
    );
    out.push_str("import {scaleLinear} from \"https://cdn.jsdelivr.net/npm/d3-scale@4/+esm\";\n");
    out.push_str("import {select} from \"https://cdn.jsdelivr.net/npm/d3-selection@3/+esm\";\n");
    out.push_str("const data = ");
    out.push_str(data);
    out.push_str(";\n");
    out.push_str("const width = 1600, height = 900;\n");
    out.push_str(
        "const svg = select('body').append('svg').attr('width', width).attr('height', height);\n",
    );
    out.push_str("const root = hierarchy(data).sum(d => d.children ? 0 : d.value);\n");
    out.push_str("treemap().size([width, height]).paddingInner(1).paddingOuter(0)(root);\n");
    out.push_str(
        "const color = scaleLinear().domain([0, root.value]).range(['#eef3ea', '#1f5e2a']);\n",
    );
    out.push_str("const fmt = d => d.toFixed(1);\n");
    out.push_str("const cell = svg.selectAll('g').data(root.leaves()).join('g').attr('transform', d => `translate(${d.x0},${d.y0})`);\n");
    out.push_str("cell.append('rect').attr('width', d => d.x1 - d.x0).attr('height', d => d.y1 - d.y0).attr('fill', d => color(d.value)).attr('class', 'cell');\n");
    out.push_str(
        "cell.append('title').text(d => `${d.data.name}: ${fmt(d.value)} bytes per row`);\n",
    );
    out.push_str("cell.filter(d => (d.x1 - d.x0) > 36 && (d.y1 - d.y0) > 16).append('text').selectAll('tspan').data(d => [d.data.name, fmt(d.value) + ' bpr']).join('tspan').attr('x', 4).attr('y', (d, i) => 15 + i * 14).attr('class', 'label').text(d => d);\n");
    out.push_str("</script>\n");
    out
}

/// The static page head: doctype, `<title>`, style, and the title bar.
fn head(title: &str) -> String {
    let mut out = String::new();
    out.push_str("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n");
    out.push_str("<title>");
    out.push_str(title);
    out.push_str("</title>\n<style>\n");
    out.push_str("body { font-family: sans-serif; margin: 0; background: #eeece9; }\n");
    out.push_str(
        "#title { text-align: center; font-size: 26px; padding: 12px; color: #332f2a; }\n",
    );
    out.push_str("svg { display: block; }\n");
    out.push_str(".cell { stroke: #a9a49d; stroke-width: 0.8px; }\n");
    out.push_str(".cell:hover { stroke: #332f2a; stroke-width: 1.6px; }\n");
    out.push_str(".label { fill: #332f2a; font-size: 13px; pointer-events: none; }\n");
    out.push_str("</style>\n</head>\n<body>\n");
    out.push_str("<div id=\"title\">");
    out.push_str(title);
    out.push_str("</div>\n");
    out
}

/// Escape `&`, `<`, `>`, `"` for HTML text.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytemass::api::MassRow;

    fn rows(columns: &[(&str, u64)]) -> Vec<MassRow> {
        columns
            .iter()
            .map(|(column, bytes)| MassRow {
                file: "sample.parquet".into(),
                size: 0,
                num_rows: 1,
                column: (*column).into(),
                compressed_bytes: *bytes,
                uncompressed_bytes: *bytes,
                codec: "SNAPPY".into(),
            })
            .collect()
    }

    #[test]
    fn render_html_is_self_contained() {
        let html = render_html(&rows(&[("text", 2), ("nums", 1)])).unwrap();
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("type=\"module\""));
        assert!(html.contains("d3-hierarchy@3"));
        assert!(html.contains("d3-scale@4"));
        assert!(html.contains("d3-selection@3"));
        assert!(html.contains("<div id=\"title\">sample.parquet</div>"));
        assert!(html.contains("\"name\": \"sample.parquet\""));
        assert!(html.contains("\"name\": \"text\""));
        assert!(html.contains("bytes per row"));
        assert!(html.contains(" bpr"));
    }

    #[test]
    fn html_escape_is_safe() {
        assert_eq!(html_escape("a<b>&\"c"), "a&lt;b&gt;&amp;&quot;c");
    }
}
