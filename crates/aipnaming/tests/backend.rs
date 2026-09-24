//! Blackbox tests for the tree-sitter backend: only the public API is used, so
//! these tests can fail when extraction behavior is wrong, not just when the
//! implementation drifts.

use aipnaming::decl::DeclKind;
use aipnaming::rust::parse_source;

fn find<'a>(
    decls: &'a [aipnaming::decl::Declaration],
    name: &str,
) -> &'a aipnaming::decl::Declaration {
    decls
        .iter()
        .find(|decl| decl.name == name)
        .unwrap_or_else(|| panic!("no declaration named {name}"))
}

#[test]
fn extracts_fields_with_owner_type_and_docs() {
    let source = r#"
/// One codec sweep row.
pub struct ReportRow {
    /// Compressed size in bytes.
    pub compressed_bytes: usize,
    pub json: bool,
    pub codec: Codec,
}
"#;
    let decls = parse_source(source);

    let row = find(&decls, "ReportRow");
    assert_eq!(row.kind, DeclKind::Struct);
    assert!(row.docs);

    let bytes = find(&decls, "compressed_bytes");
    assert_eq!(bytes.kind, DeclKind::Field);
    assert_eq!(bytes.owner.as_deref(), Some("ReportRow"));
    assert_eq!(bytes.type_name.as_deref(), Some("usize"));
    assert!(bytes.docs);

    let json = find(&decls, "json");
    assert!(json.is_bool());
    assert!(!json.docs);

    let codec = find(&decls, "codec");
    assert_eq!(codec.type_constructor_name(), Some("Codec"));
}

#[test]
fn classifies_functions_methods_and_associated_functions() {
    let source = r#"
fn bench_file(path: &str) -> u64 { 0 }

struct Codec;

impl Codec {
    pub fn from_name(name: &str) -> Option<Codec> { None }
    pub fn compressed_bytes(&self) -> usize { 0 }
}

trait Render {
    fn render(&self, json: bool) -> String;
}
"#;
    let decls = parse_source(source);

    let bench = find(&decls, "bench_file");
    assert_eq!(bench.kind, DeclKind::Function);
    assert_eq!(bench.owner, None);
    assert_eq!(bench.type_name.as_deref(), Some("u64"));

    let from_name = find(&decls, "from_name");
    assert_eq!(from_name.kind, DeclKind::AssociatedFunction);
    assert_eq!(from_name.owner.as_deref(), Some("Codec"));

    let compressed_bytes = find(&decls, "compressed_bytes");
    assert_eq!(compressed_bytes.kind, DeclKind::Method);
    assert_eq!(compressed_bytes.owner.as_deref(), Some("Codec"));

    let render = find(&decls, "render");
    assert_eq!(render.kind, DeclKind::Method);
    assert_eq!(render.owner.as_deref(), Some("Render"));
}

#[test]
fn extracts_enum_variants_in_source_order() {
    let source = r#"
enum Codec {
    Snappy,
    Zstd,
    Gzip,
}

enum Format {
    FORMAT_UNSPECIFIED,
    HARDBACK,
}
"#;
    let decls = parse_source(source);
    let variants: Vec<&str> = decls
        .iter()
        .filter(|decl| decl.kind == DeclKind::Variant)
        .map(|decl| decl.name.as_str())
        .collect();
    assert_eq!(
        variants,
        ["Snappy", "Zstd", "Gzip", "FORMAT_UNSPECIFIED", "HARDBACK"]
    );
    assert_eq!(find(&decls, "Snappy").owner.as_deref(), Some("Codec"));
}

#[test]
fn ignores_items_inside_strings_and_comments() {
    let source = r#"
const MESSAGE: &str = "struct Fake { pub is_json: bool }";

// struct AlsoFake { pub is_json: bool }
/* fn not_a_function() {} */

fn real() {}
"#;
    let decls = parse_source(source);
    let names: Vec<&str> = decls.iter().map(|decl| decl.name.as_str()).collect();
    assert_eq!(names, ["MESSAGE", "real"]);
}

#[test]
fn reports_byte_accurate_positions() {
    let source = "pub struct ReportRow {\n    pub total_bytes: u64,\n}\n";
    let decls = parse_source(source);
    let total = find(&decls, "total_bytes");
    assert_eq!(total.line, 2);
    assert_eq!(total.column, 9);
    assert_eq!(&source[total.name_range.clone()], "total_bytes");
}
