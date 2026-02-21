use anyhow::{anyhow, Result};
use tree_sitter::Language;

/// Metadata for a supported language: grammar + extraction query.
#[allow(dead_code)]
pub struct LanguageEntry {
    pub name: &'static str,
    pub language: Language,
    pub query_source: &'static str,
}

/// Symbol extracted from a tree-sitter match.
pub struct ExtractedSymbol {
    pub name: String,
    pub symbol_type: String,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
}

/// Map file extension to language name.
pub fn detect_language(path: &str) -> Option<&'static str> {
    let ext = path.rsplit('.').next()?;
    match ext {
        "rs" => Some("rust"),
        "py" | "pyi" => Some("python"),
        "ts" | "tsx" => Some("typescript"),
        "js" | "jsx" | "mjs" | "cjs" => Some("javascript"),
        "go" => Some("go"),
        "java" => Some("java"),
        "c" | "h" => Some("c"),
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "hh" => Some("cpp"),
        "kt" | "kts" => Some("kotlin"),
        _ => None,
    }
}

/// Get the tree-sitter Language + extraction query for a language name.
pub fn get_language_entry(lang: &str) -> Option<LanguageEntry> {
    match lang {
        "rust" => Some(LanguageEntry {
            name: "rust",
            language: tree_sitter_rust::LANGUAGE.into(),
            query_source: RUST_QUERY,
        }),
        "python" => Some(LanguageEntry {
            name: "python",
            language: tree_sitter_python::LANGUAGE.into(),
            query_source: PYTHON_QUERY,
        }),
        "typescript" => Some(LanguageEntry {
            name: "typescript",
            language: tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            query_source: TYPESCRIPT_QUERY,
        }),
        "javascript" => Some(LanguageEntry {
            name: "javascript",
            language: tree_sitter_javascript::LANGUAGE.into(),
            query_source: JAVASCRIPT_QUERY,
        }),
        "go" => Some(LanguageEntry {
            name: "go",
            language: tree_sitter_go::LANGUAGE.into(),
            query_source: GO_QUERY,
        }),
        "java" => Some(LanguageEntry {
            name: "java",
            language: tree_sitter_java::LANGUAGE.into(),
            query_source: JAVA_QUERY,
        }),
        "c" => Some(LanguageEntry {
            name: "c",
            language: tree_sitter_c::LANGUAGE.into(),
            query_source: C_QUERY,
        }),
        "cpp" => Some(LanguageEntry {
            name: "cpp",
            language: tree_sitter_cpp::LANGUAGE.into(),
            query_source: CPP_QUERY,
        }),
        "kotlin" => Some(LanguageEntry {
            name: "kotlin",
            language: tree_sitter_kotlin_ng::LANGUAGE.into(),
            query_source: KOTLIN_QUERY,
        }),
        _ => None,
    }
}

/// Extract symbols from source code using tree-sitter.
pub fn extract_symbols(source: &str, lang: &str) -> Result<Vec<ExtractedSymbol>> {
    let entry = get_language_entry(lang).ok_or_else(|| anyhow!("Unsupported language: {lang}"))?;

    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&entry.language)?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| anyhow!("tree-sitter parse failed for {lang}"))?;

    let query = tree_sitter::Query::new(&entry.language, entry.query_source)?;
    let mut cursor = tree_sitter::QueryCursor::new();
    let matches = cursor.matches(&query, tree.root_node(), source.as_bytes());

    let capture_names: Vec<&str> = query.capture_names().iter().map(|s| s.as_ref()).collect();

    let mut symbols = Vec::new();
    let source_bytes = source.as_bytes();

    for m in matches {
        let mut name = String::new();
        let mut symbol_type = String::new();
        let mut start_line = 0u32;
        let mut end_line = 0u32;
        let mut content_range: Option<(usize, usize)> = None;

        for cap in m.captures {
            let cap_name = capture_names[cap.index as usize];
            let node = cap.node;

            if cap_name == "symbol_name" {
                name = node.utf8_text(source_bytes).unwrap_or("").to_string();
            } else {
                // The outer capture — determines the symbol type and range
                symbol_type = cap_name.to_string();
                start_line = node.start_position().row as u32 + 1;
                end_line = node.end_position().row as u32 + 1;
                content_range = Some((node.start_byte(), node.end_byte()));
            }
        }

        if !name.is_empty() && !symbol_type.is_empty() {
            if let Some((start, end)) = content_range {
                let content = String::from_utf8_lossy(&source_bytes[start..end]).to_string();
                symbols.push(ExtractedSymbol {
                    name,
                    symbol_type,
                    start_line,
                    end_line,
                    content,
                });
            }
        }
    }

    Ok(symbols)
}

// ---------------------------------------------------------------------------
// Tree-sitter queries per language
// ---------------------------------------------------------------------------

const RUST_QUERY: &str = r#"
(function_item name: (identifier) @symbol_name) @function
(struct_item name: (type_identifier) @symbol_name) @struct
(enum_item name: (type_identifier) @symbol_name) @enum
(trait_item name: (type_identifier) @symbol_name) @trait
(impl_item type: (type_identifier) @symbol_name) @impl
(mod_item name: (identifier) @symbol_name) @module
"#;

const PYTHON_QUERY: &str = r#"
(function_definition name: (identifier) @symbol_name) @function
(class_definition name: (identifier) @symbol_name) @class
"#;

const TYPESCRIPT_QUERY: &str = r#"
(function_declaration name: (identifier) @symbol_name) @function
(class_declaration name: (type_identifier) @symbol_name) @class
(interface_declaration name: (type_identifier) @symbol_name) @interface
(type_alias_declaration name: (type_identifier) @symbol_name) @type_alias
"#;

const JAVASCRIPT_QUERY: &str = r#"
(function_declaration name: (identifier) @symbol_name) @function
(class_declaration name: (identifier) @symbol_name) @class
"#;

const GO_QUERY: &str = r#"
(function_declaration name: (identifier) @symbol_name) @function
(method_declaration name: (field_identifier) @symbol_name) @function
(type_declaration (type_spec name: (type_identifier) @symbol_name)) @struct
"#;

const JAVA_QUERY: &str = r#"
(method_declaration name: (identifier) @symbol_name) @function
(class_declaration name: (identifier) @symbol_name) @class
(interface_declaration name: (identifier) @symbol_name) @interface
(enum_declaration name: (identifier) @symbol_name) @enum
"#;

const C_QUERY: &str = r#"
(function_definition declarator: (function_declarator declarator: (identifier) @symbol_name)) @function
(struct_specifier name: (type_identifier) @symbol_name) @struct
(enum_specifier name: (type_identifier) @symbol_name) @enum
"#;

const CPP_QUERY: &str = r#"
(function_definition declarator: (function_declarator declarator: (identifier) @symbol_name)) @function
(class_specifier name: (type_identifier) @symbol_name) @class
(struct_specifier name: (type_identifier) @symbol_name) @struct
(enum_specifier name: (type_identifier) @symbol_name) @enum
"#;

const KOTLIN_QUERY: &str = r#"
(function_declaration (simple_identifier) @symbol_name) @function
(class_declaration (type_identifier) @symbol_name) @class
(object_declaration (type_identifier) @symbol_name) @class
(interface_declaration (type_identifier) @symbol_name) @interface
"#;

// ---------------------------------------------------------------------------
// Reference extraction (symbol graph edges)
// ---------------------------------------------------------------------------

/// A reference extracted from source code: a call, import, or implementation.
pub struct ExtractedReference {
    pub target_name: String,
    pub edge_type: String,
    pub line: u32,
}

/// Get the reference query source for a language.
pub fn get_ref_query_source(lang: &str) -> Option<&'static str> {
    match lang {
        "rust" => Some(RUST_REF_QUERY),
        "python" => Some(PYTHON_REF_QUERY),
        "typescript" => Some(TYPESCRIPT_REF_QUERY),
        "javascript" => Some(JAVASCRIPT_REF_QUERY),
        "go" => Some(GO_REF_QUERY),
        "java" => Some(JAVA_REF_QUERY),
        "c" => Some(C_REF_QUERY),
        "cpp" => Some(CPP_REF_QUERY),
        "kotlin" => Some(KOTLIN_REF_QUERY),
        _ => None,
    }
}

/// Extract references (calls, imports, implementations) from source code.
pub fn extract_references(source: &str, lang: &str) -> Result<Vec<ExtractedReference>> {
    let entry = get_language_entry(lang).ok_or_else(|| anyhow!("Unsupported language: {lang}"))?;
    let ref_query_src =
        get_ref_query_source(lang).ok_or_else(|| anyhow!("No ref query for: {lang}"))?;

    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&entry.language)?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| anyhow!("tree-sitter parse failed for {lang}"))?;

    let query = tree_sitter::Query::new(&entry.language, ref_query_src)?;
    let mut cursor = tree_sitter::QueryCursor::new();
    let matches = cursor.matches(&query, tree.root_node(), source.as_bytes());

    let capture_names: Vec<&str> = query.capture_names().iter().map(|s| s.as_ref()).collect();
    let source_bytes = source.as_bytes();

    let mut refs = Vec::new();

    for m in matches {
        let mut target_name = String::new();
        let mut edge_type = String::new();
        let mut line = 0u32;

        for cap in m.captures {
            let cap_name = capture_names[cap.index as usize];
            let node = cap.node;

            match cap_name {
                "call_target" | "import_target" | "impl_target" | "import_module" => {
                    // For import_module, we only use it if we don't have an import_target yet
                    if cap_name == "import_module" && !target_name.is_empty() {
                        continue;
                    }
                    target_name = node.utf8_text(source_bytes).unwrap_or("").to_string();
                    line = node.start_position().row as u32 + 1;
                }
                "calls" => edge_type = "calls".to_string(),
                "imports" => edge_type = "imports".to_string(),
                "implements" => edge_type = "implements".to_string(),
                _ => {}
            }
        }

        if !target_name.is_empty() && !edge_type.is_empty() {
            // Strip quotes from import paths (JS/TS/Go/Java string literals)
            let cleaned = target_name
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            refs.push(ExtractedReference {
                target_name: cleaned,
                edge_type,
                line,
            });
        }
    }

    Ok(refs)
}

// ---------------------------------------------------------------------------
// Reference queries per language
// ---------------------------------------------------------------------------

const RUST_REF_QUERY: &str = r#"
(call_expression function: (identifier) @call_target) @calls
(call_expression function: (field_expression field: (field_identifier) @call_target)) @calls
(call_expression function: (scoped_identifier name: (identifier) @call_target)) @calls
(use_declaration argument: (identifier) @import_target) @imports
(use_declaration argument: (scoped_identifier name: (identifier) @import_target)) @imports
(impl_item trait: (type_identifier) @impl_target) @implements
(impl_item trait: (scoped_type_identifier name: (type_identifier) @impl_target)) @implements
"#;

const PYTHON_REF_QUERY: &str = r#"
(call function: (identifier) @call_target) @calls
(call function: (attribute attribute: (identifier) @call_target)) @calls
(import_statement name: (dotted_name) @import_target) @imports
(import_from_statement module_name: (dotted_name) @import_module name: (dotted_name) @import_target) @imports
(class_definition superclasses: (argument_list (identifier) @impl_target)) @implements
"#;

const TYPESCRIPT_REF_QUERY: &str = r#"
(call_expression function: (identifier) @call_target) @calls
(call_expression function: (member_expression property: (property_identifier) @call_target)) @calls
(import_statement source: (string) @import_target) @imports
(class_declaration (class_heritage (implements_clause (type) @impl_target))) @implements
"#;

const JAVASCRIPT_REF_QUERY: &str = r#"
(call_expression function: (identifier) @call_target) @calls
(call_expression function: (member_expression property: (property_identifier) @call_target)) @calls
(import_statement source: (string) @import_target) @imports
"#;

const GO_REF_QUERY: &str = r#"
(call_expression function: (identifier) @call_target) @calls
(call_expression function: (selector_expression field: (field_identifier) @call_target)) @calls
(import_spec path: (interpreted_string_literal) @import_target) @imports
"#;

const JAVA_REF_QUERY: &str = r#"
(method_invocation name: (identifier) @call_target) @calls
(import_declaration (scoped_identifier name: (identifier) @import_target)) @imports
(class_declaration interfaces: (super_interfaces (type_list (type_identifier) @impl_target))) @implements
"#;

const C_REF_QUERY: &str = r#"
(call_expression function: (identifier) @call_target) @calls
(preproc_include path: (string_literal) @import_target) @imports
(preproc_include path: (system_lib_string) @import_target) @imports
"#;

const CPP_REF_QUERY: &str = r#"
(call_expression function: (identifier) @call_target) @calls
(call_expression function: (field_expression field: (field_identifier) @call_target)) @calls
(preproc_include path: (string_literal) @import_target) @imports
(preproc_include path: (system_lib_string) @import_target) @imports
(class_specifier (base_class_clause (type_identifier) @impl_target)) @implements
"#;

const KOTLIN_REF_QUERY: &str = r#"
(call_expression (simple_identifier) @call_target) @calls
(call_expression (navigation_expression (simple_identifier) @call_target)) @calls
(import_header (identifier) @import_target) @imports
(class_declaration (delegation_specifier (user_type (type_identifier) @impl_target))) @implements
"#;

/// Check if a file should be skipped (binary, too large, etc.)
pub fn should_skip_file(path: &str, size_bytes: u64) -> bool {
    if size_bytes > 1_048_576 {
        return true; // >1MB
    }
    let ext = match path.rsplit('.').next() {
        Some(e) => e.to_lowercase(),
        None => return false,
    };
    matches!(
        ext.as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "ico" | "svg" | "webp" | "bmp"
            | "pdf" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx"
            | "zip" | "tar" | "gz" | "bz2" | "xz" | "7z" | "rar"
            | "exe" | "dll" | "so" | "dylib" | "o" | "a" | "lib"
            | "wasm" | "class" | "pyc" | "pyo"
            | "ttf" | "otf" | "woff" | "woff2" | "eot"
            | "mp3" | "mp4" | "wav" | "avi" | "mov" | "mkv"
            | "db" | "sqlite" | "sqlite3"
            | "lock" | "min.js" | "min.css"
            | "lance" | "parquet" | "arrow"
    )
}
