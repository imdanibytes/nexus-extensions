use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::watch;

use crate::languages::{
    detect_language, extract_references, extract_symbols, should_skip_file, ExtractedReference,
};
use crate::search::{self, EdgeRecord};

/// Result of building the symbol graph for a repository.
pub struct GraphResult {
    pub edge_count: u64,
    pub head_commit: Option<String>,
}

/// Compute a stable edge ID.
pub fn edge_id(repo_id: &str, source_file: &str, line: u32, target_name: &str, edge_type: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{repo_id}:{source_file}:{line}:{target_name}:{edge_type}").as_bytes());
    hex::encode(hasher.finalize())
}

/// Build the symbol graph for a repository. Walks all files, extracts references,
/// resolves names, and stores edges in LanceDB.
pub async fn build_graph(
    repo_path: &str,
    repo_id: &str,
    edges_table: &lancedb::Table,
    chunks_table: &lancedb::Table,
    cancel: watch::Receiver<bool>,
) -> Result<GraphResult> {
    let repo = git2::Repository::discover(repo_path)
        .map_err(|e| anyhow!("Not a git repository: {e}"))?;
    let head_commit = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .map(|oid| oid.to_string());

    // Clear existing edges for this repo
    search::delete_repo_edges(edges_table, repo_id).await?;

    // Build a global symbol index from the chunks table (for name resolution across files)
    let global_symbol_index = build_symbol_index(chunks_table, repo_id).await?;

    // Two-pass approach:
    // Pass 1: Walk all files, extract symbols directly via tree-sitter (for containing-symbol lookup)
    //         and collect file sources for reference extraction.
    // Pass 2: Extract references and resolve names.
    //
    // We combine both passes into a single walk for efficiency — extract symbols AND references
    // from each file, using the direct tree-sitter symbols for containing-symbol resolution.

    let walker = ignore::WalkBuilder::new(repo_path)
        .hidden(true)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .build();

    let repo_root = Path::new(repo_path).canonicalize()?;
    let mut edge_batch: Vec<EdgeRecord> = Vec::new();
    let mut total_edges: u64 = 0;

    // Also build a fresh symbol index from direct extraction (more complete than chunks table)
    let mut direct_symbol_index: HashMap<String, Vec<SymbolInfo>> = HashMap::new();

    // First walk: extract symbols from all files
    let walker_syms = ignore::WalkBuilder::new(repo_path)
        .hidden(true)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .build();

    for entry in walker_syms {
        if *cancel.borrow() {
            return Err(anyhow!("Graph building cancelled"));
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
            continue;
        }

        let abs_path = entry.path();
        let rel_path = abs_path
            .strip_prefix(&repo_root)
            .unwrap_or(abs_path)
            .to_string_lossy()
            .to_string();

        let meta = match abs_path.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };

        if should_skip_file(&rel_path, meta.len()) {
            continue;
        }

        let lang = match detect_language(&rel_path) {
            Some(l) => l,
            None => continue,
        };

        let source = match std::fs::read_to_string(abs_path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        if let Ok(symbols) = extract_symbols(&source, lang) {
            let infos: Vec<SymbolInfo> = symbols
                .iter()
                .map(|s| SymbolInfo {
                    name: s.name.clone(),
                    symbol_type: s.symbol_type.clone(),
                    start_line: s.start_line,
                    end_line: s.end_line,
                })
                .collect();
            if !infos.is_empty() {
                direct_symbol_index.insert(rel_path, infos);
            }
        }
    }

    // Merge direct_symbol_index into global for name resolution
    // (direct extraction is more complete — includes all files regardless of chunking strategy)
    let mut merged_index = global_symbol_index;
    for (file, symbols) in &direct_symbol_index {
        merged_index
            .entry(file.clone())
            .or_insert_with(|| symbols.clone());
    }

    // Second walk: extract references and build edges
    for entry in walker {
        if *cancel.borrow() {
            return Err(anyhow!("Graph building cancelled"));
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
            continue;
        }

        let abs_path = entry.path();
        let rel_path = abs_path
            .strip_prefix(&repo_root)
            .unwrap_or(abs_path)
            .to_string_lossy()
            .to_string();

        let meta = match abs_path.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };

        if should_skip_file(&rel_path, meta.len()) {
            continue;
        }

        let lang = match detect_language(&rel_path) {
            Some(l) => l,
            None => continue,
        };

        let source = match tokio::fs::read_to_string(abs_path).await {
            Ok(s) => s,
            Err(_) => continue,
        };

        let refs = match extract_references(&source, lang) {
            Ok(r) => r,
            Err(_) => continue,
        };

        if refs.is_empty() {
            continue;
        }

        let import_map = extract_import_map(&refs);

        // Use direct tree-sitter symbols for containing-symbol lookup (not chunks table)
        let file_symbols = direct_symbol_index
            .get(&rel_path)
            .cloned()
            .unwrap_or_default();

        for reference in &refs {
            let containing = find_containing_symbol(&file_symbols, reference.line);

            let (target_qualified, target_file) = resolve_reference(
                &reference.target_name,
                &reference.edge_type,
                &rel_path,
                &import_map,
                &merged_index,
            );

            edge_batch.push(EdgeRecord {
                id: edge_id(repo_id, &rel_path, reference.line, &reference.target_name, &reference.edge_type),
                repo_id: repo_id.to_string(),
                source_file: rel_path.clone(),
                source_symbol: containing,
                source_line: reference.line,
                target_name: reference.target_name.clone(),
                target_qualified,
                target_file,
                edge_type: reference.edge_type.clone(),
            });

            if edge_batch.len() >= 500 {
                total_edges += edge_batch.len() as u64;
                search::upsert_edges(edges_table, &edge_batch).await?;
                edge_batch.clear();
            }
        }
    }

    if !edge_batch.is_empty() {
        total_edges += edge_batch.len() as u64;
        search::upsert_edges(edges_table, &edge_batch).await?;
    }

    Ok(GraphResult {
        edge_count: total_edges,
        head_commit,
    })
}

/// Symbol info from the chunks table for name resolution.
#[derive(Clone)]
struct SymbolInfo {
    name: String,
    #[allow(dead_code)]
    symbol_type: String,
    start_line: u32,
    end_line: u32,
}

/// Build an index of all symbols in a repo from the chunks table.
/// Returns a map of file_path → Vec<SymbolInfo>.
async fn build_symbol_index(
    chunks_table: &lancedb::Table,
    repo_id: &str,
) -> Result<HashMap<String, Vec<SymbolInfo>>> {
    let entries = search::scan_symbols(chunks_table, repo_id, None, None).await?;

    let mut index: HashMap<String, Vec<SymbolInfo>> = HashMap::new();
    for entry in entries {
        index
            .entry(entry.file_path.clone())
            .or_default()
            .push(SymbolInfo {
                name: entry.symbol_name,
                symbol_type: entry.symbol_type,
                start_line: entry.start_line,
                end_line: entry.end_line,
            });
    }

    Ok(index)
}

/// Find the symbol that contains a given line number.
fn find_containing_symbol(symbols: &[SymbolInfo], line: u32) -> String {
    // Find the innermost (smallest range) symbol containing this line
    let mut best: Option<&SymbolInfo> = None;

    for sym in symbols {
        if line >= sym.start_line && line <= sym.end_line {
            match best {
                None => best = Some(sym),
                Some(current) => {
                    let current_range = current.end_line - current.start_line;
                    let new_range = sym.end_line - sym.start_line;
                    if new_range < current_range {
                        best = Some(sym);
                    }
                }
            }
        }
    }

    best.map(|s| s.name.clone()).unwrap_or_default()
}

/// Extract an import map from references: maps short name → full import path.
fn extract_import_map(refs: &[ExtractedReference]) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for r in refs {
        if r.edge_type != "imports" {
            continue;
        }

        let target = &r.target_name;

        // Extract the short name from the import path
        // "crate::auth::validate" → "validate"
        // "std::collections::HashMap" → "HashMap"
        // "./utils" → "utils"
        // "express" → "express"
        if let Some(short) = target.rsplit("::").next() {
            map.insert(short.to_string(), target.clone());
        } else if let Some(short) = target.rsplit('/').next() {
            // JS/TS style paths
            let clean = short.trim_matches('"').trim_matches('\'');
            map.insert(clean.to_string(), target.clone());
        }
    }

    map
}

/// Attempt to resolve a reference to a qualified name and file path.
/// Returns (target_qualified, target_file).
fn resolve_reference(
    target_name: &str,
    edge_type: &str,
    current_file: &str,
    import_map: &HashMap<String, String>,
    symbol_index: &HashMap<String, Vec<SymbolInfo>>,
) -> (String, String) {
    // 1. Already qualified (contains :: or /)
    if target_name.contains("::") || (edge_type == "imports" && target_name.contains('/')) {
        // Try to find the file containing this symbol
        let short_name = target_name
            .rsplit("::")
            .next()
            .or_else(|| target_name.rsplit('/').next())
            .unwrap_or(target_name);

        let file = find_symbol_file(short_name, symbol_index);
        return (target_name.to_string(), file);
    }

    // 2. Import-guided resolution
    if let Some(qualified) = import_map.get(target_name) {
        let file = find_symbol_file(target_name, symbol_index);
        return (qualified.clone(), file);
    }

    // 3. Same-file resolution
    if let Some(symbols) = symbol_index.get(current_file) {
        for sym in symbols {
            if sym.name == target_name {
                return (target_name.to_string(), current_file.to_string());
            }
        }
    }

    // 4. Global symbol index — unique match
    let file = find_symbol_file(target_name, symbol_index);
    if !file.is_empty() {
        return (target_name.to_string(), file);
    }

    // Unresolved
    (String::new(), String::new())
}

/// Find the file containing a symbol by name. Returns empty string if not found
/// or ambiguous (multiple files define it).
fn find_symbol_file(name: &str, symbol_index: &HashMap<String, Vec<SymbolInfo>>) -> String {
    let mut found_file = String::new();
    let mut count = 0;

    for (file, symbols) in symbol_index {
        for sym in symbols {
            if sym.name == name {
                if count == 0 {
                    found_file = file.clone();
                }
                count += 1;
                if count > 1 {
                    return String::new(); // Ambiguous
                }
                break; // Only count once per file
            }
        }
    }

    found_file
}
