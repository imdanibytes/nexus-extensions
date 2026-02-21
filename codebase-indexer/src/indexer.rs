use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::sync::watch;

use crate::embeddings::EmbeddingProvider;
use crate::languages::{detect_language, extract_symbols, should_skip_file};
use crate::search::{self, ChunkRecord};

/// Compute a stable repo ID from the canonical path.
pub fn compute_repo_id(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    hex::encode(hasher.finalize())
}

/// Compute a stable chunk ID.
fn chunk_id(repo_id: &str, file_path: &str, symbol_name: &str, start_line: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{repo_id}:{file_path}:{symbol_name}:{start_line}").as_bytes());
    hex::encode(hasher.finalize())
}

/// Result of indexing a single repository.
pub struct IndexResult {
    pub chunk_count: u64,
    pub embed_pending: bool,
    pub head_commit: Option<String>,
}

/// Index all files in a repository. Runs in a background task.
pub async fn index_repository(
    repo_path: &str,
    repo_id: &str,
    table: &lancedb::Table,
    provider: &dyn EmbeddingProvider,
    dimensions: usize,
    cancel: watch::Receiver<bool>,
) -> Result<IndexResult> {
    let repo = git2::Repository::discover(repo_path)
        .map_err(|e| anyhow!("Not a git repository: {e}"))?;
    let head_commit = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .map(|oid| oid.to_string());

    // Walk files respecting .gitignore
    let walker = ignore::WalkBuilder::new(repo_path)
        .hidden(true) // skip hidden files
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .build();

    let repo_root = Path::new(repo_path).canonicalize()?;
    let mut all_chunks: Vec<ChunkRecord> = Vec::new();
    let mut embed_pending = false;

    for entry in walker {
        // Check cancellation
        if *cancel.borrow() {
            return Err(anyhow!("Indexing cancelled"));
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

        let source = match tokio::fs::read_to_string(abs_path).await {
            Ok(s) => s,
            Err(_) => continue, // binary or unreadable
        };

        let lang = detect_language(&rel_path).unwrap_or("unknown");
        let chunks = chunk_file(&source, &rel_path, lang, repo_id);

        all_chunks.extend(chunks);

        // Batch embed every 32 chunks
        if all_chunks.len() >= 32 {
            embed_and_upsert(&mut all_chunks, table, provider, dimensions, &mut embed_pending)
                .await?;
        }
    }

    // Flush remaining
    if !all_chunks.is_empty() {
        embed_and_upsert(&mut all_chunks, table, provider, dimensions, &mut embed_pending).await?;
    }

    // Count total chunks for this repo
    let chunk_count =
        search::count_rows_filtered(table, &format!("repo_id = '{repo_id}'")).await?;

    Ok(IndexResult {
        chunk_count,
        embed_pending,
        head_commit,
    })
}

/// Chunk a single file into ChunkRecords (no embeddings yet).
fn chunk_file(source: &str, file_path: &str, lang: &str, repo_id: &str) -> Vec<ChunkRecord> {
    let line_count = source.lines().count();
    let mut chunks = Vec::new();

    // Short files: single chunk
    if line_count < 100 {
        chunks.push(ChunkRecord {
            id: chunk_id(repo_id, file_path, "", 1),
            repo_id: repo_id.to_string(),
            file_path: file_path.to_string(),
            language: lang.to_string(),
            symbol_name: String::new(),
            symbol_type: "file_chunk".to_string(),
            start_line: 1,
            end_line: line_count as u32,
            content: source.to_string(),
            vector: None,
        });
        return chunks;
    }

    // Try tree-sitter extraction
    if lang != "unknown" {
        if let Ok(symbols) = extract_symbols(source, lang) {
            if !symbols.is_empty() {
                for sym in symbols {
                    chunks.push(ChunkRecord {
                        id: chunk_id(repo_id, file_path, &sym.name, sym.start_line),
                        repo_id: repo_id.to_string(),
                        file_path: file_path.to_string(),
                        language: lang.to_string(),
                        symbol_name: sym.name,
                        symbol_type: sym.symbol_type,
                        start_line: sym.start_line,
                        end_line: sym.end_line,
                        content: sym.content,
                        vector: None,
                    });
                }
                return chunks;
            }
        }
    }

    // Fallback: sliding window (200 lines, 50 overlap)
    let lines: Vec<&str> = source.lines().collect();
    let window = 200;
    let overlap = 50;
    let mut start = 0;

    while start < lines.len() {
        let end = (start + window).min(lines.len());
        let content = lines[start..end].join("\n");
        chunks.push(ChunkRecord {
            id: chunk_id(repo_id, file_path, "", (start + 1) as u32),
            repo_id: repo_id.to_string(),
            file_path: file_path.to_string(),
            language: lang.to_string(),
            symbol_name: String::new(),
            symbol_type: "file_chunk".to_string(),
            start_line: (start + 1) as u32,
            end_line: end as u32,
            content,
            vector: None,
        });
        if end >= lines.len() {
            break;
        }
        start += window - overlap;
    }

    chunks
}

/// Embed a batch of chunks and upsert them into LanceDB.
async fn embed_and_upsert(
    chunks: &mut Vec<ChunkRecord>,
    table: &lancedb::Table,
    provider: &dyn EmbeddingProvider,
    dimensions: usize,
    embed_pending: &mut bool,
) -> Result<()> {
    let texts: Vec<String> = chunks.iter().map(|c| c.content.clone()).collect();

    match provider.embed_batch(&texts).await {
        Ok(vectors) => {
            for (chunk, vec) in chunks.iter_mut().zip(vectors) {
                chunk.vector = Some(vec);
            }
        }
        Err(e) => {
            eprintln!("codebase-indexer: embedding failed, storing without vectors: {e}");
            *embed_pending = true;
            // Leave vectors as None — chunks still stored for map operations
        }
    }

    search::upsert_chunks(table, chunks, dimensions).await?;
    chunks.clear();
    Ok(())
}

/// Incremental sync: diff against last indexed commit, re-index changed files.
pub async fn sync_repository(
    repo_path: &str,
    repo_id: &str,
    last_commit: &str,
    table: &lancedb::Table,
    provider: &dyn EmbeddingProvider,
    dimensions: usize,
    cancel: watch::Receiver<bool>,
) -> Result<IndexResult> {
    let repo = git2::Repository::discover(repo_path)
        .map_err(|e| anyhow!("Not a git repository: {e}"))?;

    let head = repo.head()?.peel_to_commit()?;
    let head_oid = head.id().to_string();

    if head_oid == last_commit {
        let chunk_count =
            search::count_rows_filtered(table, &format!("repo_id = '{repo_id}'")).await?;
        return Ok(IndexResult {
            chunk_count,
            embed_pending: false,
            head_commit: Some(head_oid),
        });
    }

    let old_oid = git2::Oid::from_str(last_commit)
        .map_err(|e| anyhow!("Invalid last_indexed_commit: {e}"))?;
    let old_commit = repo.find_commit(old_oid)?;
    let old_tree = old_commit.tree()?;
    let new_tree = head.tree()?;

    let diff = repo.diff_tree_to_tree(Some(&old_tree), Some(&new_tree), None)?;

    let mut deleted_paths = Vec::new();
    let mut changed_paths = Vec::new();

    diff.foreach(
        &mut |delta, _| {
            match delta.status() {
                git2::Delta::Deleted => {
                    if let Some(path) = delta.old_file().path() {
                        deleted_paths.push(path.to_string_lossy().to_string());
                    }
                }
                git2::Delta::Added | git2::Delta::Modified => {
                    if let Some(path) = delta.new_file().path() {
                        changed_paths.push(path.to_string_lossy().to_string());
                    }
                }
                _ => {}
            }
            true
        },
        None,
        None,
        None,
    )?;

    // Delete removed files
    if !deleted_paths.is_empty() {
        search::delete_file_chunks(table, repo_id, &deleted_paths).await?;
    }

    // Delete then re-index changed files
    if !changed_paths.is_empty() {
        search::delete_file_chunks(table, repo_id, &changed_paths).await?;
    }

    let repo_root = Path::new(repo_path).canonicalize()?;
    let mut all_chunks: Vec<ChunkRecord> = Vec::new();
    let mut embed_pending = false;

    for rel_path in &changed_paths {
        if *cancel.borrow() {
            return Err(anyhow!("Sync cancelled"));
        }

        let abs_path = repo_root.join(rel_path);
        if !abs_path.exists() || !abs_path.is_file() {
            continue;
        }

        let meta = match abs_path.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };

        if should_skip_file(rel_path, meta.len()) {
            continue;
        }

        let source = match tokio::fs::read_to_string(&abs_path).await {
            Ok(s) => s,
            Err(_) => continue,
        };

        let lang = detect_language(rel_path).unwrap_or("unknown");
        let chunks = chunk_file(&source, rel_path, lang, repo_id);
        all_chunks.extend(chunks);

        if all_chunks.len() >= 32 {
            embed_and_upsert(&mut all_chunks, table, provider, dimensions, &mut embed_pending)
                .await?;
        }
    }

    if !all_chunks.is_empty() {
        embed_and_upsert(&mut all_chunks, table, provider, dimensions, &mut embed_pending).await?;
    }

    let chunk_count =
        search::count_rows_filtered(table, &format!("repo_id = '{repo_id}'")).await?;

    Ok(IndexResult {
        chunk_count,
        embed_pending,
        head_commit: Some(head_oid),
    })
}
