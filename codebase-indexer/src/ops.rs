use crate::graph;
use crate::indexer;
use crate::search;
use crate::state::{AppState, RepoMeta, WorkspaceMeta};
use chrono::Utc;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

type Result = std::result::Result<Value, String>;

// ---------------------------------------------------------------------------
// add_repository
// ---------------------------------------------------------------------------

pub async fn op_add_repository(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let path = input
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: path")?;

    // Canonicalize path
    let canonical = std::path::Path::new(path)
        .canonicalize()
        .map_err(|e| format!("Invalid path: {e}"))?;
    let canonical_str = canonical.to_string_lossy().to_string();

    // Verify it's a git repo
    git2::Repository::discover(&canonical)
        .map_err(|e| format!("Not a git repository: {e}"))?;

    let repo_id = indexer::compute_repo_id(&canonical_str);
    let repo_name = canonical
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".into());

    {
        let st = state.read().await;
        if st.is_indexing(&repo_id) {
            return Ok(serde_json::json!({
                "repo_id": repo_id,
                "name": repo_name,
                "status": "already_indexing",
            }));
        }
    }

    // Register repo in metadata
    {
        let mut st = state.write().await;
        let meta = st.metadata.repositories.entry(repo_id.clone()).or_insert(RepoMeta {
            id: repo_id.clone(),
            path: canonical_str.clone(),
            name: repo_name.clone(),
            chunk_count: 0,
            embed_pending: false,
            last_indexed_commit: None,
            last_indexed_at: None,
            last_sync_error: None,
            indexing: true,
            graph_edge_count: 0,
            graph_indexed_at: None,
            last_graph_commit: None,
            graph_building: false,
        });
        meta.indexing = true;
        meta.last_sync_error = None;
        st.save_metadata().await.ok();
    }

    // Spawn background indexing
    let (cancel_tx, cancel_rx) = watch::channel(false);
    {
        let mut st = state.write().await;
        st.indexing_cancels.insert(repo_id.clone(), cancel_tx);
    }

    let state_bg = state.clone();
    let repo_id_bg = repo_id.clone();
    let path_bg = canonical_str.clone();

    tokio::spawn(async move {
        // Get provider info and table before entering the long task
        let (table, dimensions) = {
            let st = state_bg.read().await;
            let dims = st.provider.dimensions();
            let table = match search::ensure_table(&st.db, dims).await {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("codebase-indexer: table creation failed: {e}");
                    let mut st = state_bg.write().await;
                    if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                        meta.indexing = false;
                        meta.last_sync_error = Some(format!("Table creation failed: {e}"));
                    }
                    st.indexing_cancels.remove(&repo_id_bg);
                    st.save_metadata().await.ok();
                    return;
                }
            };
            (table, dims)
        };

        // We need to hold a read lock to access the provider, but the indexer
        // is async and long-running. We'll work around this by creating a
        // temporary provider reference approach — since EmbeddingProvider is
        // behind a Box<dyn>, we need the lock. We'll do batched lock acquisition
        // in the indexer instead. For simplicity, we'll hold a read lock for the
        // entire index operation since writes only touch metadata.
        let result = {
            let st = state_bg.read().await;
            indexer::index_repository(
                &path_bg,
                &repo_id_bg,
                &table,
                st.provider.as_ref(),
                dimensions,
                cancel_rx,
            )
            .await
        };

        let mut st = state_bg.write().await;
        st.indexing_cancels.remove(&repo_id_bg);

        match result {
            Ok(index_result) => {
                if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                    meta.indexing = false;
                    meta.chunk_count = index_result.chunk_count;
                    meta.embed_pending = index_result.embed_pending;
                    meta.last_indexed_commit = index_result.head_commit;
                    meta.last_indexed_at = Some(Utc::now());
                    meta.last_sync_error = None;
                }
                st.save_metadata().await.ok();
                drop(st);

                // Auto-build graph after successful indexing
                spawn_graph_build(state_bg, repo_id_bg, path_bg).await;
            }
            Err(e) => {
                eprintln!("codebase-indexer: indexing failed for {repo_id_bg}: {e}");
                if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                    meta.indexing = false;
                    meta.last_sync_error = Some(e.to_string());
                }
                st.save_metadata().await.ok();
            }
        }
    });

    Ok(serde_json::json!({
        "repo_id": repo_id,
        "name": repo_name,
        "status": "indexing",
    }))
}

// ---------------------------------------------------------------------------
// remove_repository
// ---------------------------------------------------------------------------

pub async fn op_remove_repository(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let repo_id = input
        .get("repo_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: repo_id")?;

    let mut st = state.write().await;

    // Cancel if indexing
    if let Some(cancel) = st.indexing_cancels.remove(repo_id) {
        let _ = cancel.send(true);
    }

    // Cancel if graph building
    if let Some(cancel) = st.graph_cancels.remove(repo_id) {
        let _ = cancel.send(true);
    }

    // Remove from metadata
    let removed = st.metadata.repositories.remove(repo_id).is_some();

    // Remove from workspaces
    for ws in st.metadata.workspaces.values_mut() {
        ws.repo_ids.retain(|id| id != repo_id);
    }

    st.save_metadata().await.ok();

    // Delete chunks and edges
    if removed {
        let dims = st.provider.dimensions();
        if let Ok(table) = search::ensure_table(&st.db, dims).await {
            search::delete_repo_chunks(&table, repo_id).await.ok();
        }
        if let Ok(edges_table) = search::ensure_edges_table(&st.db).await {
            search::delete_repo_edges(&edges_table, repo_id).await.ok();
        }
    }

    Ok(serde_json::json!({
        "removed": removed,
        "repo_id": repo_id,
    }))
}

// ---------------------------------------------------------------------------
// list_repositories
// ---------------------------------------------------------------------------

pub async fn op_list_repositories(state: Arc<RwLock<AppState>>) -> Result {
    let st = state.read().await;
    let repos: Vec<Value> = st
        .metadata
        .repositories
        .values()
        .map(|r| {
            serde_json::json!({
                "repo_id": r.id,
                "name": r.name,
                "path": r.path,
                "chunk_count": r.chunk_count,
                "embed_pending": r.embed_pending,
                "last_indexed_commit": r.last_indexed_commit,
                "last_indexed_at": r.last_indexed_at,
                "last_sync_error": r.last_sync_error,
                "indexing": r.indexing || st.indexing_cancels.contains_key(&r.id),
                "graph_edge_count": r.graph_edge_count,
                "graph_indexed_at": r.graph_indexed_at,
                "graph_building": r.graph_building || st.graph_cancels.contains_key(&r.id),
            })
        })
        .collect();

    Ok(serde_json::json!({
        "count": repos.len(),
        "repositories": repos,
    }))
}

// ---------------------------------------------------------------------------
// sync
// ---------------------------------------------------------------------------

pub async fn op_sync(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let specific_repo = input.get("repo_id").and_then(|v| v.as_str());

    let repos_to_sync: Vec<(String, String, Option<String>)> = {
        let st = state.read().await;
        st.metadata
            .repositories
            .values()
            .filter(|r| {
                if let Some(id) = specific_repo {
                    r.id == id
                } else {
                    true
                }
            })
            .filter(|r| !r.indexing && !st.indexing_cancels.contains_key(&r.id))
            .map(|r| (r.id.clone(), r.path.clone(), r.last_indexed_commit.clone()))
            .collect()
    };

    if repos_to_sync.is_empty() {
        return Ok(serde_json::json!({
            "synced": 0,
            "message": "No repositories to sync",
        }));
    }

    let mut synced = Vec::new();

    for (repo_id, repo_path, last_commit) in &repos_to_sync {
        // Mark as indexing
        {
            let mut st = state.write().await;
            if let Some(meta) = st.metadata.repositories.get_mut(repo_id) {
                meta.indexing = true;
            }
        }

        let (cancel_tx, cancel_rx) = watch::channel(false);
        {
            let mut st = state.write().await;
            st.indexing_cancels.insert(repo_id.clone(), cancel_tx);
        }

        let result = {
            let st = state.read().await;
            let dims = st.provider.dimensions();
            let table = search::ensure_table(&st.db, dims)
                .await
                .map_err(|e| e.to_string())?;

            match last_commit {
                Some(commit) => {
                    indexer::sync_repository(
                        repo_path,
                        repo_id,
                        commit,
                        &table,
                        st.provider.as_ref(),
                        dims,
                        cancel_rx,
                    )
                    .await
                }
                None => {
                    // No previous commit — full re-index
                    indexer::index_repository(
                        repo_path,
                        repo_id,
                        &table,
                        st.provider.as_ref(),
                        dims,
                        cancel_rx,
                    )
                    .await
                }
            }
        };

        let mut st = state.write().await;
        st.indexing_cancels.remove(repo_id);

        match result {
            Ok(index_result) => {
                let repo_path_clone = repo_path.clone();
                if let Some(meta) = st.metadata.repositories.get_mut(repo_id) {
                    meta.indexing = false;
                    meta.chunk_count = index_result.chunk_count;
                    meta.embed_pending = index_result.embed_pending;
                    meta.last_indexed_commit = index_result.head_commit;
                    meta.last_indexed_at = Some(Utc::now());
                    meta.last_sync_error = None;
                }
                st.save_metadata().await.ok();
                synced.push(repo_id.clone());

                // Auto-build graph after successful sync
                drop(st);
                spawn_graph_build(state.clone(), repo_id.clone(), repo_path_clone).await;
                // Re-acquire for next iteration
                // (no more writes needed in loop body after this point)
            }
            Err(e) => {
                if let Some(meta) = st.metadata.repositories.get_mut(repo_id) {
                    meta.indexing = false;
                    meta.last_sync_error = Some(e.to_string());
                }
                st.save_metadata().await.ok();
            }
        }
    }

    Ok(serde_json::json!({
        "synced": synced.len(),
        "repo_ids": synced,
    }))
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

pub async fn op_search(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let query = input
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: query")?;

    let repo_id = input.get("repo_id").and_then(|v| v.as_str());
    let workspace = input.get("workspace").and_then(|v| v.as_str());
    let language = input.get("language").and_then(|v| v.as_str());
    let symbol_type = input.get("symbol_type").and_then(|v| v.as_str());
    let limit = input
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(10) as usize;
    let full_content = input
        .get("full_content")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let st = state.read().await;

    // Embed the query
    let query_vec = st
        .provider
        .embed_batch(&[query.to_string()])
        .await
        .map_err(|e| {
            format!(
                "Embedding provider not available at {}. Cannot search without embeddings: {e}",
                st.metadata.embedding.base_url
            )
        })?;

    let query_vec = query_vec
        .into_iter()
        .next()
        .ok_or("Empty embedding response")?;

    // Build filter
    let mut filter_parts = Vec::new();

    if let Some(id) = repo_id {
        filter_parts.push(format!("repo_id = '{id}'"));
    }

    if let Some(ws_name) = workspace {
        if let Some(ws) = st.metadata.workspaces.get(ws_name) {
            if !ws.repo_ids.is_empty() {
                let ids: Vec<String> = ws.repo_ids.iter().map(|id| format!("'{id}'")).collect();
                filter_parts.push(format!("repo_id IN ({})", ids.join(", ")));
            }
        }
    }

    if let Some(lang) = language {
        filter_parts.push(format!("language = '{lang}'"));
    }

    if let Some(st_type) = symbol_type {
        filter_parts.push(format!("symbol_type = '{st_type}'"));
    }

    let filter = if filter_parts.is_empty() {
        None
    } else {
        Some(filter_parts.join(" AND "))
    };

    let dims = st.provider.dimensions();
    let table = search::ensure_table(&st.db, dims)
        .await
        .map_err(|e| e.to_string())?;

    let results = search::vector_search(&table, &query_vec, filter, limit, full_content)
        .await
        .map_err(|e| e.to_string())?;

    let results_json: Vec<Value> = results
        .iter()
        .map(|r| {
            let mut obj = serde_json::json!({
                "file_path": r.file_path,
                "symbol_name": r.symbol_name,
                "symbol_type": r.symbol_type,
                "start_line": r.start_line,
                "end_line": r.end_line,
                "score": (r.score * 1000.0).round() / 1000.0,
                "snippet": r.snippet,
                "repo_id": r.repo_id,
                "language": r.language,
            });
            if let Some(ref content) = r.content {
                obj.as_object_mut()
                    .unwrap()
                    .insert("content".into(), Value::String(content.clone()));
            }
            obj
        })
        .collect();

    Ok(serde_json::json!({
        "query": query,
        "result_count": results_json.len(),
        "results": results_json,
    }))
}

// ---------------------------------------------------------------------------
// map
// ---------------------------------------------------------------------------

pub async fn op_map(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let repo_id = input
        .get("repo_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: repo_id")?;

    let language = input.get("language").and_then(|v| v.as_str());
    let symbol_type = input.get("symbol_type").and_then(|v| v.as_str());

    let st = state.read().await;
    let dims = st.provider.dimensions();
    let table = search::ensure_table(&st.db, dims)
        .await
        .map_err(|e| e.to_string())?;

    let entries = search::scan_symbols(&table, repo_id, language, symbol_type)
        .await
        .map_err(|e| e.to_string())?;

    // Group by file_path
    let mut files: HashMap<String, Vec<Value>> = HashMap::new();
    for entry in &entries {
        files
            .entry(entry.file_path.clone())
            .or_default()
            .push(serde_json::json!({
                "symbol_name": entry.symbol_name,
                "symbol_type": entry.symbol_type,
                "start_line": entry.start_line,
                "end_line": entry.end_line,
            }));
    }

    Ok(serde_json::json!({
        "repo_id": repo_id,
        "files": files,
        "total_symbols": entries.len(),
    }))
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

pub async fn op_status(state: Arc<RwLock<AppState>>) -> Result {
    let st = state.read().await;

    let provider_reachable = st.provider.health_check().await;
    let dims = st.provider.dimensions();

    let total_chunks = match search::ensure_table(&st.db, dims).await {
        Ok(table) => search::count_rows(&table).await.unwrap_or(0),
        Err(_) => 0,
    };

    let total_edges = match search::ensure_edges_table(&st.db).await {
        Ok(table) => search::count_rows(&table).await.unwrap_or(0),
        Err(_) => 0,
    };

    let indexed_count = st
        .metadata
        .repositories
        .values()
        .filter(|r| !r.indexing && r.last_indexed_commit.is_some())
        .count();
    let indexing_count = st
        .metadata
        .repositories
        .values()
        .filter(|r| r.indexing || st.indexing_cancels.contains_key(&r.id))
        .count();
    let pending_count = st
        .metadata
        .repositories
        .values()
        .filter(|r| r.embed_pending)
        .count();
    let graph_building_count = st
        .metadata
        .repositories
        .values()
        .filter(|r| r.graph_building || st.graph_cancels.contains_key(&r.id))
        .count();

    Ok(serde_json::json!({
        "provider": {
            "type": st.provider.provider_type(),
            "model": st.provider.model_id(),
            "reachable": provider_reachable,
            "url": st.metadata.embedding.base_url,
        },
        "db": {
            "total_chunks": total_chunks,
            "total_edges": total_edges,
        },
        "repos": {
            "indexed": indexed_count,
            "indexing": indexing_count,
            "pending_embed": pending_count,
            "graph_building": graph_building_count,
        },
    }))
}

// ---------------------------------------------------------------------------
// create_workspace
// ---------------------------------------------------------------------------

pub async fn op_create_workspace(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let name = input
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: name")?;

    let description = input
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let repo_ids: Vec<String> = input
        .get("repo_ids")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .ok_or("missing required field: repo_ids")?;

    let mut st = state.write().await;

    // Validate repo_ids exist
    for id in &repo_ids {
        if !st.metadata.repositories.contains_key(id) {
            return Err(format!("Unknown repository: {id}"));
        }
    }

    st.metadata.workspaces.insert(
        name.to_string(),
        WorkspaceMeta {
            name: name.to_string(),
            description: description.to_string(),
            repo_ids: repo_ids.clone(),
        },
    );

    st.save_metadata().await.ok();

    Ok(serde_json::json!({
        "name": name,
        "description": description,
        "repo_ids": repo_ids,
    }))
}

// ---------------------------------------------------------------------------
// list_workspaces
// ---------------------------------------------------------------------------

pub async fn op_list_workspaces(state: Arc<RwLock<AppState>>) -> Result {
    let st = state.read().await;
    let workspaces: Vec<Value> = st
        .metadata
        .workspaces
        .values()
        .map(|ws| {
            serde_json::json!({
                "name": ws.name,
                "description": ws.description,
                "repo_ids": ws.repo_ids,
            })
        })
        .collect();

    Ok(serde_json::json!({
        "count": workspaces.len(),
        "workspaces": workspaces,
    }))
}

// ---------------------------------------------------------------------------
// Graph helper: spawn background graph build
// ---------------------------------------------------------------------------

async fn spawn_graph_build(state: Arc<RwLock<AppState>>, repo_id: String, repo_path: String) {
    // Check if already building
    {
        let st = state.read().await;
        if st.graph_cancels.contains_key(&repo_id) {
            return;
        }
    }

    let (cancel_tx, cancel_rx) = watch::channel(false);
    {
        let mut st = state.write().await;
        st.graph_cancels.insert(repo_id.clone(), cancel_tx);
        if let Some(meta) = st.metadata.repositories.get_mut(&repo_id) {
            meta.graph_building = true;
        }
        st.save_metadata().await.ok();
    }

    let state_bg = state.clone();
    let repo_id_bg = repo_id.clone();

    tokio::spawn(async move {
        let result = {
            let st = state_bg.read().await;
            let dims = st.provider.dimensions();

            let chunks_table = match search::ensure_table(&st.db, dims).await {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("codebase-indexer: graph build failed (chunks table): {e}");
                    let mut st = state_bg.write().await;
                    if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                        meta.graph_building = false;
                    }
                    st.graph_cancels.remove(&repo_id_bg);
                    st.save_metadata().await.ok();
                    return;
                }
            };

            let edges_table = match search::ensure_edges_table(&st.db).await {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("codebase-indexer: graph build failed (edges table): {e}");
                    let mut st = state_bg.write().await;
                    if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                        meta.graph_building = false;
                    }
                    st.graph_cancels.remove(&repo_id_bg);
                    st.save_metadata().await.ok();
                    return;
                }
            };

            graph::build_graph(&repo_path, &repo_id_bg, &edges_table, &chunks_table, cancel_rx)
                .await
        };

        let mut st = state_bg.write().await;
        st.graph_cancels.remove(&repo_id_bg);

        match result {
            Ok(graph_result) => {
                if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                    meta.graph_building = false;
                    meta.graph_edge_count = graph_result.edge_count;
                    meta.graph_indexed_at = Some(Utc::now());
                    meta.last_graph_commit = graph_result.head_commit;
                }
            }
            Err(e) => {
                eprintln!("codebase-indexer: graph build failed for {repo_id_bg}: {e}");
                if let Some(meta) = st.metadata.repositories.get_mut(&repo_id_bg) {
                    meta.graph_building = false;
                }
            }
        }
        st.save_metadata().await.ok();
    });
}

// ---------------------------------------------------------------------------
// build_graph — manual rebuild
// ---------------------------------------------------------------------------

pub async fn op_build_graph(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let repo_id = input
        .get("repo_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: repo_id")?;

    let repo_path = {
        let st = state.read().await;
        let meta = st
            .metadata
            .repositories
            .get(repo_id)
            .ok_or_else(|| format!("Unknown repository: {repo_id}"))?;

        if st.graph_cancels.contains_key(repo_id) {
            return Ok(serde_json::json!({
                "repo_id": repo_id,
                "status": "already_building",
            }));
        }

        meta.path.clone()
    };

    spawn_graph_build(state, repo_id.to_string(), repo_path).await;

    Ok(serde_json::json!({
        "repo_id": repo_id,
        "status": "building",
    }))
}

// ---------------------------------------------------------------------------
// find_references
// ---------------------------------------------------------------------------

pub async fn op_find_references(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let symbol = input
        .get("symbol")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: symbol")?;

    let repo_id = input.get("repo_id").and_then(|v| v.as_str());
    let edge_type = input.get("edge_type").and_then(|v| v.as_str());
    let limit = input
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;

    let st = state.read().await;
    let edges_table = search::ensure_edges_table(&st.db)
        .await
        .map_err(|e| e.to_string())?;

    let escaped = symbol.replace('\'', "''");
    let mut filter_parts = vec![format!(
        "(target_name = '{escaped}' OR target_qualified = '{escaped}' OR source_symbol = '{escaped}')"
    )];

    if let Some(id) = repo_id {
        filter_parts.push(format!("repo_id = '{id}'"));
    }

    if let Some(et) = edge_type {
        filter_parts.push(format!("edge_type = '{et}'"));
    }

    let filter = filter_parts.join(" AND ");

    let results = search::query_edges(&edges_table, &filter, Some(limit))
        .await
        .map_err(|e| e.to_string())?;

    let refs: Vec<Value> = results
        .iter()
        .map(|e| {
            serde_json::json!({
                "source_file": e.source_file,
                "source_symbol": e.source_symbol,
                "source_line": e.source_line,
                "target_name": e.target_name,
                "target_qualified": e.target_qualified,
                "target_file": e.target_file,
                "edge_type": e.edge_type,
            })
        })
        .collect();

    Ok(serde_json::json!({
        "symbol": symbol,
        "result_count": refs.len(),
        "references": refs,
    }))
}

// ---------------------------------------------------------------------------
// call_graph — BFS traversal
// ---------------------------------------------------------------------------

pub async fn op_call_graph(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let symbol = input
        .get("symbol")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: symbol")?;

    let repo_id = input
        .get("repo_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: repo_id")?;

    let direction = input
        .get("direction")
        .and_then(|v| v.as_str())
        .unwrap_or("callees");

    let depth = input
        .get("depth")
        .and_then(|v| v.as_u64())
        .unwrap_or(2)
        .min(5) as usize;

    let st = state.read().await;
    let edges_table = search::ensure_edges_table(&st.db)
        .await
        .map_err(|e| e.to_string())?;

    // BFS
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    let mut edges_out: Vec<Value> = Vec::new();

    queue.push_back((symbol.to_string(), 0));
    visited.insert(symbol.to_string());

    while let Some((current, current_depth)) = queue.pop_front() {
        if current_depth >= depth {
            continue;
        }

        let escaped = current.replace('\'', "''");
        let filter = if direction == "callers" {
            format!(
                "repo_id = '{repo_id}' AND edge_type = 'calls' AND (target_name = '{escaped}' OR target_qualified = '{escaped}')"
            )
        } else {
            format!(
                "repo_id = '{repo_id}' AND edge_type = 'calls' AND source_symbol = '{escaped}'"
            )
        };

        let results = search::query_edges(&edges_table, &filter, Some(100))
            .await
            .map_err(|e| e.to_string())?;

        for edge in &results {
            let next = if direction == "callers" {
                &edge.source_symbol
            } else {
                &edge.target_name
            };

            edges_out.push(serde_json::json!({
                "from": if direction == "callers" { &edge.source_symbol } else { &current },
                "to": if direction == "callers" { &current } else { &edge.target_name },
                "source_file": edge.source_file,
                "source_line": edge.source_line,
                "target_file": edge.target_file,
                "depth": current_depth + 1,
            }));

            if !next.is_empty() && !visited.contains(next) {
                visited.insert(next.clone());
                queue.push_back((next.clone(), current_depth + 1));
            }
        }
    }

    Ok(serde_json::json!({
        "symbol": symbol,
        "direction": direction,
        "depth": depth,
        "edge_count": edges_out.len(),
        "edges": edges_out,
    }))
}

// ---------------------------------------------------------------------------
// dependency_graph — file/module import graph
// ---------------------------------------------------------------------------

pub async fn op_dependency_graph(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let repo_id = input
        .get("repo_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: repo_id")?;

    let file_path = input.get("file_path").and_then(|v| v.as_str());

    let st = state.read().await;
    let edges_table = search::ensure_edges_table(&st.db)
        .await
        .map_err(|e| e.to_string())?;

    let filter = if let Some(fp) = file_path {
        let escaped = fp.replace('\'', "''");
        format!("repo_id = '{repo_id}' AND edge_type = 'imports' AND source_file = '{escaped}'")
    } else {
        format!("repo_id = '{repo_id}' AND edge_type = 'imports'")
    };

    let results = search::query_edges(&edges_table, &filter, Some(5000))
        .await
        .map_err(|e| e.to_string())?;

    // Group by source file
    let mut deps: HashMap<String, Vec<Value>> = HashMap::new();
    for edge in &results {
        deps.entry(edge.source_file.clone())
            .or_default()
            .push(serde_json::json!({
                "target": edge.target_name,
                "target_qualified": edge.target_qualified,
                "target_file": edge.target_file,
                "line": edge.source_line,
            }));
    }

    Ok(serde_json::json!({
        "repo_id": repo_id,
        "file_count": deps.len(),
        "total_imports": results.len(),
        "dependencies": deps,
    }))
}

// ---------------------------------------------------------------------------
// type_hierarchy — trait/interface implementations
// ---------------------------------------------------------------------------

pub async fn op_type_hierarchy(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let repo_id = input
        .get("repo_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: repo_id")?;

    let symbol = input.get("symbol").and_then(|v| v.as_str());

    let st = state.read().await;
    let edges_table = search::ensure_edges_table(&st.db)
        .await
        .map_err(|e| e.to_string())?;

    let filter = if let Some(sym) = symbol {
        let escaped = sym.replace('\'', "''");
        format!(
            "repo_id = '{repo_id}' AND edge_type = 'implements' AND (target_name = '{escaped}' OR target_qualified = '{escaped}')"
        )
    } else {
        format!("repo_id = '{repo_id}' AND edge_type = 'implements'")
    };

    let results = search::query_edges(&edges_table, &filter, Some(1000))
        .await
        .map_err(|e| e.to_string())?;

    // Group by trait/interface name
    let mut hierarchy: HashMap<String, Vec<Value>> = HashMap::new();
    for edge in &results {
        let key = if edge.target_qualified.is_empty() {
            &edge.target_name
        } else {
            &edge.target_qualified
        };
        hierarchy
            .entry(key.clone())
            .or_default()
            .push(serde_json::json!({
                "implementor": edge.source_symbol,
                "file": edge.source_file,
                "line": edge.source_line,
            }));
    }

    Ok(serde_json::json!({
        "repo_id": repo_id,
        "trait_count": hierarchy.len(),
        "total_implementations": results.len(),
        "hierarchy": hierarchy,
    }))
}
