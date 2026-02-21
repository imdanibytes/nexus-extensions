use anyhow::{anyhow, Result};
use arrow_array::{
    builder::FixedSizeListBuilder, builder::Float32Builder, Array, Float32Array,
    RecordBatch, RecordBatchIterator, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use std::sync::Arc;

/// Build the Arrow schema for the chunks table.
pub fn chunks_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("repo_id", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("symbol_name", DataType::Utf8, false),
        Field::new("symbol_type", DataType::Utf8, false),
        Field::new("start_line", DataType::UInt32, false),
        Field::new("end_line", DataType::UInt32, false),
        Field::new("content", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions as i32,
            ),
            true,
        ),
    ]))
}

/// Represents a single chunk to insert.
pub struct ChunkRecord {
    pub id: String,
    pub repo_id: String,
    pub file_path: String,
    pub language: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
    pub vector: Option<Vec<f32>>,
}

/// Build a RecordBatch from a slice of ChunkRecords.
pub fn build_record_batch(chunks: &[ChunkRecord], dimensions: usize) -> Result<RecordBatch> {
    let schema = chunks_schema(dimensions);

    let ids = StringArray::from(chunks.iter().map(|c| c.id.as_str()).collect::<Vec<_>>());
    let repo_ids =
        StringArray::from(chunks.iter().map(|c| c.repo_id.as_str()).collect::<Vec<_>>());
    let file_paths =
        StringArray::from(chunks.iter().map(|c| c.file_path.as_str()).collect::<Vec<_>>());
    let languages =
        StringArray::from(chunks.iter().map(|c| c.language.as_str()).collect::<Vec<_>>());
    let symbol_names =
        StringArray::from(chunks.iter().map(|c| c.symbol_name.as_str()).collect::<Vec<_>>());
    let symbol_types =
        StringArray::from(chunks.iter().map(|c| c.symbol_type.as_str()).collect::<Vec<_>>());
    let start_lines =
        UInt32Array::from(chunks.iter().map(|c| c.start_line).collect::<Vec<_>>());
    let end_lines = UInt32Array::from(chunks.iter().map(|c| c.end_line).collect::<Vec<_>>());
    let contents =
        StringArray::from(chunks.iter().map(|c| c.content.as_str()).collect::<Vec<_>>());

    // Build the FixedSizeList vector column using a builder
    let dim = dimensions as i32;
    let mut list_builder =
        FixedSizeListBuilder::new(Float32Builder::new(), dim);

    for chunk in chunks {
        match &chunk.vector {
            Some(vec) => {
                let values = list_builder.values();
                for &v in vec {
                    values.append_value(v);
                }
                list_builder.append(true);
            }
            None => {
                let values = list_builder.values();
                for _ in 0..dimensions {
                    values.append_null();
                }
                list_builder.append(false);
            }
        }
    }

    let vector_col = list_builder.finish();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(repo_ids),
            Arc::new(file_paths),
            Arc::new(languages),
            Arc::new(symbol_names),
            Arc::new(symbol_types),
            Arc::new(start_lines),
            Arc::new(end_lines),
            Arc::new(contents),
            Arc::new(vector_col),
        ],
    )?;

    Ok(batch)
}

/// Ensure the chunks table exists, creating it if needed.
pub async fn ensure_table(db: &lancedb::Connection, dimensions: usize) -> Result<lancedb::Table> {
    let schema = chunks_schema(dimensions);

    match db.open_table("chunks").execute().await {
        Ok(table) => Ok(table),
        Err(_) => {
            // Create empty table with schema
            let empty_batch = RecordBatch::new_empty(schema.clone());
            let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], schema);
            let table = db.create_table("chunks", Box::new(batches)).execute().await?;
            Ok(table)
        }
    }
}

/// Insert or upsert chunks into the table.
pub async fn upsert_chunks(
    table: &lancedb::Table,
    chunks: &[ChunkRecord],
    dimensions: usize,
) -> Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }
    let batch = build_record_batch(chunks, dimensions)?;
    let schema = chunks_schema(dimensions);
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema);

    let mut merge = table.merge_insert(&["id"]);
    merge.when_matched_update_all(None);
    merge.when_not_matched_insert_all();
    merge.execute(Box::new(batches)).await?;

    Ok(())
}

/// Delete all chunks for a given repo.
pub async fn delete_repo_chunks(table: &lancedb::Table, repo_id: &str) -> Result<()> {
    table
        .delete(&format!("repo_id = '{repo_id}'"))
        .await?;
    Ok(())
}

/// Delete chunks for specific files in a repo.
pub async fn delete_file_chunks(
    table: &lancedb::Table,
    repo_id: &str,
    file_paths: &[String],
) -> Result<()> {
    for path in file_paths {
        let escaped = path.replace('\'', "''");
        table
            .delete(&format!("repo_id = '{repo_id}' AND file_path = '{escaped}'"))
            .await?;
    }
    Ok(())
}

/// Search result returned to callers.
pub struct SearchResult {
    pub file_path: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub start_line: u32,
    pub end_line: u32,
    pub score: f32,
    pub snippet: String,
    pub content: Option<String>,
    pub repo_id: String,
    pub language: String,
}

/// Perform a vector search.
pub async fn vector_search(
    table: &lancedb::Table,
    query_vec: &[f32],
    filter: Option<String>,
    limit: usize,
    full_content: bool,
) -> Result<Vec<SearchResult>> {
    let mut builder = table
        .vector_search(query_vec)
        .map_err(|e| anyhow!("Vector search setup failed: {e}"))?;

    if let Some(ref f) = filter {
        builder = builder.only_if(f.clone());
    }

    let results = builder.limit(limit).execute().await?;

    let batches: Vec<RecordBatch> = results.try_collect().await?;

    let mut search_results = Vec::new();
    for batch in &batches {
        let file_paths = batch
            .column_by_name("file_path")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing file_path column"))?;
        let symbol_names = batch
            .column_by_name("symbol_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing symbol_name column"))?;
        let symbol_types = batch
            .column_by_name("symbol_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing symbol_type column"))?;
        let start_lines = batch
            .column_by_name("start_line")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| anyhow!("Missing start_line column"))?;
        let end_lines = batch
            .column_by_name("end_line")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| anyhow!("Missing end_line column"))?;
        let contents = batch
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing content column"))?;
        let repo_ids = batch
            .column_by_name("repo_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing repo_id column"))?;
        let languages = batch
            .column_by_name("language")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing language column"))?;
        let distances = batch
            .column_by_name("_distance")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

        for i in 0..batch.num_rows() {
            let content_str = contents.value(i);
            let snippet: String = content_str.chars().take(300).collect();
            let score = distances.map(|d| 1.0 - d.value(i)).unwrap_or(0.0);

            search_results.push(SearchResult {
                file_path: file_paths.value(i).to_string(),
                symbol_name: symbol_names.value(i).to_string(),
                symbol_type: symbol_types.value(i).to_string(),
                start_line: start_lines.value(i),
                end_line: end_lines.value(i),
                score,
                snippet,
                content: if full_content {
                    Some(content_str.to_string())
                } else {
                    None
                },
                repo_id: repo_ids.value(i).to_string(),
                language: languages.value(i).to_string(),
            });
        }
    }

    Ok(search_results)
}

/// Scan chunks for the map operation (no vector search).
pub async fn scan_symbols(
    table: &lancedb::Table,
    repo_id: &str,
    language_filter: Option<&str>,
    symbol_type_filter: Option<&str>,
) -> Result<Vec<MapEntry>> {
    let mut filter_parts = vec![format!("repo_id = '{repo_id}'")];
    if let Some(lang) = language_filter {
        filter_parts.push(format!("language = '{lang}'"));
    }
    if let Some(st) = symbol_type_filter {
        filter_parts.push(format!("symbol_type = '{st}'"));
    }
    let filter = filter_parts.join(" AND ");

    let results = table
        .query()
        .only_if(filter)
        .select(lancedb::query::Select::Columns(vec![
            "file_path".into(),
            "symbol_name".into(),
            "symbol_type".into(),
            "start_line".into(),
            "end_line".into(),
            "language".into(),
        ]))
        .execute()
        .await?;

    let batches: Vec<RecordBatch> = results.try_collect().await?;

    let mut entries = Vec::new();
    for batch in &batches {
        let file_paths = batch
            .column_by_name("file_path")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing file_path column"))?;
        let symbol_names = batch
            .column_by_name("symbol_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing symbol_name column"))?;
        let symbol_types = batch
            .column_by_name("symbol_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing symbol_type column"))?;
        let start_lines = batch
            .column_by_name("start_line")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| anyhow!("Missing start_line column"))?;
        let end_lines = batch
            .column_by_name("end_line")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| anyhow!("Missing end_line column"))?;

        for i in 0..batch.num_rows() {
            entries.push(MapEntry {
                file_path: file_paths.value(i).to_string(),
                symbol_name: symbol_names.value(i).to_string(),
                symbol_type: symbol_types.value(i).to_string(),
                start_line: start_lines.value(i),
                end_line: end_lines.value(i),
            });
        }
    }

    Ok(entries)
}

pub struct MapEntry {
    pub file_path: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub start_line: u32,
    pub end_line: u32,
}

/// Count total rows in the table.
pub async fn count_rows(table: &lancedb::Table) -> Result<u64> {
    let count = table.count_rows(None).await?;
    Ok(count as u64)
}

/// Count rows matching a filter.
pub async fn count_rows_filtered(table: &lancedb::Table, filter: &str) -> Result<u64> {
    let count = table.count_rows(Some(filter.to_string())).await?;
    Ok(count as u64)
}

// ---------------------------------------------------------------------------
// Edges table (symbol graph)
// ---------------------------------------------------------------------------

/// Arrow schema for the edges table — pure relational, no vector column.
pub fn edges_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("repo_id", DataType::Utf8, false),
        Field::new("source_file", DataType::Utf8, false),
        Field::new("source_symbol", DataType::Utf8, false),
        Field::new("source_line", DataType::UInt32, false),
        Field::new("target_name", DataType::Utf8, false),
        Field::new("target_qualified", DataType::Utf8, false),
        Field::new("target_file", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
    ]))
}

/// A single edge to insert.
pub struct EdgeRecord {
    pub id: String,
    pub repo_id: String,
    pub source_file: String,
    pub source_symbol: String,
    pub source_line: u32,
    pub target_name: String,
    pub target_qualified: String,
    pub target_file: String,
    pub edge_type: String,
}

/// Build a RecordBatch from a slice of EdgeRecords.
pub fn build_edge_batch(edges: &[EdgeRecord]) -> Result<RecordBatch> {
    let schema = edges_schema();

    let ids = StringArray::from(edges.iter().map(|e| e.id.as_str()).collect::<Vec<_>>());
    let repo_ids = StringArray::from(edges.iter().map(|e| e.repo_id.as_str()).collect::<Vec<_>>());
    let source_files =
        StringArray::from(edges.iter().map(|e| e.source_file.as_str()).collect::<Vec<_>>());
    let source_symbols =
        StringArray::from(edges.iter().map(|e| e.source_symbol.as_str()).collect::<Vec<_>>());
    let source_lines =
        UInt32Array::from(edges.iter().map(|e| e.source_line).collect::<Vec<_>>());
    let target_names =
        StringArray::from(edges.iter().map(|e| e.target_name.as_str()).collect::<Vec<_>>());
    let target_qualifieds =
        StringArray::from(edges.iter().map(|e| e.target_qualified.as_str()).collect::<Vec<_>>());
    let target_files =
        StringArray::from(edges.iter().map(|e| e.target_file.as_str()).collect::<Vec<_>>());
    let edge_types =
        StringArray::from(edges.iter().map(|e| e.edge_type.as_str()).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(repo_ids),
            Arc::new(source_files),
            Arc::new(source_symbols),
            Arc::new(source_lines),
            Arc::new(target_names),
            Arc::new(target_qualifieds),
            Arc::new(target_files),
            Arc::new(edge_types),
        ],
    )?;

    Ok(batch)
}

/// Ensure the edges table exists, creating it if needed.
pub async fn ensure_edges_table(db: &lancedb::Connection) -> Result<lancedb::Table> {
    let schema = edges_schema();

    match db.open_table("edges").execute().await {
        Ok(table) => Ok(table),
        Err(_) => {
            let empty_batch = RecordBatch::new_empty(schema.clone());
            let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], schema);
            let table = db.create_table("edges", Box::new(batches)).execute().await?;
            Ok(table)
        }
    }
}

/// Insert or upsert edges into the table.
pub async fn upsert_edges(table: &lancedb::Table, edges: &[EdgeRecord]) -> Result<()> {
    if edges.is_empty() {
        return Ok(());
    }
    let batch = build_edge_batch(edges)?;
    let schema = edges_schema();
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema);

    let mut merge = table.merge_insert(&["id"]);
    merge.when_matched_update_all(None);
    merge.when_not_matched_insert_all();
    merge.execute(Box::new(batches)).await?;

    Ok(())
}

/// Delete all edges for a given repo.
pub async fn delete_repo_edges(table: &lancedb::Table, repo_id: &str) -> Result<()> {
    table.delete(&format!("repo_id = '{repo_id}'")).await?;
    Ok(())
}

/// Delete edges originating from specific files in a repo.
#[allow(dead_code)]
pub async fn delete_file_edges(
    table: &lancedb::Table,
    repo_id: &str,
    file_paths: &[String],
) -> Result<()> {
    for path in file_paths {
        let escaped = path.replace('\'', "''");
        table
            .delete(&format!(
                "repo_id = '{repo_id}' AND source_file = '{escaped}'"
            ))
            .await?;
    }
    Ok(())
}

/// Result of an edge query.
#[allow(dead_code)]
pub struct EdgeQueryResult {
    pub id: String,
    pub repo_id: String,
    pub source_file: String,
    pub source_symbol: String,
    pub source_line: u32,
    pub target_name: String,
    pub target_qualified: String,
    pub target_file: String,
    pub edge_type: String,
}

/// Query edges with a SQL-like filter.
pub async fn query_edges(
    table: &lancedb::Table,
    filter: &str,
    limit: Option<usize>,
) -> Result<Vec<EdgeQueryResult>> {
    let mut builder = table.query().only_if(filter);
    if let Some(lim) = limit {
        builder = builder.limit(lim);
    }

    let results = builder.execute().await?;
    let batches: Vec<RecordBatch> = results.try_collect().await?;

    let mut edges = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing id column"))?;
        let repo_ids = batch
            .column_by_name("repo_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing repo_id column"))?;
        let source_files = batch
            .column_by_name("source_file")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing source_file column"))?;
        let source_symbols = batch
            .column_by_name("source_symbol")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing source_symbol column"))?;
        let source_lines = batch
            .column_by_name("source_line")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| anyhow!("Missing source_line column"))?;
        let target_names = batch
            .column_by_name("target_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing target_name column"))?;
        let target_qualifieds = batch
            .column_by_name("target_qualified")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing target_qualified column"))?;
        let target_files = batch
            .column_by_name("target_file")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing target_file column"))?;
        let edge_types = batch
            .column_by_name("edge_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow!("Missing edge_type column"))?;

        for i in 0..batch.num_rows() {
            edges.push(EdgeQueryResult {
                id: ids.value(i).to_string(),
                repo_id: repo_ids.value(i).to_string(),
                source_file: source_files.value(i).to_string(),
                source_symbol: source_symbols.value(i).to_string(),
                source_line: source_lines.value(i),
                target_name: target_names.value(i).to_string(),
                target_qualified: target_qualifieds.value(i).to_string(),
                target_file: target_files.value(i).to_string(),
                edge_type: edge_types.value(i).to_string(),
            });
        }
    }

    Ok(edges)
}

/// Count edges matching a filter.
#[allow(dead_code)]
pub async fn count_edges_filtered(table: &lancedb::Table, filter: &str) -> Result<u64> {
    let count = table.count_rows(Some(filter.to_string())).await?;
    Ok(count as u64)
}
