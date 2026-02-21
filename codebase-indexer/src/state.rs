use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::watch;

use crate::embeddings::{self, EmbeddingProvider, ProviderConfig};

/// Repository metadata persisted in metadata.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoMeta {
    pub id: String,
    pub path: String,
    pub name: String,
    pub chunk_count: u64,
    pub embed_pending: bool,
    pub last_indexed_commit: Option<String>,
    pub last_indexed_at: Option<DateTime<Utc>>,
    pub last_sync_error: Option<String>,
    pub indexing: bool,
    #[serde(default)]
    pub graph_edge_count: u64,
    #[serde(default)]
    pub graph_indexed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_graph_commit: Option<String>,
    #[serde(default)]
    pub graph_building: bool,
}

/// Workspace: a named grouping of repositories.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceMeta {
    pub name: String,
    pub description: String,
    pub repo_ids: Vec<String>,
}

/// Top-level persistent metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub embedding: ProviderConfig,
    #[serde(default)]
    pub repositories: HashMap<String, RepoMeta>,
    #[serde(default)]
    pub workspaces: HashMap<String, WorkspaceMeta>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            embedding: ProviderConfig::default(),
            repositories: HashMap::new(),
            workspaces: HashMap::new(),
        }
    }
}

/// Runtime state — held behind Arc<RwLock<AppState>>.
pub struct AppState {
    pub data_dir: PathBuf,
    pub metadata: Metadata,
    pub provider: Box<dyn EmbeddingProvider>,
    pub db: lancedb::Connection,
    /// Cancellation channels for background indexing tasks, keyed by repo_id.
    pub indexing_cancels: HashMap<String, watch::Sender<bool>>,
    /// Cancellation channels for background graph building tasks, keyed by repo_id.
    pub graph_cancels: HashMap<String, watch::Sender<bool>>,
}

impl AppState {
    pub async fn initialize(data_dir: PathBuf) -> Result<Self> {
        // Load or create metadata
        let meta_path = data_dir.join("metadata.json");
        let metadata = if meta_path.exists() {
            let raw = tokio::fs::read_to_string(&meta_path).await?;
            serde_json::from_str(&raw).unwrap_or_default()
        } else {
            Metadata::default()
        };

        // Create embedding provider
        let provider = embeddings::create_provider(&metadata.embedding)?;

        // Open LanceDB
        let db_path = data_dir.join("index.lance");
        tokio::fs::create_dir_all(&db_path).await.ok();
        let db = lancedb::connect(db_path.to_str().unwrap()).execute().await?;

        Ok(Self {
            data_dir,
            metadata,
            provider,
            db,
            indexing_cancels: HashMap::new(),
            graph_cancels: HashMap::new(),
        })
    }

    /// Persist metadata to disk.
    pub async fn save_metadata(&self) -> Result<()> {
        let path = self.data_dir.join("metadata.json");
        let json = serde_json::to_string_pretty(&self.metadata)?;
        tokio::fs::write(path, json).await?;
        Ok(())
    }

    /// Check if a repo is currently being indexed.
    pub fn is_indexing(&self, repo_id: &str) -> bool {
        self.indexing_cancels.contains_key(repo_id)
    }

    /// Check if a repo's graph is currently being built.
    #[allow(dead_code)]
    pub fn is_graph_building(&self, repo_id: &str) -> bool {
        self.graph_cancels.contains_key(repo_id)
    }
}
