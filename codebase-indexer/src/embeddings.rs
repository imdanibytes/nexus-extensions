use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Configuration for the embedding provider, persisted in metadata.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub provider: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_dimensions")]
    pub dimensions: usize,
    pub aws_region: Option<String>,
    pub aws_profile: Option<String>,
}

fn default_base_url() -> String {
    "http://localhost:11434".into()
}
fn default_model() -> String {
    "nomic-embed-text".into()
}
fn default_dimensions() -> usize {
    768
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            provider: "ollama".into(),
            base_url: default_base_url(),
            model: default_model(),
            dimensions: default_dimensions(),
            aws_region: None,
            aws_profile: None,
        }
    }
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
    fn dimensions(&self) -> usize;
    fn model_id(&self) -> &str;
    fn provider_type(&self) -> &str;
    async fn health_check(&self) -> bool;
}

pub fn create_provider(config: &ProviderConfig) -> Result<Box<dyn EmbeddingProvider>> {
    match config.provider.as_str() {
        "ollama" => Ok(Box::new(OllamaProvider::new(config))),
        "bedrock" => Ok(Box::new(BedrockProvider::new(config)?)),
        other => Err(anyhow!("Unknown embedding provider: {other}")),
    }
}

// ---------------------------------------------------------------------------
// Ollama
// ---------------------------------------------------------------------------

pub struct OllamaProvider {
    client: reqwest::Client,
    base_url: String,
    model: String,
    dimensions: usize,
}

#[derive(Serialize)]
struct OllamaEmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Deserialize)]
struct OllamaEmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

impl OllamaProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: config.base_url.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            dimensions: config.dimensions,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OllamaProvider {
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let url = format!("{}/api/embed", self.base_url);
        let body = OllamaEmbedRequest {
            model: self.model.clone(),
            input: texts.to_vec(),
        };
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("Ollama request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Ollama returned {status}: {text}"));
        }

        let data: OllamaEmbedResponse = resp
            .json()
            .await
            .map_err(|e| anyhow!("Ollama response parse error: {e}"))?;

        Ok(data.embeddings)
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }
    fn model_id(&self) -> &str {
        &self.model
    }
    fn provider_type(&self) -> &str {
        "ollama"
    }

    async fn health_check(&self) -> bool {
        let url = format!("{}/api/tags", self.base_url);
        self.client
            .get(&url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// AWS Bedrock
// ---------------------------------------------------------------------------

pub struct BedrockProvider {
    model: String,
    dimensions: usize,
    region: String,
    profile: Option<String>,
}

impl BedrockProvider {
    pub fn new(config: &ProviderConfig) -> Result<Self> {
        Ok(Self {
            model: if config.model == default_model() {
                "amazon.titan-embed-text-v2:0".into()
            } else {
                config.model.clone()
            },
            dimensions: if config.dimensions == default_dimensions() {
                1024
            } else {
                config.dimensions
            },
            region: config.aws_region.clone().unwrap_or_else(|| "us-east-1".into()),
            profile: config.aws_profile.clone(),
        })
    }

    async fn build_client(&self) -> aws_sdk_bedrockruntime::Client {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()));
        if let Some(ref profile) = self.profile {
            loader = loader.profile_name(profile);
        }
        let config = loader.load().await;
        aws_sdk_bedrockruntime::Client::new(&config)
    }
}

#[async_trait]
impl EmbeddingProvider for BedrockProvider {
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let client = self.build_client().await;
        let mut results = Vec::with_capacity(texts.len());

        // Bedrock Titan embed doesn't support batching — one call per text
        for text in texts {
            let body = serde_json::json!({
                "inputText": text,
                "dimensions": self.dimensions,
            });
            let blob = aws_sdk_bedrockruntime::primitives::Blob::new(
                serde_json::to_vec(&body)?,
            );

            let resp = client
                .invoke_model()
                .model_id(&self.model)
                .content_type("application/json")
                .accept("application/json")
                .body(blob)
                .send()
                .await
                .map_err(|e| anyhow!("Bedrock invoke failed: {e}"))?;

            let resp_bytes = resp.body().as_ref();
            let resp_json: serde_json::Value = serde_json::from_slice(resp_bytes)?;
            let embedding: Vec<f32> = resp_json
                .get("embedding")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .ok_or_else(|| anyhow!("Bedrock response missing 'embedding' field"))?;

            results.push(embedding);
        }

        Ok(results)
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }
    fn model_id(&self) -> &str {
        &self.model
    }
    fn provider_type(&self) -> &str {
        "bedrock"
    }

    async fn health_check(&self) -> bool {
        // Try a minimal invocation to check connectivity
        let client = self.build_client().await;
        let body = serde_json::json!({
            "inputText": "health check",
            "dimensions": self.dimensions,
        });
        let blob = aws_sdk_bedrockruntime::primitives::Blob::new(
            serde_json::to_vec(&body).unwrap_or_default(),
        );
        client
            .invoke_model()
            .model_id(&self.model)
            .content_type("application/json")
            .accept("application/json")
            .body(blob)
            .send()
            .await
            .is_ok()
    }
}
