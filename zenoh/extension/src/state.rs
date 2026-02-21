use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use tokio::sync::watch;

/// Metadata tracked per discovered key expression (no payload buffering).
#[derive(Clone, Serialize)]
pub struct TopicMeta {
    pub key_expr: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub sample_count: u64,
    pub total_payload_bytes: u64,
    pub last_encoding: String,
}

impl TopicMeta {
    pub fn new(key_expr: String, encoding: String, payload_len: u64) -> Self {
        let now = Utc::now();
        Self {
            key_expr,
            first_seen: now,
            last_seen: now,
            sample_count: 1,
            total_payload_bytes: payload_len,
            last_encoding: encoding,
        }
    }

    pub fn update(&mut self, encoding: String, payload_len: u64) {
        self.last_seen = Utc::now();
        self.sample_count += 1;
        self.total_payload_bytes += payload_len;
        self.last_encoding = encoding;
    }

    pub fn rate_hz(&self) -> f64 {
        let elapsed = (self.last_seen - self.first_seen).num_milliseconds() as f64 / 1000.0;
        if elapsed < 0.001 {
            0.0
        } else {
            self.sample_count as f64 / elapsed
        }
    }

    pub fn avg_payload_size(&self) -> u64 {
        if self.sample_count == 0 {
            0
        } else {
            self.total_payload_bytes / self.sample_count
        }
    }
}

/// A single buffered sample from a subscription.
#[derive(Clone, Serialize)]
pub struct BufferedSample {
    pub key_expr: String,
    pub payload_b64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_str: Option<String>,
    pub encoding: String,
    pub timestamp: DateTime<Utc>,
}

/// An active subscription with a bounded ring buffer.
pub struct Subscription {
    pub key_expr: String,
    pub buffer: VecDeque<BufferedSample>,
    pub buffer_capacity: usize,
    pub overflow_count: u64,
    pub total_received: u64,
    pub created_at: DateTime<Utc>,
    pub cancel: watch::Sender<bool>,
}

impl Subscription {
    pub fn new(key_expr: String, buffer_capacity: usize, cancel: watch::Sender<bool>) -> Self {
        Self {
            key_expr,
            buffer: VecDeque::with_capacity(buffer_capacity),
            buffer_capacity,
            overflow_count: 0,
            total_received: 0,
            created_at: Utc::now(),
            cancel,
        }
    }

    pub fn push(&mut self, sample: BufferedSample) {
        self.total_received += 1;
        if self.buffer.len() >= self.buffer_capacity {
            self.buffer.pop_front();
            self.overflow_count += 1;
        }
        self.buffer.push_back(sample);
    }

    pub fn drain(&mut self, limit: usize) -> Vec<BufferedSample> {
        let n = limit.min(self.buffer.len());
        self.buffer.drain(..n).collect()
    }
}

/// Top-level shared state behind Arc<RwLock>.
pub struct AppState {
    pub topics: HashMap<String, TopicMeta>,
    pub subscriptions: HashMap<String, Subscription>,
    pub discovery_active: bool,
    pub discovery_cancel: Option<watch::Sender<bool>>,
    pub discovery_key_expr: String,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            subscriptions: HashMap::new(),
            discovery_active: false,
            discovery_cancel: None,
            discovery_key_expr: String::new(),
        }
    }
}
