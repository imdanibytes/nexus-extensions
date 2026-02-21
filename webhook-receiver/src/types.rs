use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    pub name: String,
    pub event_type: String,
    pub verification_method: String,
    pub verification_secret: Option<String>,
    pub status: String,
    pub url: String,
    pub trigger_count: u64,
    pub last_triggered: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delivery {
    pub id: String,
    pub webhook_id: String,
    pub received_at: DateTime<Utc>,
    pub status: String, // "success" | "verification_failed"
    pub headers: serde_json::Value,
    pub body: serde_json::Value,
}

/// A webhook delivery queued for event.publish, not yet sent to host.
#[derive(Debug, Clone)]
pub struct PendingEvent {
    pub event_type: String,
    pub subject: String,
    pub data: serde_json::Value,
}
