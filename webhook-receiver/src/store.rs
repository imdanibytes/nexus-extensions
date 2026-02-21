use crate::types::{Delivery, Webhook};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

const MAX_DELIVERIES_PER_WEBHOOK: usize = 50;

pub struct WebhookStore {
    pub webhooks: HashMap<String, Webhook>,
    pub deliveries: HashMap<String, VecDeque<Delivery>>,
    data_dir: PathBuf,
}

impl WebhookStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            webhooks: HashMap::new(),
            deliveries: HashMap::new(),
            data_dir,
        }
    }

    pub fn load(&mut self) -> Result<(), String> {
        let webhooks_path = self.data_dir.join("webhooks.json");
        if webhooks_path.exists() {
            let content = std::fs::read_to_string(&webhooks_path)
                .map_err(|e| format!("failed to read webhooks.json: {e}"))?;
            let items: Vec<Webhook> = serde_json::from_str(&content)
                .map_err(|e| format!("failed to parse webhooks.json: {e}"))?;
            for wh in items {
                self.webhooks.insert(wh.id.clone(), wh);
            }
        }

        // Load deliveries for each webhook
        let deliveries_dir = self.data_dir.join("deliveries");
        if deliveries_dir.exists() {
            for (id, _) in &self.webhooks {
                let path = deliveries_dir.join(format!("{id}.json"));
                if path.exists() {
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        if let Ok(items) = serde_json::from_str::<Vec<Delivery>>(&content) {
                            self.deliveries
                                .insert(id.clone(), VecDeque::from(items));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn save_webhooks(&self) -> Result<(), String> {
        let path = self.data_dir.join("webhooks.json");
        let items: Vec<&Webhook> = self.webhooks.values().collect();
        let content = serde_json::to_string_pretty(&items)
            .map_err(|e| format!("failed to serialize webhooks: {e}"))?;
        std::fs::write(&path, content).map_err(|e| format!("failed to write webhooks.json: {e}"))
    }

    pub fn save_deliveries(&self, webhook_id: &str) -> Result<(), String> {
        let dir = self.data_dir.join("deliveries");
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("failed to create deliveries dir: {e}"))?;
        let path = dir.join(format!("{webhook_id}.json"));
        let empty = VecDeque::new();
        let deliveries = self.deliveries.get(webhook_id).unwrap_or(&empty);
        let items: Vec<&Delivery> = deliveries.iter().collect();
        let content = serde_json::to_string_pretty(&items)
            .map_err(|e| format!("failed to serialize deliveries: {e}"))?;
        std::fs::write(&path, content)
            .map_err(|e| format!("failed to write deliveries file: {e}"))
    }

    pub fn list(&self) -> Vec<&Webhook> {
        let mut items: Vec<&Webhook> = self.webhooks.values().collect();
        items.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        items
    }

    pub fn get(&self, id: &str) -> Option<&Webhook> {
        self.webhooks.get(id)
    }

    /// Insert a webhook and return a clone of it.
    pub fn insert(&mut self, wh: Webhook) -> Webhook {
        let id = wh.id.clone();
        self.webhooks.insert(id.clone(), wh);
        self.webhooks.get(&id).unwrap().clone()
    }

    pub fn update(&mut self, id: &str, data: serde_json::Value) -> Option<Webhook> {
        let wh = self.webhooks.get_mut(id)?;
        if let Some(v) = data.get("name").and_then(|v| v.as_str()) {
            wh.name = v.to_string();
        }
        if let Some(v) = data.get("event_type").and_then(|v| v.as_str()) {
            wh.event_type = v.to_string();
        }
        if let Some(v) = data.get("verification_method").and_then(|v| v.as_str()) {
            wh.verification_method = v.to_string();
        }
        if data.get("verification_secret").is_some() {
            wh.verification_secret = data["verification_secret"].as_str().map(|s| s.to_string());
        }
        if let Some(v) = data.get("status").and_then(|v| v.as_str()) {
            wh.status = v.to_string();
        }
        Some(wh.clone())
    }

    pub fn delete(&mut self, id: &str) -> bool {
        self.webhooks.remove(id).is_some()
    }

    pub fn record_delivery(&mut self, delivery: Delivery) {
        let id = delivery.webhook_id.clone();
        let queue = self.deliveries.entry(id.clone()).or_default();
        if queue.len() >= MAX_DELIVERIES_PER_WEBHOOK {
            queue.pop_front();
        }
        queue.push_back(delivery);

        // Update trigger count + last_triggered
        if let Some(wh) = self.webhooks.get_mut(&id) {
            wh.trigger_count += 1;
            wh.last_triggered = Some(Utc::now().to_rfc3339());
        }
    }

    pub fn get_deliveries(&self, webhook_id: &str) -> Vec<&Delivery> {
        self.deliveries
            .get(webhook_id)
            .map(|q| q.iter().rev().collect())
            .unwrap_or_default()
    }

}
