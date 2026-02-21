use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::fs;

const MAX_SCHEDULES: usize = 100;
const MAX_HISTORY: usize = 100;

#[derive(Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub name: String,
    pub event_type: String,
    pub event_data: Value,
    pub schedule_type: String,
    pub cron_expression: Option<String>,
    pub interval_seconds: Option<u64>,
    pub run_at: Option<String>,
    pub timezone: String,
    pub status: String,
    pub last_fired: Option<String>,
    pub next_fire: Option<String>,
    pub fire_count: u64,
    pub created_at: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FireRecord {
    pub fire_time: String,
    pub event_id: String,
}

pub struct ScheduleStore {
    pub schedules: Vec<Schedule>,
    data_dir: PathBuf,
}

impl ScheduleStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            schedules: Vec::new(),
            data_dir,
        }
    }

    pub async fn load(&mut self) -> Result<(), String> {
        let path = self.data_dir.join("schedules.json");
        if !path.exists() {
            return Ok(());
        }
        let content = fs::read_to_string(&path)
            .await
            .map_err(|e| format!("read schedules.json: {e}"))?;
        self.schedules = serde_json::from_str(&content)
            .map_err(|e| format!("parse schedules.json: {e}"))?;
        Ok(())
    }

    pub async fn save(&self) -> Result<(), String> {
        fs::create_dir_all(&self.data_dir)
            .await
            .map_err(|e| format!("create data dir: {e}"))?;
        let path = self.data_dir.join("schedules.json");
        let content = serde_json::to_string_pretty(&self.schedules)
            .map_err(|e| format!("serialize schedules: {e}"))?;
        fs::write(&path, content)
            .await
            .map_err(|e| format!("write schedules.json: {e}"))?;
        Ok(())
    }

    pub fn list(&self) -> &[Schedule] {
        &self.schedules
    }

    pub fn get(&self, id: &str) -> Option<&Schedule> {
        self.schedules.iter().find(|s| s.id == id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Schedule> {
        self.schedules.iter_mut().find(|s| s.id == id)
    }

    pub fn create(&mut self, schedule: Schedule) -> Result<&Schedule, String> {
        if self.schedules.len() >= MAX_SCHEDULES {
            return Err(format!("max schedules ({MAX_SCHEDULES}) reached"));
        }
        self.schedules.push(schedule);
        Ok(self.schedules.last().unwrap())
    }

    pub fn update(&mut self, id: &str, data: Value) -> Result<Schedule, String> {
        let s = self.schedules.iter_mut().find(|s| s.id == id)
            .ok_or_else(|| format!("schedule not found: {id}"))?;

        if let Some(v) = data.get("name").and_then(|v| v.as_str()) {
            s.name = v.to_string();
        }
        if let Some(v) = data.get("event_type").and_then(|v| v.as_str()) {
            s.event_type = v.to_string();
        }
        if let Some(v) = data.get("event_data") {
            s.event_data = v.clone();
        }
        if let Some(v) = data.get("schedule_type").and_then(|v| v.as_str()) {
            s.schedule_type = v.to_string();
        }
        if let Some(v) = data.get("cron_expression") {
            s.cron_expression = v.as_str().map(|s| s.to_string());
        }
        if let Some(v) = data.get("interval_seconds") {
            s.interval_seconds = v.as_u64();
        }
        if let Some(v) = data.get("run_at") {
            s.run_at = v.as_str().map(|s| s.to_string());
        }
        if let Some(v) = data.get("timezone").and_then(|v| v.as_str()) {
            s.timezone = v.to_string();
        }
        if let Some(v) = data.get("status").and_then(|v| v.as_str()) {
            s.status = v.to_string();
        }

        Ok(s.clone())
    }

    pub fn delete(&mut self, id: &str) -> Result<(), String> {
        let pos = self.schedules.iter().position(|s| s.id == id)
            .ok_or_else(|| format!("schedule not found: {id}"))?;
        self.schedules.remove(pos);
        Ok(())
    }

    pub async fn append_history(&self, schedule_id: &str, record: FireRecord) -> Result<(), String> {
        let history_dir = self.data_dir.join("history");
        fs::create_dir_all(&history_dir)
            .await
            .map_err(|e| format!("create history dir: {e}"))?;

        let path = history_dir.join(format!("{schedule_id}.json"));

        let mut entries: VecDeque<FireRecord> = if path.exists() {
            let content = fs::read_to_string(&path)
                .await
                .map_err(|e| format!("read history: {e}"))?;
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            VecDeque::new()
        };

        entries.push_back(record);
        while entries.len() > MAX_HISTORY {
            entries.pop_front();
        }

        let content = serde_json::to_string_pretty(&entries)
            .map_err(|e| format!("serialize history: {e}"))?;
        fs::write(&path, content)
            .await
            .map_err(|e| format!("write history: {e}"))?;

        Ok(())
    }

    pub async fn get_history(&self, schedule_id: &str) -> Result<Vec<FireRecord>, String> {
        let path = self.data_dir.join("history").join(format!("{schedule_id}.json"));
        if !path.exists() {
            return Ok(Vec::new());
        }
        let content = fs::read_to_string(&path)
            .await
            .map_err(|e| format!("read history: {e}"))?;
        let entries: VecDeque<FireRecord> = serde_json::from_str(&content)
            .map_err(|e| format!("parse history: {e}"))?;
        Ok(entries.into_iter().collect())
    }
}
