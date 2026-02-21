mod next_fire;
mod store;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use next_fire::compute_next_fire;
use store::{FireRecord, Schedule, ScheduleStore};

// ---------------------------------------------------------------------------
// JSON-RPC types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
    id: u64,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: u64,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

fn ok_response(id: u64, data: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        result: Some(serde_json::json!({
            "success": true,
            "data": data,
            "message": null
        })),
        error: None,
        id,
    }
}

fn err_response(id: u64, code: i64, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        result: None,
        error: Some(JsonRpcError { code, message }),
        id,
    }
}

// ---------------------------------------------------------------------------
// Stdout writer task
//
// All output goes through a channel to a single owner of stdout, ensuring
// serialized writes without needing to share a non-Send StdoutLock.
// ---------------------------------------------------------------------------

type StdoutTx = mpsc::Sender<String>;

fn spawn_stdout_writer() -> StdoutTx {
    let (tx, mut rx) = mpsc::channel::<String>(256);
    std::thread::spawn(move || {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        while let Some(line) = rx.blocking_recv() {
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        }
    });
    tx
}

async fn send_line(tx: &StdoutTx, line: String) {
    let _ = tx.send(line).await;
}

// ---------------------------------------------------------------------------
// Data dir resolution
// ---------------------------------------------------------------------------

fn data_dir() -> PathBuf {
    if let Ok(d) = std::env::var("DATA_DIR") {
        return PathBuf::from(d);
    }
    let mut p = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    p.pop();
    p.join("data")
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let store = Arc::new(Mutex::new(ScheduleStore::new(data_dir())));

    let stdout_tx = spawn_stdout_writer();

    let ipc_id_counter = Arc::new(std::sync::atomic::AtomicU64::new(1000));

    // Spawn timer loop
    let store_timer = store.clone();
    let stdout_timer = stdout_tx.clone();
    let ipc_counter_timer = ipc_id_counter.clone();
    tokio::spawn(async move {
        timer_loop(store_timer, stdout_timer, ipc_counter_timer).await;
    });

    // Stdin reader runs in a blocking thread
    let store_main = store.clone();
    let stdout_main = stdout_tx.clone();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        let mut lines = stdin.lock().lines();

        loop {
            let line = match lines.next() {
                Some(Ok(l)) => l,
                _ => break,
            };

            if line.trim().is_empty() {
                continue;
            }

            let request: JsonRpcRequest = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(e) => {
                    let resp = err_response(0, -32700, format!("Parse error: {e}"));
                    handle.block_on(send_line(
                        &stdout_main,
                        serde_json::to_string(&resp).unwrap(),
                    ));
                    continue;
                }
            };

            let is_shutdown = request.method == "shutdown";

            let response = handle.block_on(handle_request(
                &request,
                store_main.clone(),
                stdout_main.clone(),
            ));

            handle.block_on(send_line(
                &stdout_main,
                serde_json::to_string(&response).unwrap(),
            ));

            if is_shutdown {
                break;
            }
        }
    })
    .await
    .ok();
}

// ---------------------------------------------------------------------------
// Request dispatcher
// ---------------------------------------------------------------------------

async fn handle_request(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
    stdout: StdoutTx,
) -> JsonRpcResponse {
    match req.method.as_str() {
        "initialize" => {
            let mut st = store.lock().await;
            if let Err(e) = st.load().await {
                eprintln!("scheduler: failed to load schedules: {e}");
            }
            let now = Utc::now();
            for s in st.schedules.iter_mut() {
                if s.status == "active" {
                    s.next_fire = compute_next_fire(s, now);
                }
            }
            if let Err(e) = st.save().await {
                eprintln!("scheduler: failed to save after init: {e}");
            }
            JsonRpcResponse {
                jsonrpc: "2.0",
                result: Some(serde_json::json!({ "ready": true })),
                error: None,
                id: req.id,
            }
        }

        "shutdown" => {
            let st = store.lock().await;
            if let Err(e) = st.save().await {
                eprintln!("scheduler: failed to save on shutdown: {e}");
            }
            JsonRpcResponse {
                jsonrpc: "2.0",
                result: Some(serde_json::json!({})),
                error: None,
                id: req.id,
            }
        }

        "execute" => handle_execute(req, store, stdout).await,

        "resources.list" => handle_resources_list(req, store).await,
        "resources.get" => handle_resources_get(req, store).await,
        "resources.create" => handle_resources_create(req, store).await,
        "resources.update" => handle_resources_update(req, store).await,
        "resources.delete" => handle_resources_delete(req, store).await,

        _ => err_response(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

// ---------------------------------------------------------------------------
// Execute operations
// ---------------------------------------------------------------------------

async fn handle_execute(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
    stdout: StdoutTx,
) -> JsonRpcResponse {
    let operation = req
        .params
        .get("operation")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let input = req
        .params
        .get("input")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));

    let result = match operation {
        "trigger_now" => op_trigger_now(&input, store, stdout).await,
        "get_fire_history" => op_get_fire_history(&input, store).await,
        _ => Err(format!("Unknown operation: {operation}")),
    };

    match result {
        Ok(data) => ok_response(req.id, data),
        Err(msg) => err_response(req.id, -32000, msg),
    }
}

async fn op_trigger_now(
    input: &Value,
    store: Arc<Mutex<ScheduleStore>>,
    stdout: StdoutTx,
) -> Result<Value, String> {
    let schedule_id = input
        .get("schedule_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: schedule_id")?
        .to_string();

    let (event_type, event_data, schedule_name) = {
        let st = store.lock().await;
        let s = st
            .get(&schedule_id)
            .ok_or_else(|| format!("schedule not found: {schedule_id}"))?;
        (s.event_type.clone(), s.event_data.clone(), s.name.clone())
    };

    let fire_time = Utc::now().to_rfc3339();
    let event_id = uuid::Uuid::new_v4().to_string();
    let ipc_id = (uuid::Uuid::new_v4().as_u128() % 1_000_000) as u64 + 2000;

    let publish_req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "event.publish",
        "params": {
            "type": event_type,
            "data": {
                "schedule_id": schedule_id,
                "schedule_name": schedule_name,
                "fire_time": fire_time,
                "event_data": event_data,
            },
            "subject": schedule_id,
        },
        "id": ipc_id,
    });

    send_line(&stdout, serde_json::to_string(&publish_req).unwrap()).await;

    let record = FireRecord {
        fire_time: fire_time.clone(),
        event_id: event_id.clone(),
    };

    {
        let mut st = store.lock().await;
        if let Some(s) = st.get_mut(&schedule_id) {
            s.last_fired = Some(fire_time.clone());
            s.fire_count += 1;
        }
        if let Err(e) = st.save().await {
            eprintln!("scheduler: save after trigger_now: {e}");
        }
        if let Err(e) = st.append_history(&schedule_id, record).await {
            eprintln!("scheduler: history write: {e}");
        }
    }

    Ok(serde_json::json!({
        "fired": true,
        "event_id": event_id,
        "fire_time": fire_time,
    }))
}

async fn op_get_fire_history(
    input: &Value,
    store: Arc<Mutex<ScheduleStore>>,
) -> Result<Value, String> {
    let schedule_id = input
        .get("schedule_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: schedule_id")?;

    let st = store.lock().await;
    let fires = st.get_history(schedule_id).await?;

    Ok(serde_json::json!({ "fires": fires }))
}

// ---------------------------------------------------------------------------
// Resource CRUD handlers
// ---------------------------------------------------------------------------

async fn handle_resources_list(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
) -> JsonRpcResponse {
    let resource_type = req
        .params
        .get("resource_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if resource_type != "schedules" {
        return err_response(
            req.id,
            -32000,
            format!("unknown resource_type: {resource_type}"),
        );
    }

    let st = store.lock().await;
    let items: Vec<Value> = st
        .list()
        .iter()
        .map(|s| serde_json::to_value(s).unwrap())
        .collect();
    let total = items.len();

    ok_response(req.id, serde_json::json!({ "items": items, "total": total }))
}

async fn handle_resources_get(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
) -> JsonRpcResponse {
    let id = match req.params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return err_response(req.id, -32000, "missing required field: id".to_string()),
    };

    let st = store.lock().await;
    match st.get(id) {
        Some(s) => ok_response(req.id, serde_json::to_value(s).unwrap()),
        None => err_response(req.id, -32000, format!("schedule not found: {id}")),
    }
}

async fn handle_resources_create(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
) -> JsonRpcResponse {
    let data = match req.params.get("data") {
        Some(d) => d.clone(),
        None => return err_response(req.id, -32000, "missing required field: data".to_string()),
    };

    let name = match data.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err_response(req.id, -32000, "missing required field: name".to_string()),
    };
    let event_type = match data.get("event_type").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => {
            return err_response(
                req.id,
                -32000,
                "missing required field: event_type".to_string(),
            )
        }
    };
    let schedule_type = match data.get("schedule_type").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => {
            return err_response(
                req.id,
                -32000,
                "missing required field: schedule_type".to_string(),
            )
        }
    };

    if schedule_type == "interval" {
        let interval = data
            .get("interval_seconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if interval < 10 {
            return err_response(
                req.id,
                -32000,
                "interval_seconds must be >= 10".to_string(),
            );
        }
    }

    let now = Utc::now();
    let id = format!("sch_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let mut schedule = Schedule {
        id,
        name,
        event_type,
        event_data: data
            .get("event_data")
            .cloned()
            .unwrap_or(Value::Object(Default::default())),
        schedule_type,
        cron_expression: data
            .get("cron_expression")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        interval_seconds: data.get("interval_seconds").and_then(|v| v.as_u64()),
        run_at: data
            .get("run_at")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        timezone: data
            .get("timezone")
            .and_then(|v| v.as_str())
            .unwrap_or("UTC")
            .to_string(),
        status: "active".to_string(),
        last_fired: None,
        next_fire: None,
        fire_count: 0,
        created_at: now.to_rfc3339(),
    };

    schedule.next_fire = compute_next_fire(&schedule, now);

    let mut st = store.lock().await;
    match st.create(schedule) {
        Ok(created) => {
            let result = serde_json::to_value(created).unwrap();
            if let Err(e) = st.save().await {
                eprintln!("scheduler: save after create: {e}");
            }
            ok_response(req.id, result)
        }
        Err(e) => err_response(req.id, -32000, e),
    }
}

async fn handle_resources_update(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
) -> JsonRpcResponse {
    let id = match req.params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return err_response(req.id, -32000, "missing required field: id".to_string()),
    };
    let data = match req.params.get("data") {
        Some(d) => d.clone(),
        None => return err_response(req.id, -32000, "missing required field: data".to_string()),
    };

    if let Some(interval) = data.get("interval_seconds").and_then(|v| v.as_u64()) {
        if interval < 10 {
            return err_response(
                req.id,
                -32000,
                "interval_seconds must be >= 10".to_string(),
            );
        }
    }

    let mut st = store.lock().await;
    match st.update(&id, data) {
        Ok(mut updated) => {
            let now = Utc::now();
            updated.next_fire = compute_next_fire(&updated, now);
            if let Some(s) = st.get_mut(&id) {
                s.next_fire = updated.next_fire.clone();
            }
            let result = serde_json::to_value(&updated).unwrap();
            if let Err(e) = st.save().await {
                eprintln!("scheduler: save after update: {e}");
            }
            ok_response(req.id, result)
        }
        Err(e) => err_response(req.id, -32000, e),
    }
}

async fn handle_resources_delete(
    req: &JsonRpcRequest,
    store: Arc<Mutex<ScheduleStore>>,
) -> JsonRpcResponse {
    let id = match req.params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return err_response(req.id, -32000, "missing required field: id".to_string()),
    };

    let mut st = store.lock().await;
    match st.delete(&id) {
        Ok(()) => {
            if let Err(e) = st.save().await {
                eprintln!("scheduler: save after delete: {e}");
            }
            ok_response(req.id, serde_json::json!({ "deleted": true }))
        }
        Err(e) => err_response(req.id, -32000, e),
    }
}

// ---------------------------------------------------------------------------
// Timer loop
// ---------------------------------------------------------------------------

async fn timer_loop(
    store: Arc<Mutex<ScheduleStore>>,
    stdout: StdoutTx,
    ipc_id_counter: Arc<std::sync::atomic::AtomicU64>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        let now = Utc::now();

        let due_ids: Vec<String> = {
            let st = store.lock().await;
            st.list()
                .iter()
                .filter(|s| {
                    s.status == "active"
                        && s.next_fire
                            .as_ref()
                            .and_then(|nf| nf.parse::<chrono::DateTime<Utc>>().ok())
                            .map(|nf| nf <= now)
                            .unwrap_or(false)
                })
                .map(|s| s.id.clone())
                .collect()
        };

        for schedule_id in due_ids {
            fire_schedule(&schedule_id, &store, &stdout, &ipc_id_counter, now).await;
        }
    }
}

async fn fire_schedule(
    schedule_id: &str,
    store: &Arc<Mutex<ScheduleStore>>,
    stdout: &StdoutTx,
    ipc_id_counter: &Arc<std::sync::atomic::AtomicU64>,
    now: chrono::DateTime<Utc>,
) {
    let (event_type, event_data, schedule_name, schedule_type) = {
        let st = store.lock().await;
        match st.get(schedule_id) {
            Some(s) => (
                s.event_type.clone(),
                s.event_data.clone(),
                s.name.clone(),
                s.schedule_type.clone(),
            ),
            None => return,
        }
    };

    let fire_time = now.to_rfc3339();
    let event_id = uuid::Uuid::new_v4().to_string();
    let ipc_id = ipc_id_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let publish_req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "event.publish",
        "params": {
            "type": event_type,
            "data": {
                "schedule_id": schedule_id,
                "schedule_name": schedule_name,
                "fire_time": fire_time,
                "event_data": event_data,
            },
            "subject": schedule_id,
        },
        "id": ipc_id,
    });

    send_line(stdout, serde_json::to_string(&publish_req).unwrap()).await;

    eprintln!("scheduler: fired {schedule_id} ({schedule_name}) at {fire_time}");

    {
        let mut st = store.lock().await;
        if let Some(s) = st.get_mut(schedule_id) {
            s.last_fired = Some(fire_time.clone());
            s.fire_count += 1;

            if schedule_type == "once" {
                s.status = "completed".to_string();
                s.next_fire = None;
            } else {
                s.next_fire = compute_next_fire(s, now);
            }
        }

        let record = FireRecord {
            fire_time: fire_time.clone(),
            event_id,
        };

        if let Err(e) = st.save().await {
            eprintln!("scheduler: save after fire: {e}");
        }
        if let Err(e) = st.append_history(schedule_id, record).await {
            eprintln!("scheduler: history write: {e}");
        }
    }
}
