mod http_server;
mod store;
mod types;
mod verification;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use types::{Delivery, PendingEvent, Webhook};
use uuid::Uuid;

// ─── JSON-RPC wire types ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
    id: Value,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: Value,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

fn ok_response(id: Value, data: Value) -> JsonRpcResponse {
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

fn err_response(id: Value, code: i64, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        result: None,
        error: Some(JsonRpcError { code, message }),
        id,
    }
}

// ─── Shared app state ────────────────────────────────────────────────────────

struct AppState {
    store: store::WebhookStore,
    /// Mirror of webhooks for the HTTP server (refreshed on mutation).
    webhook_map: http_server::WebhookMap,
    port: Option<u16>,
    base_url: String,
    /// Events received from HTTP server, not yet published to host.
    pending_rx: Option<mpsc::UnboundedReceiver<(PendingEvent, Delivery)>>,
}

impl AppState {
    fn new(data_dir: PathBuf) -> Self {
        Self {
            store: store::WebhookStore::new(data_dir),
            webhook_map: Arc::new(RwLock::new(HashMap::new())),
            port: None,
            base_url: String::new(),
            pending_rx: None,
        }
    }

    fn refresh_webhook_map(&self) {
        if let Ok(mut map) = self.webhook_map.try_write() {
            map.clear();
            for wh in self.store.list() {
                map.insert(wh.id.clone(), wh.clone());
            }
        }
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let state: Arc<RwLock<AppState>> =
        Arc::new(RwLock::new(AppState::new(PathBuf::from("."))));

    let state_clone = state.clone();
    let handle = tokio::runtime::Handle::current();

    // Stdin loop on a blocking thread to avoid blocking the async runtime.
    tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut out = stdout.lock();
        let mut line = String::new();

        loop {
            line.clear();
            match stdin.lock().read_line(&mut line) {
                Ok(0) | Err(_) => break,
                _ => {}
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let request: JsonRpcRequest = match serde_json::from_str(trimmed) {
                Ok(r) => r,
                Err(e) => {
                    let resp =
                        err_response(Value::Number(0.into()), -32700, format!("Parse error: {e}"));
                    emit_line(&mut out, &resp);
                    continue;
                }
            };

            let is_shutdown = request.method == "shutdown";

            let response =
                handle.block_on(handle_request(&request, &state_clone, &mut out));
            emit_line(&mut out, &response);

            if is_shutdown {
                break;
            }
        }
    })
    .await
    .unwrap();
}

fn emit_line(out: &mut impl Write, resp: &JsonRpcResponse) {
    let line = serde_json::to_string(resp).expect("serialize response");
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

// ─── Request dispatch ─────────────────────────────────────────────────────────

async fn handle_request(
    req: &JsonRpcRequest,
    state: &Arc<RwLock<AppState>>,
    out: &mut dyn Write,
) -> JsonRpcResponse {
    match req.method.as_str() {
        "initialize" => handle_initialize(req, state).await,
        "shutdown" => handle_shutdown(req, state).await,
        "execute" => handle_execute(req, state, out).await,
        m if m.starts_with("resources.") => handle_resources(req, state, out).await,
        _ => err_response(req.id.clone(), -32601, format!("Unknown method: {}", req.method)),
    }
}

async fn handle_initialize(
    req: &JsonRpcRequest,
    state: &Arc<RwLock<AppState>>,
) -> JsonRpcResponse {
    let data_dir = req
        .params
        .get("data_dir")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let webhook_map = {
        let st = state.read().await;
        st.webhook_map.clone()
    };

    let (port, pending_rx) = match http_server::start(webhook_map).await {
        Ok(r) => r,
        Err(e) => {
            return err_response(req.id.clone(), -32603, format!("HTTP server failed: {e}"));
        }
    };

    let base_url = format!("http://127.0.0.1:{port}");

    let mut st = state.write().await;
    st.store = store::WebhookStore::new(data_dir);
    st.port = Some(port);
    st.base_url = base_url.clone();
    st.pending_rx = Some(pending_rx);

    if let Err(e) = st.store.load() {
        eprintln!("webhook-receiver: failed to load state: {e}");
    }

    // Update all stored webhook URLs to reflect the current port
    let ids: Vec<String> = st.store.webhooks.keys().cloned().collect();
    for id in &ids {
        if let Some(wh) = st.store.webhooks.get_mut(id) {
            wh.url = format!("{base_url}/hooks/{id}");
        }
    }
    if !ids.is_empty() {
        let _ = st.store.save_webhooks();
    }

    st.refresh_webhook_map();

    JsonRpcResponse {
        jsonrpc: "2.0",
        result: Some(serde_json::json!({ "ready": true })),
        error: None,
        id: req.id.clone(),
    }
}

async fn handle_shutdown(
    req: &JsonRpcRequest,
    state: &Arc<RwLock<AppState>>,
) -> JsonRpcResponse {
    let st = state.read().await;
    let _ = st.store.save_webhooks();
    JsonRpcResponse {
        jsonrpc: "2.0",
        result: Some(serde_json::json!({})),
        error: None,
        id: req.id.clone(),
    }
}

// ─── Execute handler ──────────────────────────────────────────────────────────

async fn handle_execute(
    req: &JsonRpcRequest,
    state: &Arc<RwLock<AppState>>,
    out: &mut dyn Write,
) -> JsonRpcResponse {
    drain_pending_events(state, out).await;

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
        "get_server_info" => op_get_server_info(state).await,
        "get_recent_deliveries" => op_get_recent_deliveries(&input, state).await,
        "list_webhooks" => op_list_webhooks(state).await,
        "create_webhook" => op_create_webhook(&input, state).await,
        "update_webhook" => op_update_webhook(&input, state).await,
        "delete_webhook" => op_delete_webhook(&input, state).await,
        _ => Err(format!("Unknown operation: {operation}")),
    };

    match result {
        Ok(data) => ok_response(req.id.clone(), data),
        Err(msg) => err_response(req.id.clone(), -32000, msg),
    }
}

// ─── Resources handler ────────────────────────────────────────────────────────

async fn handle_resources(
    req: &JsonRpcRequest,
    state: &Arc<RwLock<AppState>>,
    out: &mut dyn Write,
) -> JsonRpcResponse {
    drain_pending_events(state, out).await;

    let resource_method = req.method.strip_prefix("resources.").unwrap_or("");
    let resource_type = req
        .params
        .get("resource_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if resource_type != "webhooks" {
        return err_response(
            req.id.clone(),
            -32602,
            format!("Unknown resource type: {resource_type}"),
        );
    }

    let result = match resource_method {
        "list" => resource_list(state).await,
        "get" => {
            let id = req.params.get("id").and_then(|v| v.as_str()).unwrap_or("");
            resource_get(id, state).await
        }
        "create" => {
            let data = req
                .params
                .get("data")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            resource_create(&data, state).await
        }
        "update" => {
            let id = req.params.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let data = req
                .params
                .get("data")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            resource_update(id, data, state).await
        }
        "delete" => {
            let id = req.params.get("id").and_then(|v| v.as_str()).unwrap_or("");
            resource_delete(id, state).await
        }
        _ => Err(format!("Unknown resource method: {resource_method}")),
    };

    match result {
        Ok(data) => JsonRpcResponse {
            jsonrpc: "2.0",
            result: Some(data),
            error: None,
            id: req.id.clone(),
        },
        Err(msg) => err_response(req.id.clone(), -32000, msg),
    }
}

// ─── Pending event draining ───────────────────────────────────────────────────

/// Drain any deliveries queued by the HTTP server and publish them as IPC events.
/// Must be called while the host is in its read loop (i.e., during any execute/resources call).
async fn drain_pending_events(
    state: &Arc<RwLock<AppState>>,
    out: &mut dyn Write,
) {
    let mut pending: Vec<(PendingEvent, Delivery)> = Vec::new();
    {
        let mut st = state.write().await;
        if let Some(rx) = &mut st.pending_rx {
            while let Ok(item) = rx.try_recv() {
                pending.push(item);
            }
        }
    }

    if pending.is_empty() {
        return;
    }

    static NEXT_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(20000);
    let stdin = io::stdin();

    for (event, _delivery) in &pending {
        let call_id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "event.publish",
            "params": {
                "type": event.event_type,
                "subject": event.subject,
                "data": event.data,
            },
            "id": call_id,
        });
        let _ = writeln!(out, "{}", serde_json::to_string(&req).unwrap());
        let _ = out.flush();

        // Read back the host's response to keep the protocol in sync
        let mut line = String::new();
        let _ = stdin.lock().read_line(&mut line);
    }

    // Persist deliveries after publishing
    {
        let mut st = state.write().await;
        for (_, delivery) in pending {
            let wh_id = delivery.webhook_id.clone();
            st.store.record_delivery(delivery);
            let _ = st.store.save_deliveries(&wh_id);
        }
        let _ = st.store.save_webhooks();
        st.refresh_webhook_map();
    }
}

// ─── Operations ───────────────────────────────────────────────────────────────

async fn op_get_server_info(state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let st = state.read().await;
    let port = st.port.ok_or("server not initialized")?;
    Ok(serde_json::json!({
        "port": port,
        "base_url": st.base_url,
        "webhook_count": st.store.webhooks.len(),
    }))
}

async fn op_get_recent_deliveries(
    input: &Value,
    state: &Arc<RwLock<AppState>>,
) -> Result<Value, String> {
    let webhook_id = input
        .get("webhook_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: webhook_id")?;

    let st = state.read().await;
    if !st.store.webhooks.contains_key(webhook_id) {
        return Err(format!("webhook not found: {webhook_id}"));
    }

    let deliveries: Vec<Value> = st
        .store
        .get_deliveries(webhook_id)
        .into_iter()
        .map(|d| serde_json::to_value(d).unwrap_or(Value::Null))
        .collect();

    Ok(serde_json::json!({ "deliveries": deliveries }))
}

// ─── MCP-exposed operations (delegate to resource CRUD) ──────────────────

async fn op_list_webhooks(state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let st = state.read().await;
    let items: Vec<Value> = st
        .store
        .list()
        .into_iter()
        .map(|w| serde_json::to_value(w).unwrap_or(Value::Null))
        .collect();
    Ok(serde_json::json!({ "webhooks": items, "count": items.len() }))
}

async fn op_create_webhook(input: &Value, state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    resource_create(input, state).await
}

async fn op_update_webhook(input: &Value, state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let id = input
        .get("webhook_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: webhook_id")?;
    resource_update(id, input.clone(), state).await
}

async fn op_delete_webhook(input: &Value, state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let id = input
        .get("webhook_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: webhook_id")?;
    resource_delete(id, state).await
}

// ─── Resource CRUD ────────────────────────────────────────────────────────────

async fn resource_list(state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let st = state.read().await;
    let items: Vec<Value> = st
        .store
        .list()
        .into_iter()
        .map(|w| serde_json::to_value(w).unwrap_or(Value::Null))
        .collect();
    let total = items.len();
    Ok(serde_json::json!({ "items": items, "total": total }))
}

async fn resource_get(id: &str, state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let st = state.read().await;
    match st.store.get(id) {
        Some(wh) => Ok(serde_json::to_value(wh).unwrap()),
        None => Err(format!("webhook not found: {id}")),
    }
}

async fn resource_create(data: &Value, state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let name = data
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: name")?
        .to_string();
    let event_type = data
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: event_type")?
        .to_string();
    let verification_method = data
        .get("verification_method")
        .and_then(|v| v.as_str())
        .unwrap_or("none")
        .to_string();
    let verification_secret = data
        .get("verification_secret")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let id = format!("wh_{}", &Uuid::new_v4().to_string()[..8]);
    let base_url = {
        let st = state.read().await;
        st.base_url.clone()
    };
    let url = format!("{base_url}/hooks/{id}");

    let wh = Webhook {
        id: id.clone(),
        name,
        event_type,
        verification_method,
        verification_secret,
        status: "active".to_string(),
        url,
        trigger_count: 0,
        last_triggered: None,
        created_at: Utc::now().to_rfc3339(),
    };

    let mut st = state.write().await;
    let created = st.store.insert(wh);
    st.store.save_webhooks().map_err(|e| e.to_string())?;
    st.refresh_webhook_map();

    Ok(serde_json::to_value(&created).unwrap())
}

async fn resource_update(
    id: &str,
    data: Value,
    state: &Arc<RwLock<AppState>>,
) -> Result<Value, String> {
    let mut st = state.write().await;
    match st.store.update(id, data) {
        Some(updated) => {
            st.store.save_webhooks().map_err(|e| e.to_string())?;
            st.refresh_webhook_map();
            Ok(serde_json::to_value(&updated).unwrap())
        }
        None => Err(format!("webhook not found: {id}")),
    }
}

async fn resource_delete(id: &str, state: &Arc<RwLock<AppState>>) -> Result<Value, String> {
    let mut st = state.write().await;
    if st.store.delete(id) {
        st.store.save_webhooks().map_err(|e| e.to_string())?;
        st.refresh_webhook_map();
        Ok(serde_json::json!({ "deleted": true }))
    } else {
        Err(format!("webhook not found: {id}"))
    }
}
