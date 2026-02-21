mod discovery;
mod ops;
mod state;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use state::AppState;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::RwLock;

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

#[tokio::main]
async fn main() {
    // Open zenoh session
    let config = match std::env::var("ZENOH_CONFIG") {
        Ok(path) => zenoh::Config::from_file(&path).unwrap_or_else(|e| {
            eprintln!("zenoh: failed to load config from {path}: {e}, using default");
            zenoh::Config::default()
        }),
        Err(_) => zenoh::Config::default(),
    };

    let session = Arc::new(zenoh::open(config).await.unwrap_or_else(|e| {
        eprintln!("zenoh: failed to open session: {e}");
        std::process::exit(1);
    }));

    let state = Arc::new(RwLock::new(AppState::new()));

    // Read stdin in a blocking thread, dispatch to async handlers
    let session_clone = session.clone();
    let state_clone = state.clone();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        let mut line = String::new();

        loop {
            line.clear();
            match stdin.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                _ => {}
            }

            if line.trim().is_empty() {
                continue;
            }

            let request: JsonRpcRequest = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(e) => {
                    let resp = err_response(0, -32700, format!("Parse error: {e}"));
                    let _ = writeln!(stdout, "{}", serde_json::to_string(&resp).unwrap());
                    let _ = stdout.flush();
                    continue;
                }
            };

            let is_shutdown = request.method == "shutdown";

            let response = handle.block_on(handle_request(
                &request,
                &session_clone,
                &state_clone,
            ));

            let _ = writeln!(stdout, "{}", serde_json::to_string(&response).unwrap());
            let _ = stdout.flush();

            if is_shutdown {
                break;
            }
        }
    })
    .await
    .unwrap();
}

async fn handle_request(
    req: &JsonRpcRequest,
    session: &Arc<zenoh::Session>,
    state: &Arc<RwLock<AppState>>,
) -> JsonRpcResponse {
    match req.method.as_str() {
        "initialize" => JsonRpcResponse {
            jsonrpc: "2.0",
            result: Some(serde_json::json!({ "ready": true })),
            error: None,
            id: req.id,
        },

        "shutdown" => {
            // Clean up: stop discovery and all subscriptions
            {
                let mut st = state.write().await;
                if let Some(cancel) = st.discovery_cancel.take() {
                    let _ = cancel.send(true);
                }
                for (_, sub) in st.subscriptions.drain() {
                    let _ = sub.cancel.send(true);
                }
            }
            JsonRpcResponse {
                jsonrpc: "2.0",
                result: Some(serde_json::json!({})),
                error: None,
                id: req.id,
            }
        }

        "execute" => handle_execute(req, session, state).await,

        _ => err_response(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

async fn handle_execute(
    req: &JsonRpcRequest,
    session: &Arc<zenoh::Session>,
    state: &Arc<RwLock<AppState>>,
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
        "session_info" => ops::op_session_info(session).await,
        "start_discovery" => ops::op_start_discovery(&input, session.clone(), state.clone()).await,
        "stop_discovery" => ops::op_stop_discovery(state.clone()).await,
        "get_topics" => ops::op_get_topics(&input, state.clone()).await,
        "subscribe" => ops::op_subscribe(&input, session.clone(), state.clone()).await,
        "unsubscribe" => ops::op_unsubscribe(&input, state.clone()).await,
        "poll" => ops::op_poll(&input, state.clone()).await,
        "list_subscriptions" => ops::op_list_subscriptions(state.clone()).await,
        _ => Err(format!("Unknown operation: {operation}")),
    };

    match result {
        Ok(data) => ok_response(req.id, data),
        Err(msg) => err_response(req.id, -32000, msg),
    }
}
