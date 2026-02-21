mod embeddings;
mod graph;
mod indexer;
mod languages;
mod ops;
mod search;
mod state;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use state::AppState;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

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

/// Shared handle to the initialized state. Set once during `initialize`.
static STATE: OnceCell<Arc<RwLock<AppState>>> = OnceCell::const_new();

#[tokio::main]
async fn main() {
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
            let response = handle.block_on(handle_request(&request));

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

async fn handle_request(req: &JsonRpcRequest) -> JsonRpcResponse {
    match req.method.as_str() {
        "initialize" => {
            let data_dir = req
                .params
                .get("data_dir")
                .and_then(|v| v.as_str())
                .unwrap_or("/tmp/codebase-indexer");

            match AppState::initialize(PathBuf::from(data_dir)).await {
                Ok(app_state) => {
                    let state = Arc::new(RwLock::new(app_state));
                    // If already initialized, this is a no-op (OnceCell).
                    let _ = STATE.set(state);
                    JsonRpcResponse {
                        jsonrpc: "2.0",
                        result: Some(serde_json::json!({ "ready": true })),
                        error: None,
                        id: req.id,
                    }
                }
                Err(e) => err_response(req.id, -32000, format!("Initialize failed: {e}")),
            }
        }

        "shutdown" => {
            if let Some(state) = STATE.get() {
                let mut st = state.write().await;
                for (_, cancel) in st.indexing_cancels.drain() {
                    let _ = cancel.send(true);
                }
                for (_, cancel) in st.graph_cancels.drain() {
                    let _ = cancel.send(true);
                }
                st.save_metadata().await.ok();
            }
            JsonRpcResponse {
                jsonrpc: "2.0",
                result: Some(serde_json::json!({})),
                error: None,
                id: req.id,
            }
        }

        "execute" => handle_execute(req).await,

        _ => err_response(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

async fn handle_execute(req: &JsonRpcRequest) -> JsonRpcResponse {
    let state = match STATE.get() {
        Some(s) => s.clone(),
        None => {
            return err_response(
                req.id,
                -32000,
                "Extension not initialized. Send 'initialize' first.".into(),
            )
        }
    };

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
        "add_repository" => ops::op_add_repository(&input, state).await,
        "remove_repository" => ops::op_remove_repository(&input, state).await,
        "list_repositories" => ops::op_list_repositories(state).await,
        "sync" => ops::op_sync(&input, state).await,
        "search" => ops::op_search(&input, state).await,
        "map" => ops::op_map(&input, state).await,
        "status" => ops::op_status(state).await,
        "create_workspace" => ops::op_create_workspace(&input, state).await,
        "list_workspaces" => ops::op_list_workspaces(state).await,
        "build_graph" => ops::op_build_graph(&input, state).await,
        "find_references" => ops::op_find_references(&input, state).await,
        "call_graph" => ops::op_call_graph(&input, state).await,
        "dependency_graph" => ops::op_dependency_graph(&input, state).await,
        "type_hierarchy" => ops::op_type_hierarchy(&input, state).await,
        _ => Err(format!("Unknown operation: {operation}")),
    };

    match result {
        Ok(data) => ok_response(req.id, data),
        Err(msg) => err_response(req.id, -32000, msg),
    }
}
