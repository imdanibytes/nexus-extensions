use crate::types::{Delivery, PendingEvent, Webhook};
use crate::verification;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
    Router,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};

/// Shared snapshot of webhooks for the HTTP server.
/// Refreshed whenever webhooks are mutated.
pub type WebhookMap = Arc<RwLock<HashMap<String, Webhook>>>;

#[derive(Clone)]
struct ServerState {
    webhooks: WebhookMap,
    event_tx: mpsc::UnboundedSender<(PendingEvent, Delivery)>,
}

/// Spawn the Axum server on 127.0.0.1:0 and return the bound port and
/// a channel receiver for incoming webhook deliveries.
pub async fn start(
    webhooks: WebhookMap,
) -> Result<(u16, mpsc::UnboundedReceiver<(PendingEvent, Delivery)>), String> {
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let state = ServerState {
        webhooks,
        event_tx,
    };

    let app = Router::new()
        .route("/hooks/{webhook_id}", post(handle_webhook))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("failed to bind HTTP server: {e}"))?;

    let port = listener.local_addr().unwrap().port();
    eprintln!("webhook-receiver: HTTP server bound on port {port}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap_or_else(|e| {
            eprintln!("webhook-receiver: HTTP server error: {e}");
        });
    });

    Ok((port, event_rx))
}

async fn handle_webhook(
    Path(webhook_id): Path<String>,
    State(state): State<ServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let webhooks = state.webhooks.read().await;
    let wh = match webhooks.get(&webhook_id) {
        Some(w) => w.clone(),
        None => return StatusCode::NOT_FOUND,
    };
    drop(webhooks);

    if wh.status == "paused" {
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    // Verify signature
    let verified = match wh.verification_method.as_str() {
        "none" => true,
        "github-hmac" => {
            let secret = wh.verification_secret.as_deref().unwrap_or("");
            let sig = headers
                .get("x-hub-signature-256")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            verification::verify_github_hmac(secret, &body, sig)
        }
        "standard-webhooks" => {
            let secret = wh.verification_secret.as_deref().unwrap_or("");
            let msg_id = headers
                .get("webhook-id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let timestamp = headers
                .get("webhook-timestamp")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let sig = headers
                .get("webhook-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            verification::verify_standard_webhooks(secret, &body, msg_id, timestamp, sig)
        }
        "custom-header" => {
            // Secret format: "Header-Name:expected-value"
            let secret = wh.verification_secret.as_deref().unwrap_or("");
            if let Some((header_name, expected)) = secret.split_once(':') {
                let actual = headers
                    .get(header_name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                verification::verify_custom_header(expected, actual)
            } else {
                false
            }
        }
        _ => false,
    };

    if !verified {
        eprintln!("webhook-receiver: verification failed for webhook {webhook_id}");
        return StatusCode::UNAUTHORIZED;
    }

    // Parse body as JSON; fall back to raw string
    let body_value: serde_json::Value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&body).into()));

    // Capture request headers as a JSON object
    let header_map: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str().ok().map(|s| (k.to_string(), serde_json::Value::String(s.to_string())))
        })
        .collect();

    let delivery_id = uuid::Uuid::new_v4().to_string();
    let delivery = Delivery {
        id: delivery_id.clone(),
        webhook_id: webhook_id.clone(),
        received_at: Utc::now(),
        status: "success".to_string(),
        headers: serde_json::Value::Object(header_map),
        body: body_value.clone(),
    };

    let event = PendingEvent {
        event_type: wh.event_type.clone(),
        subject: webhook_id.clone(),
        data: body_value,
    };

    let _ = state.event_tx.send((event, delivery));

    StatusCode::OK
}
