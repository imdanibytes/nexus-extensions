use crate::discovery::spawn_discovery;
use crate::state::{AppState, BufferedSample};
use base64::Engine as _;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

type Result = std::result::Result<Value, String>;

pub async fn op_session_info(session: &zenoh::Session) -> Result {
    let zid = session.zid().to_string();
    let peers: Vec<String> = session
        .info()
        .peers_zid()
        .await
        .map(|z| z.to_string())
        .collect();
    let routers: Vec<String> = session
        .info()
        .routers_zid()
        .await
        .map(|z| z.to_string())
        .collect();
    let config_source = std::env::var("ZENOH_CONFIG").unwrap_or_else(|_| "default".into());

    Ok(serde_json::json!({
        "zid": zid,
        "peers": peers,
        "routers": routers,
        "config_source": config_source,
        "connected": true,
    }))
}

pub async fn op_start_discovery(
    input: &Value,
    session: Arc<zenoh::Session>,
    state: Arc<RwLock<AppState>>,
) -> Result {
    let key_expr = input
        .get("key_expr")
        .and_then(|v| v.as_str())
        .unwrap_or("**")
        .to_string();

    let mut st = state.write().await;

    // Stop existing discovery if running
    if let Some(cancel) = st.discovery_cancel.take() {
        let _ = cancel.send(true);
    }
    st.topics.clear();
    st.discovery_active = true;
    st.discovery_key_expr = key_expr.clone();
    drop(st);

    let cancel = spawn_discovery(session, state.clone(), key_expr.clone());

    let mut st = state.write().await;
    st.discovery_cancel = Some(cancel);

    Ok(serde_json::json!({
        "started": true,
        "key_expr": key_expr,
    }))
}

pub async fn op_stop_discovery(state: Arc<RwLock<AppState>>) -> Result {
    let mut st = state.write().await;
    if let Some(cancel) = st.discovery_cancel.take() {
        let _ = cancel.send(true);
    }
    st.discovery_active = false;
    st.topics.clear();
    st.discovery_key_expr.clear();

    Ok(serde_json::json!({ "stopped": true }))
}

pub async fn op_get_topics(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let prefix = input
        .get("prefix")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let now = chrono::Utc::now();
    let st = state.read().await;
    let topics: Vec<Value> = st
        .topics
        .values()
        .filter(|t| prefix.is_empty() || t.key_expr.starts_with(prefix))
        .map(|t| {
            let silent_secs = (now - t.last_seen).num_seconds();
            serde_json::json!({
                "key_expr": t.key_expr,
                "first_seen": t.first_seen.to_rfc3339(),
                "last_seen": t.last_seen.to_rfc3339(),
                "sample_count": t.sample_count,
                "rate_hz": (t.rate_hz() * 100.0).round() / 100.0,
                "avg_payload_size": t.avg_payload_size(),
                "last_encoding": t.last_encoding,
                "stale": silent_secs >= 5,
                "silent_secs": silent_secs,
            })
        })
        .collect();

    Ok(serde_json::json!({
        "discovery_active": st.discovery_active,
        "topic_count": topics.len(),
        "topics": topics,
    }))
}

pub async fn op_subscribe(
    input: &Value,
    session: Arc<zenoh::Session>,
    state: Arc<RwLock<AppState>>,
) -> Result {
    let key_expr = input
        .get("key_expr")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: key_expr")?
        .to_string();

    let buffer_size = input
        .get("buffer_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;

    let sub_id = uuid::Uuid::new_v4().to_string();

    let (cancel_tx, mut cancel_rx) = watch::channel(false);

    let sub = crate::state::Subscription::new(key_expr.clone(), buffer_size, cancel_tx);

    {
        let mut st = state.write().await;
        st.subscriptions.insert(sub_id.clone(), sub);
    }

    // Spawn background task to receive samples
    let state_clone = state.clone();
    let sub_id_clone = sub_id.clone();
    let key_expr_clone = key_expr.clone();
    tokio::spawn(async move {
        let subscriber = match session.declare_subscriber(&key_expr_clone).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("subscribe: failed for {key_expr_clone}: {e}");
                return;
            }
        };

        loop {
            tokio::select! {
                sample = subscriber.recv_async() => {
                    let sample = match sample {
                        Ok(s) => s,
                        Err(_) => break,
                    };

                    let ke = sample.key_expr().as_str().to_string();
                    let payload_bytes: Vec<u8> = sample.payload().to_bytes().to_vec();
                    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&payload_bytes);
                    let payload_str = String::from_utf8(payload_bytes).ok();
                    let encoding = sample.encoding().to_string();

                    let buffered = BufferedSample {
                        key_expr: ke,
                        payload_b64,
                        payload_str,
                        encoding,
                        timestamp: chrono::Utc::now(),
                    };

                    let mut st = state_clone.write().await;
                    if let Some(sub) = st.subscriptions.get_mut(&sub_id_clone) {
                        sub.push(buffered);
                    } else {
                        // Subscription was removed, stop the task
                        break;
                    }
                }
                _ = cancel_rx.changed() => {
                    if *cancel_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    Ok(serde_json::json!({
        "sub_id": sub_id,
        "key_expr": key_expr,
        "buffer_size": buffer_size,
    }))
}

pub async fn op_unsubscribe(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let sub_id = input
        .get("sub_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: sub_id")?;

    let mut st = state.write().await;
    match st.subscriptions.remove(sub_id) {
        Some(sub) => {
            let _ = sub.cancel.send(true);
            Ok(serde_json::json!({
                "removed": true,
                "sub_id": sub_id,
            }))
        }
        None => Err(format!("subscription not found: {sub_id}")),
    }
}

pub async fn op_poll(input: &Value, state: Arc<RwLock<AppState>>) -> Result {
    let sub_id = input
        .get("sub_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: sub_id")?;

    let limit = input
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(10) as usize;

    let mut st = state.write().await;
    match st.subscriptions.get_mut(sub_id) {
        Some(sub) => {
            let samples = sub.drain(limit);
            let overflow = sub.overflow_count;
            let buffered = sub.buffer.len();
            Ok(serde_json::json!({
                "sub_id": sub_id,
                "samples": samples,
                "sample_count": samples.len(),
                "overflow_count": overflow,
                "buffered_remaining": buffered,
            }))
        }
        None => Err(format!("subscription not found: {sub_id}")),
    }
}

pub async fn op_list_subscriptions(state: Arc<RwLock<AppState>>) -> Result {
    let st = state.read().await;
    let subs: Vec<Value> = st
        .subscriptions
        .iter()
        .map(|(id, sub)| {
            serde_json::json!({
                "sub_id": id,
                "key_expr": sub.key_expr,
                "buffered": sub.buffer.len(),
                "buffer_capacity": sub.buffer_capacity,
                "overflow_count": sub.overflow_count,
                "total_received": sub.total_received,
                "created_at": sub.created_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(serde_json::json!({
        "count": subs.len(),
        "subscriptions": subs,
    }))
}
