use crate::state::{AppState, TopicMeta};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

const TOPIC_EXPIRY_SECS: i64 = 30;

/// Spawn a background task that subscribes to `key_expr` and updates topic metadata.
/// Also spawns a cleanup task that removes topics silent for 30+ seconds.
/// Returns the cancel sender — send `true` to stop both tasks.
pub fn spawn_discovery(
    session: Arc<zenoh::Session>,
    state: Arc<RwLock<AppState>>,
    key_expr: String,
) -> watch::Sender<bool> {
    let (cancel_tx, mut cancel_rx) = watch::channel(false);

    // Subscriber task
    let state_sub = state.clone();
    tokio::spawn(async move {
        let subscriber = match session.declare_subscriber(&key_expr).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("discovery: failed to subscribe to {key_expr}: {e}");
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
                    let encoding = sample.encoding().to_string();
                    let payload_len = sample.payload().len() as u64;

                    let mut st = state_sub.write().await;
                    match st.topics.get_mut(&ke) {
                        Some(meta) => meta.update(encoding, payload_len),
                        None => {
                            st.topics.insert(
                                ke.clone(),
                                TopicMeta::new(ke, encoding, payload_len),
                            );
                        }
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

    // Cleanup task — prune expired topics every 5 seconds
    let mut cancel_rx2 = cancel_tx.subscribe();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Utc::now();
                    let mut st = state.write().await;
                    st.topics.retain(|_, meta| {
                        (now - meta.last_seen).num_seconds() < TOPIC_EXPIRY_SECS
                    });
                }
                _ = cancel_rx2.changed() => {
                    if *cancel_rx2.borrow() {
                        break;
                    }
                }
            }
        }
    });

    cancel_tx
}
