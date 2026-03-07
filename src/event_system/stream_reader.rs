use std::{fmt::Debug, time::Duration};

use deadpool_redis::Pool;
use redis::{AsyncCommands, Value, streams::StreamReadOptions};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::{QueueName, worker::shutdown_switch::ShutdownSwitch};

const XREAD_BLOCK: Duration = Duration::from_secs(5);
const XREAD_COUNT: usize = 100;

pub async fn listen_to_events<R>(
    pool: Pool,
    queue_name: QueueName,
    shutdown_switch: ShutdownSwitch,
    event_tx: broadcast::Sender<super::QueueEvent<R>>,
) where
    R: Debug + Clone + Send + 'static + DeserializeOwned,
{
    let events_key = queue_name.events();
    let mut last_id = "$".to_string();
    let opts = StreamReadOptions::default()
        .block(XREAD_BLOCK.as_millis() as usize)
        .count(XREAD_COUNT);

    while shutdown_switch.running() {
        let mut con = match pool.get().await {
            Ok(con) => con,
            Err(e) => {
                warn!("Failed to get Redis connection for events: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let reply: redis::streams::StreamReadReply =
            match con.xread_options(&[&events_key], &[&last_id], &opts).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("XREAD error on {events_key}: {e}");
                    continue;
                }
            };

        for key in &reply.keys {
            for entry in &key.ids {
                last_id.clone_from(&entry.id);

                let Some(event) = super::QueueEvent::parse(&entry.map) else {
                    continue;
                };

                debug!(
                    ?event,
                    stream_id = %entry.id,
                    "Stream event on {}",
                    queue_name.as_str()
                );

                let _ = event_tx.send(event);
            }
        }
    }
}

pub(super) fn extract_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}
