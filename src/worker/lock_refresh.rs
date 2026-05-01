use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use dashmap::DashMap;
use deadpool_redis::Pool;
use tracing::{debug, warn};

use crate::{
    QueueName,
    luacommands::{ExtendLocks, InvokeLuaScript},
    worker::shutdown_switch::ShutdownSwitch,
};

pub async fn lock_refresh(
    pool: Pool,
    queue_name: QueueName,
    refresh_map: Arc<DashMap<String, Weak<str>>>,
    shutdown_switch: ShutdownSwitch,
    lock_duration: Duration,
) {
    let refresh_interval = lock_duration / 2;

    debug!("start");
    while shutdown_switch.running() {
        // Collect live entries (where the Arc<str> token is still alive)
        let mut job_ids = Vec::new();
        let mut tokens = Vec::new();

        refresh_map.retain(|job_id, weak_token| {
            if let Some(token) = weak_token.upgrade() {
                job_ids.push(job_id.clone());
                tokens.push(token.to_string());
                true
            } else {
                false
            }
        });

        if job_ids.is_empty() {
            shutdown_switch.sleep(refresh_interval).await;
            continue;
        }

        debug!("Refreshing locks for {} jobs", job_ids.len());

        let mut con = match pool.get().await {
            Ok(con) => con,
            Err(e) => {
                warn!("Failed to get Redis connection for lock refresh: {e}");
                continue;
            }
        };

        let extend = ExtendLocks {
            queue: &queue_name,
            job_ids: &job_ids,
            tokens: &tokens,
            lock_duration,
        };

        match extend.call(&mut *con).await {
            Ok(failed_ids) => {
                for id in &failed_ids {
                    warn!(job_id = %id, "Failed to extend lock, removing from refresh map");
                    refresh_map.remove(id);
                }
            }
            Err(e) => {
                warn!("extendLocks script error: {e}");
            }
        }
        shutdown_switch.sleep(refresh_interval).await;
    }
}
