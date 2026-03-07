use std::{sync::Arc, time::Duration};

use chrono::Utc;
use deadpool_redis::Pool;
use tokio::{sync::RwLock, time::sleep};
use tracing::{info, warn};

use crate::{
    luacommands::{InvokeLuaScript, MoveStalledJobsToWait},
    queue::QueueName, worker::shutdown_switch::ShutdownSwitch,
};

/// Move stalled jobs (worker doesn't refresh job lock) to
/// waiting or failed. To be canceled once the worker is dropped.
/// It never panics and will recover if redis is temporarily unavailable.
pub async fn stalled_to_wait(
    pool: Pool,
    queue_name: QueueName,
    _shutdown_switch: ShutdownSwitch,
    stalled_after: Arc<RwLock<Duration>>,
    max_stalled_before_failed: Arc<RwLock<usize>>,
) {
    loop {
        let stalled_after: Duration = *stalled_after.read().await;
        let max_stalled_before_failed: usize = *max_stalled_before_failed.read().await;
        let con = pool.get().await;
        if let Err(e) = con {
            warn!("Failed to get redis connection from pool for stalled-to-wait: {e:?}");
            sleep(stalled_after / 2).await;
            continue;
        };
        let mut con = con.unwrap();
        let stw = MoveStalledJobsToWait {
            queue: &queue_name,
            timestamp: Utc::now(),
            max_duration: stalled_after,
            max_stalled_before_failed,
        };
        let res = stw.call(&mut con).await;
        if let Err(e) = res {
            warn!("Failed to moved stalled jobs to wait: {e:?}");
            sleep(stalled_after / 2).await;
            continue;
        }
        let res = res.unwrap();
        if !res.is_empty() {
            info!(
                "The following jobs of queue {} stalled: {:?}",
                queue_name.as_str(),
                res
            );
        }
        sleep(stalled_after / 2).await;
    }
}
