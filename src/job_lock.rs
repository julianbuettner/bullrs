use std::time::Duration;

use deadpool_redis::Pool;
use log::debug;
use tokio::{
    task::{self, JoinHandle},
    time::sleep,
};

use crate::queue::QueueName;

const LOCK_REFRESH_COOLDOWN: Duration = Duration::from_secs(10);

struct LockRefreshHandle(JoinHandle<()>);

impl Drop for LockRefreshHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn keep_job_lock_inner(queue_name: QueueName, pool: Pool, id: String) {
    loop {
        {
            let con = pool.get().await;
            if con.is_err() {
                debug!("Could not get Redis connection from pool, will not try to extend lock.");
                return;
            }
            // lua scripts
        }
        sleep(LOCK_REFRESH_COOLDOWN);
    }
}

fn keep_job_lock(queue_name: QueueName, pool: Pool, id: String) -> LockRefreshHandle {
    let h = task::spawn(keep_job_lock_inner(queue_name, pool, id));
    LockRefreshHandle(h)
}
