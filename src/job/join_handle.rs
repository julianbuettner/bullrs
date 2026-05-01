use std::{fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};

use deadpool_redis::Pool;
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};

use crate::{
    error::JobAwaitError,
    event_system::{EventSystem, QueueEvent},
    luacommands::{InvokeLuaScript, IsFinished, IsFinishedOk},
    queue::QueueName,
};

// This is more of a fallback, so it can be fairly high to keep load low
const POLL_INTERVAL: Duration = Duration::from_secs(4);

/// Returned when enqueuing a job.
/// Can be used for awaiting the job's return value, changing its parameters
/// (at least before being picked up by a worker) or even to cancel the job.
pub struct JobJoinHandle<D, R: Debug + Clone> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    event_system: Arc<EventSystem<R>>,
    phantom: PhantomData<D>,
}

impl<D, R> JobJoinHandle<D, R>
where
    R: Debug + Clone + Send + 'static + DeserializeOwned,
{
    pub(crate) fn new(
        queue_name: QueueName,
        pool: Pool,
        id: String,
        event_system: Arc<EventSystem<R>>,
    ) -> Self {
        Self {
            queue_name,
            pool,
            id,
            event_system,
            phantom: PhantomData,
        }
    }

    /// Access the job ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Wait for the job to complete and return its result.
    ///
    /// Uses two concurrent strategies:
    /// - Listens for completion/failure events via the Redis stream subscription
    /// - Polls Redis directly every 2 seconds as a fallback for missed events
    ///
    /// Returns `Ok(R)` on success or `Err(JobAwaitError)` on failure.
    pub async fn result(self) -> Result<R, JobAwaitError> {
        let mut event_rx = self.event_system.subscribe();
        let pool = self.pool;
        let queue_name = self.queue_name;
        let id = self.id;

        tokio::select! {
            result = wait_for_event(&mut event_rx, &id) => result,
            result = poll_until_finished(&pool, &queue_name, &id) => result,
        }
    }
}

async fn wait_for_event<R>(
    event_rx: &mut broadcast::Receiver<QueueEvent<R>>,
    job_id: &str,
) -> Result<R, JobAwaitError>
where
    R: Debug + Clone + Send + 'static,
{
    loop {
        let event = match event_rx.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("Job {job_id} event receiver lagged, missed {n} events");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!("Event broadcast closed while waiting for job {job_id}");
                // Let the poll branch handle it
                std::future::pending::<()>().await;
                unreachable!()
            }
        };

        match event {
            QueueEvent::Completed {
                job_id: ref eid,
                return_value,
                ..
            } if eid == job_id => {
                return Ok(return_value);
            }
            QueueEvent::Failed {
                job_id: ref eid,
                ref failed_reason,
                ..
            } if eid == job_id => {
                return Err(JobAwaitError::JobFailed {
                    reason: failed_reason.clone().unwrap_or_default(),
                });
            }
            _ => {}
        }
    }
}

async fn poll_until_finished<R>(
    pool: &Pool,
    queue_name: &QueueName,
    job_id: &str,
) -> Result<R, JobAwaitError>
where
    R: DeserializeOwned,
{
    loop {
        let mut con = match pool.get().await {
            Ok(con) => con,
            Err(deadpool_redis::PoolError::Closed) => {
                return Err(JobAwaitError::PoolError(deadpool_redis::PoolError::Closed));
            }
            Err(e) => {
                warn!("Poll for job {job_id}: pool error, retrying: {e}");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };

        let is_finished = IsFinished {
            queue: queue_name,
            job_id,
        };

        match is_finished.call(&mut *con).await {
            Ok(IsFinishedOk::Completed(json)) => {
                let value: R = serde_json::from_str(&json).map_err(JobAwaitError::Deserialize)?;
                return Ok(value);
            }
            Ok(IsFinishedOk::Failed(reason)) => {
                return Err(JobAwaitError::JobFailed { reason });
            }
            Ok(IsFinishedOk::Missing) => {
                return Err(JobAwaitError::JobNotFound);
            }
            Ok(IsFinishedOk::NotFinished) => {
                trace!("Poll: job {job_id} not finished yet");
            }
            Err(e) => {
                warn!("Poll for job {job_id}: redis error, retrying: {e}");
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
