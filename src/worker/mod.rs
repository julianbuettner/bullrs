use nanoid::nanoid;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use deadpool_redis::Pool;
use serde::de::DeserializeOwned;
use tokio::{
    sync::{
        RwLock, Semaphore,
        mpsc::{Receiver, channel},
    },
    task::JoinHandle,
};

use crate::{
    job::JobWorkHandle,
    queue::QueueName,
    worker::{stalled_to_wait_handle::stalled_to_wait, workererror::WorkerError},
};

mod drop_handler;
mod pull_job;
mod stalled_to_wait_handle;
mod workererror;
use pull_job::pull_job_thread;

/// The worker makes jobs available for processing.
///
/// The worker maintains one or multiple connections to the Redis database to dequeue
/// jobs.
pub struct Worker<D, R> {
    pool: Pool,
    queue_name: QueueName,
    semaphore: Arc<Semaphore>,
    job_fetch_handles: Vec<JoinHandle<()>>,
    stalled_to_wait_handle: JoinHandle<()>,
    job_receiver: Receiver<Result<JobWorkHandle<D, R>, WorkerError>>,
    stalled_after: Arc<RwLock<Duration>>,
    max_stalled_before_failed: Arc<RwLock<usize>>,
    cooldown_after_error: Duration,
    uid: String,
    /// If terminating has been gracefully initiated we know that
    /// no more jobs are coming from the pull job due to planned termination
    /// and not due to error and self-termination.
    terminating_initiated: AtomicBool,
}

/// Parameterize a worker. The defaults should be fine for most use cases.
/// For high performance applications, try increasing `parallel_jobs` and `parallel_connections`.
#[derive(Clone, Debug)]
pub struct WorkerArgs {
    /// How many jobs should a worker work at once at max.
    /// Parallel jobs should be more than `parallel_connections` to be meaningful.
    /// This value is only used by this worker instance. It can be limited by the global
    /// concurrency set for the queue, which applies for all worker instances together.
    /// Use queue.set_concurrency(None) to clear the concurrency or set it to `Some(higher)` value.
    /// Defaults to 32.
    pub parallel_jobs: usize,
    /// How many parallel connections should a worker have to the Redis database.
    /// The jobs per second are limited by the redis round trip divided by parallel_connections.
    /// Defaults to 1.
    pub parallel_connections: usize,
    /// After this many stalls a job is marked as failed.
    /// Defaults to 1.
    pub max_stalled_before_failed: usize,
    /// After this much time a job is marked as stalled.
    /// This value should be the same for all workers working the same queue,
    /// as workers refresh locks after stalled_after / 2
    /// and a mismatch means one worker thinks it doesn't have to refresh yet,
    /// while the other thinks the job has stalled. Because of this it is
    /// recommended to leave this value at it's default.
    /// Defaults to 30s.
    pub stalled_after: Duration,
    /// When the worker fails to pull the next job, like due to the database being down, how
    /// long should it sleep before retrying. Keep in mind, that every error is emitted
    /// when calling [Worker::next].
    /// Defaults to 3s.
    cooldown_after_error: Duration,
}

impl Default for WorkerArgs {
    fn default() -> Self {
        Self {
            parallel_jobs: 32,
            parallel_connections: 1,
            max_stalled_before_failed: 1,
            stalled_after: Duration::from_secs(30),
            cooldown_after_error: Duration::from_secs(3),
        }
    }
}

impl<D, R> Worker<D, R>
where
    R: Send + 'static,
    D: Send + 'static + DeserializeOwned + std::fmt::Debug,
{
    pub(crate) fn new(pool: Pool, queue_name: QueueName, args: WorkerArgs) -> Self {
        let uid = nanoid!();
        let semaphore = Arc::new(Semaphore::new(args.parallel_jobs));
        let (tx, job_receiver) = channel(args.parallel_jobs);
        let stalled_after = Arc::new(RwLock::new(args.stalled_after));
        let max_stalled_before_failed = Arc::new(RwLock::new(args.max_stalled_before_failed));
        let cooldown_after_error = args.cooldown_after_error;

        let job_fetch_handles: Vec<_> = (0..args.parallel_connections)
            .map(|_| {
                tokio::spawn(pull_job_thread(
                    pool.clone(),
                    queue_name.clone(),
                    tx.clone(),
                    semaphore.clone(),
                    args.cooldown_after_error,
                ))
            })
            .collect();

        let stalled_to_wait_handle = tokio::spawn(stalled_to_wait(
            pool.clone(),
            queue_name.clone(),
            stalled_after.clone(),
            max_stalled_before_failed.clone(),
        ));

        Self {
            uid,
            pool,
            queue_name,
            semaphore,
            job_fetch_handles,
            job_receiver,
            max_stalled_before_failed,
            stalled_to_wait_handle,
            cooldown_after_error: cooldown_after_error,
            stalled_after,
            terminating_initiated: AtomicBool::from(false),
        }
    }

    /// Get the next job. It might have been queued in memory.
    /// Returns `None` if the workers is terminating and all remaining messages have been processed.
    /// The worker will only terminate gracefully or for unrecoverable errors
    /// like if the pool has been closed externally. Note that if there are _n_ parallel
    /// connections configured, there will be _n_ times as many error messages per time.
    /// Please call [JobWorkHandle::done()] or [JobWorkHandle::failed()], otherwise it will be marked
    /// as failed (and retried) when dropped or it will stall if redis is unavailable and the job
    /// can't be moved to failed/retrying.
    pub async fn next(&mut self) -> Option<Result<JobWorkHandle<D, R>, WorkerError>> {
        self.job_receiver.recv().await
    }

    /// Check if a worker has at least one job or error ready.
    /// This will guarantee that [Worker::next] will return `Some`thing.
    pub fn has_next(&self) -> bool {
        !self.job_receiver.is_empty()
    }

    /// Terminate this worker gracefully. [Worker::next] will emit the last pre-loaded
    /// jobs and then return only `None` values.
    pub fn terminate(&self) {
        self.terminating_initiated.store(true, Ordering::SeqCst);
        self.stalled_to_wait_handle.abort();
        for h in self.job_fetch_handles.iter() {
            h.abort();
        }
    }

    fn is_terminating_gracefully(&self) -> bool {
        self.terminating_initiated.load(Ordering::SeqCst)
    }
}

impl<D, R> Drop for Worker<D, R> {
    fn drop(&mut self) {
        self.stalled_to_wait_handle.abort();
        self.job_fetch_handles.iter().for_each(|h| h.abort());
    }
}
