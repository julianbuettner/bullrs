use std::{fmt::Display, marker::PhantomData, sync::Arc, time::Duration};

use anyhow::Result;
use chrono::Utc;
use deadpool_redis::Pool;
use log::trace;
use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use serde::de::DeserializeOwned;
use tokio::{
    spawn,
    sync::{
        OwnedSemaphorePermit, Semaphore, SemaphorePermit,
        mpsc::{Receiver, Sender, channel},
    },
    task::JoinHandle,
    time::sleep,
};

use crate::{
    job::LightJobHandle, job_depre::JobState, luacommands::{InvokeLuaScript as _, MoveStalledJobsToWait}, queue::QueueName
};

pub struct Worker<D, R, P = String, E = String> {
    pool: Pool,
    queue_name: QueueName,
    semaphore: Arc<Semaphore>,
    background_handles: JoinHandle<Vec<()>>,
    job_receiver: Receiver<LightJobHandle<D, R, P, E>>,
    phantom: PhantomData<(D, R, P, E)>, // Data, Result, Progress, Error
}

/// Parameterize a worker. The defaults
/// should be fine. For high performance applications,
/// increase parallel_jobs or parallel_connections.
#[derive(Clone, Debug)]
pub struct WorkerArgs {
    /// How many jobs should a worker work at once at max.
    /// Parallel jobs should be more than parallel connections to be meaningful.
    pub parallel_jobs: usize,
    /// How many parallel connections should a worker have to the Redis database.
    /// The jobs per second are limited by parallel_connections divided by redis ping.
    pub parallel_connections: usize,
}

impl Default for WorkerArgs {
    fn default() -> Self {
        Self {
            parallel_jobs: 1024,
            parallel_connections: 32,
        }
    }
}

impl<D, R, P, E> Worker<D, R, P, E> {
    fn new(pool: Pool, queue_name: QueueName, args: WorkerArgs) -> Self {
        let semaphore = Arc::new(Semaphore::new(args.parallel_jobs));
        let (tx, job_receiver) = channel(args.parallel_jobs);

        Self {
            pool,
            queue_name,
            semaphore,
            background_handles: todo!(),
            job_receiver,
            phantom: PhantomData,
        }
    }
}

pub struct CallbackWorker {
    pool: Pool,
    queue_name: String,
    semaphore: Semaphore,
    background_handle: JoinHandle<()>,
}

impl Drop for CallbackWorker {
    fn drop(&mut self) {
        self.background_handle.abort();
    }
}

async fn background_work_setp(pool: &Pool, queue_name: &String) -> anyhow::Result<()> {
    let mut con = pool.get().await?;
    let q = QueueName::new(queue_name);
    let job = MoveStalledJobsToWait {
        queue: &q,
        max_stalled_before_failed: 16,
        timestamp: Utc::now(),
        max_duration: Duration::from_millis(1_000),
    };
    let (failed, stalled) = job.call(&mut con).await?;

    if !failed.is_empty() {
        dbg!(failed);
    }
    if !stalled.is_empty() {
        dbg!(stalled);
    }
    Ok(())
}

async fn background_work(pool: Pool, queue_name: String) -> () {
    loop {
        if let Err(e) = background_work_setp(&pool, &queue_name).await {
            dbg!(e);
        }
        sleep(Duration::from_millis(500)).await;
    }
}
