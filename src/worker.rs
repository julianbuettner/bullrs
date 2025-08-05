use std::{fmt::Display, marker::PhantomData, sync::Arc, time::Duration};

use anyhow::Result;
use chrono::Utc;
use deadpool_redis::Pool;
use log::trace;
use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::to_string;
use tokio::{
    spawn,
    sync::{
        OwnedSemaphorePermit, Semaphore, SemaphorePermit,
        mpsc::{Receiver, Sender, channel},
    },
    task::JoinHandle,
    time::sleep,
};
use uuid::Uuid;

use crate::{
    job::LightJobHandle,
    luacommands::{InvokeLuaScript as _, MoveStalledJobsToWait, MoveToActive, RateLimiter},
    queue::QueueName,
};

pub struct Worker<D, R, P = String, E = String> {
    pool: Pool,
    queue_name: QueueName,
    semaphore: Arc<Semaphore>,
    background_handles: Vec<JoinHandle<()>>,
    job_receiver: Receiver<LightJobHandle<D, R, P, E>>,
    uid: String,
    phantom: PhantomData<(D, R, P, E)>, // Data, Result, Progress, Error
}

/// Parameterize a worker. The defaults
/// should be fine for most use cases. For high performance applications,
/// increase parallel_jobs or parallel_connections.
#[derive(Clone, Debug)]
pub struct WorkerArgs {
    /// How many jobs should a worker work at once at max.
    /// Parallel jobs should be more than parallel connections to be meaningful.
    pub parallel_jobs: usize,
    /// How many parallel connections should a worker have to the Redis database.
    /// The jobs per second are limited by redis round trip divided by parallel_connections.
    pub parallel_connections: usize,
}

impl Default for WorkerArgs {
    fn default() -> Self {
        Self {
            parallel_jobs: 332,
            parallel_connections: 1,
        }
    }
}

impl<D, R, P, E> Worker<D, R, P, E>
where
    R: Send + 'static,
    P: Send + 'static,
    D: Send + 'static + DeserializeOwned,
    E: Send + 'static,
{
    pub fn new(pool: Pool, queue_name: QueueName, args: WorkerArgs) -> Self {
        let uid = uuid::Uuid::new_v4().to_string();
        let semaphore = Arc::new(Semaphore::new(args.parallel_jobs));
        let (tx, job_receiver) = channel(args.parallel_jobs);

        let pull_thread_handles: Vec<_> = (0..args.parallel_connections)
            .map(|_| {
                tokio::spawn(pull_job_thread(
                    pool.clone(),
                    queue_name.clone(),
                    tx.clone(),
                ))
            })
            .collect();

        Self {
            uid,
            pool,
            queue_name,
            semaphore,
            background_handles: pull_thread_handles,
            job_receiver,
            phantom: PhantomData,
        }
    }
}

async fn pull_job_thread<D, R, P, E>(
    pool: Pool,
    queue_name: QueueName,
    job_sender: Sender<LightJobHandle<D, R, P, E>>,
) where
    D: DeserializeOwned,
{
    let id = Uuid::new_v4().to_string();
    let mut counter: usize = 0;
    loop {
        let mut con = pool.get().await.unwrap();
        let mts = MoveToActive::<D> {
            queue: &queue_name,
            worker_id: &id,
            limiter: RateLimiter {
                max: 0,
                duration: Duration::from_millis(0),
            },
            lock_duration: Duration::from_secs(30),
            token: &id,
            phantom: PhantomData, // TODO without
        };
        let get_job = mts.call(&mut con).await.unwrap();
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

async fn background_work_setup(pool: &Pool, queue_name: &String) -> anyhow::Result<()> {
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
        if let Err(e) = background_work_setup(&pool, &queue_name).await {
            dbg!(e);
        }
        sleep(Duration::from_millis(500)).await;
    }
}
