use core::time;
use nanoid::nanoid;
use std::{
    cmp,
    fmt::Display,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{Date, DateTime, Utc};
use deadpool_redis::Pool;
use log::trace;
use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::to_string;
use tokio::{
    spawn,
    sync::{
        OwnedSemaphorePermit, Semaphore, SemaphorePermit,
        mpsc::{self, Receiver, Sender, channel},
        watch,
    },
    task::JoinHandle,
    time::sleep,
};
use uuid::Uuid;

use crate::{
    job::LightJobHandle,
    luacommands::{
        InvokeLuaScript as _, MoveStalledJobsToWait, MoveToActive, MoveToActiveResult,
        MoveToActiveReturn, RateLimiter,
    },
    queue::QueueName,
};

pub struct Worker<D, R> {
    pool: Pool,
    queue_name: QueueName,
    semaphore: Arc<Semaphore>,
    background_handles: Vec<JoinHandle<()>>,
    job_receiver: Receiver<LightJobHandle<D, R>>,
    uid: String,
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
            parallel_jobs: 32,
            parallel_connections: 1,
        }
    }
}

impl<D, R> Worker<D, R>
where
    R: Send + 'static,
    D: Send + 'static + DeserializeOwned + std::fmt::Debug,
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
                    semaphore.clone(),
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
        }
    }

    pub async fn pop(&mut self) -> LightJobHandle<D, R> {
        self.job_receiver.recv().await.expect("TODO")
    }
}

async fn lock_refresh() {}

async fn pull_marker(
    pool: Pool,
    queue_name: QueueName,
    sender: mpsc::Sender<(String, DateTime<Utc>)>,
) {
    let mut con = pool.get().await.expect("TODO");
    let marker_name = queue_name.marker();
    loop {
        let res: Option<(String, String, i64)> =
            con.bzpopmin(&marker_name, 30.).await.expect("TODO");
        if res.is_none() {
            continue;
        }
        let (_key, job_id, timestamp) = res.unwrap();
        let ts: DateTime<Utc> = DateTime::from_timestamp_millis(timestamp).expect("TODO");
        sender.send((job_id, ts)).await.expect("TODO");
    }
}

async fn pull_job_thread<D, R>(
    pool: Pool,
    queue_name: QueueName,
    job_sender: Sender<LightJobHandle<D, R>>,
    semaphore: Arc<Semaphore>,
) where
    D: DeserializeOwned + std::fmt::Debug,
{
    let (marker_send, mut marker_recv) = mpsc::channel(1);
    spawn(pull_marker(pool.clone(), queue_name.clone(), marker_send));

    let worker_id = nanoid!();
    let mut counter: usize = 0;
    loop {
        println!("Semaphore");
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore crash");
        println!("...acquired. Getting connection {worker_id}.");
        let start = Instant::now();
        let mut con = pool.get().await.expect("TODO");
        println!(
            "...having connection {worker_id} after {:?}. Dequque.",
            start.elapsed()
        );
        let token = format!("{worker_id}-{counter}");
        counter += 1;
        let mts = MoveToActive::<D> {
            queue: &queue_name,
            worker_id: &worker_id,
            limiter: RateLimiter {
                max: 0,
                duration: Duration::from_millis(0),
            },
            lock_duration: Duration::from_secs(30),
            token: &token,
            phantom: PhantomData, // TODO without
        };
        println!("Dequeue what I can get");
        let get_job = mts.call(&mut con).await.unwrap();
        let sleep_timer =
            match get_job {
                MoveToActiveResult::JobData { id, data } => {
                    let lock_refresh_handle = tokio::spawn(lock_refresh());
                    job_sender
                        .send(LightJobHandle::new(
                            queue_name.clone(),
                            pool.clone(),
                            id,
                            permit,
                            data.data,
                            lock_refresh_handle,
                        ))
                        .await
                        .expect("TODO");
                    None
                }
                MoveToActiveResult::Delay { delay } => Some(delay),
                MoveToActiveResult::WaitUntil { timestamp } => Some(Duration::from_millis(
                    cmp::max(0, (timestamp - Utc::now()).num_milliseconds()) as u64,
                )),
                MoveToActiveResult::NothingToDo => Some(Duration::from_secs(10)),
            };
        if let Some(sleep_timer) = sleep_timer {
            println!("Got sleep: {:?}", sleep_timer);
            // Sleep until known job is ready, but also wake up if new job comes in
            let timeout = sleep(sleep_timer);
            let marker = marker_recv.recv();
            tokio::select! {
                t = timeout => println!("Classical timeout: {:?}",t),
                event = marker => {
                    let (member, score) = event.expect("TODO");
                    println!("EVENT: {}, {}", member, score);
                },
            };
        }
    }
}

// OLD CODE BELOW

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
