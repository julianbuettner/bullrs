use std::{fmt::Display, marker::PhantomData, sync::Arc, time::Duration};

use anyhow::Result;
use chrono::Utc;
use deadpool_redis::Pool;
use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use serde::de::DeserializeOwned;
use tokio::{
    spawn,
    sync::{Semaphore, SemaphorePermit},
    task::JoinHandle,
    time::sleep,
};

use crate::{job::JobState, luacommands::MoveStalledJobsToWait, queue::QueueName};

pub struct OwnedJobHandle<D, R, P = String> {
    queue_name: QueueName,
    pool: Pool,
    pub id: String,
    pub name: String,
    pub data: D,
    semaphore: Arc<Semaphore>,
    lock_refresh_handle: JoinHandle<()>,
    phantom: PhantomData<(R, P)>,
}

impl<D, R, P> Drop for OwnedJobHandle<D, R, P> {
    fn drop(&mut self) {
        // The owned variant reduced the semaphore permit count by one (by forgetting),
        // simulating holding a owned semaphore permit
        self.semaphore.add_permits(1);
        self.lock_refresh_handle.abort();
    }
}

pub struct LightJobHandle<'a, D, R, P = String> {
    queue_name: &'a QueueName,
    pool: &'a Pool,
    pub id: String,
    pub name: String,
    pub data: D,
    semaphore_permit: SemaphorePermit<'a>,
    phantom: PhantomData<(R, P)>,
    lock_refresh_handle: JoinHandle<()>,
}

impl<'a, D, R, P> Drop for LightJobHandle<'a, D, R, P> {
    fn drop(&mut self) {
        self.lock_refresh_handle.abort();
    }
}

pub trait JobHandle<D> {
    fn get_id(&self) -> &str;
    fn get_name(&self) -> &str;
    fn get_pool(&self) -> &Pool;

    async fn failed(&self, error: impl Display) -> anyhow::Result<()> {
        let mut con = self.get_pool().get().await?;
        Ok(())
    }
}

impl<D, R, P> JobHandle<D> for OwnedJobHandle<D, R, P> {
    fn get_id(&self) -> &str {
        &self.id
    }
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_pool(&self) -> &Pool {
        &self.pool
    }
}

impl<'a, D, R, P> JobHandle<D> for LightJobHandle<'a, D, R, P> {
    fn get_id(&self) -> &str {
        &self.id
    }
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_pool(&self) -> &Pool {
        &self.pool
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

impl CallbackWorker {
    pub async fn new(pool: Pool, queue_name: impl ToString, parallelity: usize) -> Self {
        let background_handle =
            tokio::task::spawn(background_work(pool.clone(), queue_name.to_string()));
        Self {
            pool,
            queue_name: queue_name.to_string(),
            semaphore: Semaphore::new(std::cmp::max(parallelity, 1)),
            background_handle,
        }
    }

    pub async fn get_job_blocking<D, R, P>(&self) -> Result<JobHandle<D, R, P>>
    where
        P: DeserializeOwned,
        D: DeserializeOwned,
        R: DeserializeOwned,
    {
        let mut con = self.pool.get().await?;
        let semaphore_handle = self.semaphore.acquire().await?;
        let job_id: String;
        loop {
            let job_id_result: Option<String> = con
                .blmove(
                    format!("bull:{}:wait", self.queue_name),
                    format!("bull:{}:active", self.queue_name),
                    redis::Direction::Left,
                    redis::Direction::Right,
                    Duration::from_secs(3).as_secs_f64(),
                )
                .await?;
            if job_id_result.is_none() {
                continue;
            }
            job_id = job_id_result.unwrap();
            break;
        }

        let job_state: JobState<_, R> = con
            .hgetall(format!("bull:{}:{}", self.queue_name, job_id))
            .await?;
        Ok(JobHandle {
            id: job_id,
            name: job_state.name,
            data: job_state.data,
            queue_name: &self.queue_name,
            pool: &self.pool,
            semaphore_handle,
            phantom: PhantomData,
        })
    }

    pub async fn work_blocking_callback<D, R, P>(
        &self,
        callback: impl AsyncFn(JobHandle<D, R, P>) -> Result<R, String>,
    ) -> Result<()>
    where
        P: DeserializeOwned,
        D: DeserializeOwned,
        R: DeserializeOwned,
    {
        loop {
            let job = self.get_job_blocking().await?;
            match  callback(job).await {
                Ok(v) => job.
            }
        }
    }
}
