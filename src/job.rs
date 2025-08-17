use std::{marker::PhantomData, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use deadpool_redis::Pool;
use log::{debug, trace, warn};
use redis::{AsyncCommands as _, RedisResult};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc::Sender},
    task::{self, JoinHandle},
    time::sleep,
};

use crate::{
    Progress,
    job_options::JobOptions,
    luacommands::{InvokeLuaScript, KeepJobsConfig, MoveToFinished, MoveToFinishedOptions},
    queue::QueueName,
};

const JOB_POLL_ERROR_COOLDOWN: Duration = Duration::from_millis(100);

pub struct LightJobHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    semaphore_permit: OwnedSemaphorePermit,
    data: D,
    phantom: PhantomData<R>, // Result
    lock_refresh_handle: JoinHandle<()>,
    has_been_finished: bool,
    lock_token: String,
    worker_name: String,
}

impl<D, R> LightJobHandle<D, R> {
    pub fn new(
        queue_name: QueueName,
        pool: Pool,
        id: String,
        semaphore_permit: OwnedSemaphorePermit,
        data: D,
        lock_refresh_handle: JoinHandle<()>,
        lock_token: String,
        worker_name: String,
    ) -> Self {
        Self {
            queue_name,
            pool,
            id,
            semaphore_permit,
            data,
            lock_refresh_handle,
            phantom: PhantomData,
            lock_token,
            worker_name,
            has_been_finished: false,
        }
    }

    pub fn data(&self) -> &D {
        &self.data
    }
    async fn finished<'a>(mut self, result: Result<&'a R, &'a str>)
    where
        R: Serialize,
    {
        self.has_been_finished = true;
        self.lock_refresh_handle.abort();
        let move_to_finished = MoveToFinished {
            queue: &self.queue_name,
            job_id: &self.id,
            timestamp: Utc::now(),
            result,
            options: MoveToFinishedOptions {
                lock_token: self.lock_token.clone(),
                keep_jobs: KeepJobsConfig {
                    count: -1,
                    age: None,
                },
                lock_duration: Duration::from_secs(30),
                attempts: 99,
                max_metrics_size: None,
                fail_parent_on_fail: None,
                continue_parent_on_failure: None,
                ignore_dependency_on_fail: None,
                remove_dependency_on_fail: None,
                worker_name: self.worker_name.clone(),
                limiter: None,
            },
            job_fields: Default::default(),
        };
        let mut con = self.pool.get().await.expect("TODO");
        move_to_finished.call(&mut con).await.expect("TODO");
    }
    pub async fn done(mut self, value: &R)
    where
        R: Serialize,
    {
        self.finished(Ok(value)).await
    }
    pub async fn failed(mut self, error: &str)
    where
        R: Serialize,
    {
        self.finished(Err(error)).await
    }
}

impl<D, R> Drop for LightJobHandle<D, R> {
    fn drop(&mut self) {
        if !self.has_been_finished {
            warn!(
                "Job \"{}\" of queue \"{}\" has been dropped without done() or failed() being called.",
                self.id,
                self.queue_name.as_str()
            );
        }
    }
}

// All possible fields a Job can have in the Redis HashMap
struct JobState<D, R> {
    atm: Option<usize>, // attempts made
    data: D,
    delay: Option<Duration>,
    failed_reason: Option<String>,
    finished_on: Option<DateTime<Utc>>,
    name: String,
    opts: Option<JobOptions>,
    priority: Option<usize>,
    progress: Option<Progress>,
    result: Option<R>,
    stc: Option<usize>,
    timestamp: DateTime<Utc>,
    stack_trace: Option<String>,
}

pub struct JobHandle<D, R, P = String, E = String> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    job_state: JobState<D, R>,
    semaphore: OwnedSemaphorePermit,
    lock_refresh_handle: JoinHandle<()>,
    phantom: PhantomData<(D, R, P, E)>, // Data, Result, Progress, Error
}

impl<D, R, P> JobHandle<D, R, P> {
    fn get_id(&self) -> &str {
        &self.id
    }
    fn get_name(&self) -> &str {
        &self.job_state.name
    }
    fn get_pool(&self) -> &Pool {
        &self.pool
    }
}

// async fn wait_for_new_jobs<D, R, P, E>(
//     tx: Sender<JobHandle<D, R, P, E>>,
//     queue_name: QueueName,
//     pool: Pool,
//     semaphore: Arc<Semaphore>,
// ) {
// const TIMEOUT: Duration = Duration::from_secs(120);
// loop {
//     let semaphore_permit = semaphore.clone().acquire_owned().await;
//     if semaphore_permit.is_err() {
//         // Semaphore has been closed
//         return;
//     }
//     let semaphore_permit = semaphore_permit.unwrap();
//     let con = pool.get().await;
//     if let Err(e) = con {
//         trace!("Error during getting Redis connection from pool: {:?}", e);
//         sleep(JOB_POLL_ERROR_COOLDOWN).await;
//         continue;
//     }
//     let mut con = con.unwrap();
//     let job_id_result_future = con.blmove(
//         queue_name.wait(),
//         queue_name.active(),
//         // TODO check if directions are right
//         redis::Direction::Left,
//         redis::Direction::Right,
//         TIMEOUT.as_secs_f64(),
//     );
//     // Await new job id or return if receiver closed
//     let job_id = tokio::select!(
//         job_id_result = job_id_result_future => {
//             let jir: RedisResult<Option<String>> = job_id_result;
//             if let Err(e) = jir {
//                 trace!("RedisError getting Redis Job from queue: {:?}", e);
//                 sleep(JOB_POLL_ERROR_COOLDOWN).await;
//                 continue;
//             }
//             jir.unwrap()
//         },
//         () = tx.closed() => {
//             break;
//         },
//     );
//     if job_id.is_none() {
//         continue;
//     }
//     let job_id = job_id.unwrap();
//
//     let light_job_handle = LightJobHandle {
//         id: job_id,
//         queue_name: queue_name.clone(),
//         pool: pool.clone(),
//         semaphore_permit,
//         lock_refresh_handle: todo!(),
//         phantom: PhantomData::<(D, R, P, E)>,
//     };
//
//     // if job_id_result.is_err() {
//     //     trace!("Error trying to fetch ")
//     // }
//     todo!()
// }
// }

struct Intermediate<D, R> {
    atm: Option<usize>, // attempts made
    data: Option<D>,
    delay: Option<Duration>,
    failed_reason: Option<String>,
    finished_on: Option<DateTime<Utc>>,
    name: Option<String>,
    opts: Option<JobOptions>,
    priority: Option<usize>,
    progress: Option<Progress>,
    result: Option<R>,
    stc: Option<usize>,
    timestamp: Option<DateTime<Utc>>,
    stack_trace: Option<String>,
}

impl<D, R> Default for Intermediate<D, R> {
    fn default() -> Self {
        Self {
            atm: None,
            data: None,
            delay: None,
            failed_reason: None,
            finished_on: None,
            name: None,
            opts: None,
            priority: None,
            progress: None,
            result: None,
            stc: None,
            timestamp: None,
            stack_trace: None,
        }
    }
}

impl<D, R> LightJobHandle<D, R>
where
    D: DeserializeOwned,
    R: DeserializeOwned,
{
    // pub async fn into_job_handle(self) -> Result<JobHandle<D, R>> {
    //     let mut con = self.pool.get().await?;
    //
    //     let mut intermediate: Intermediate<D, R> = Default::default();
    //
    //     let res: Vec<String> = con.hgetall(self.queue_name.job(&self.id)).await?;
    //     if res.len() % 2 != 0 {
    //         bail!("Redis Key Value result must always result in a even length");
    //     }
    //     let mut res_it = res.into_iter();
    //     while let (Some(key), Some(value)) = (res_it.next(), res_it.next()) {
    //         match key.as_str() {
    //             "atm" => intermediate.atm = serde_json::from_str(&value)?,
    //             "data" => intermediate.data = serde_json::from_str(&value)?,
    //             "name" => intermediate.name = serde_json::from_str(&value)?,
    //             "delay" => intermediate.delay = serde_json::from_str(&value)?,
    //             "failedReason" => intermediate.failed_reason = Some(value),
    //             "finishedOn" => {
    //                 intermediate.finished_on = {
    //                     let t: i64 = serde_json::from_str(&value)?;
    //                     DateTime::from_timestamp_millis(t)
    //                 }
    //             }
    //             "opts" => intermediate.opts = serde_json::from_str(&value)?,
    //             "priority" => intermediate.priority = serde_json::from_str(&value)?,
    //             "progress" => intermediate.progress = serde_json::from_str(&value)?,
    //             "result" => intermediate.result = serde_json::from_str(&value)?,
    //             "stc" => intermediate.stc = serde_json::from_str(&value)?,
    //             "timestamp" => {
    //                 intermediate.timestamp = {
    //                     let t: i64 = serde_json::from_str(&value)?;
    //                     DateTime::from_timestamp_millis(t)
    //                 }
    //             }
    //             "stacktrace" => intermediate.stack_trace = Some(value),
    //             unknown => debug!("Unknown key in Job {}: {}", &self.id, unknown),
    //         }
    //     }
    //     if intermediate.data.is_none() {
    //         bail!(
    //             "Job {} in queue {} did not contain job payload.",
    //             self.id,
    //             self.queue_name.as_str()
    //         );
    //     }
    //     if intermediate.timestamp.is_none() {
    //         bail!(
    //             "Job {} in queue {} did not contain timestamp.",
    //             self.id,
    //             self.queue_name.as_str()
    //         );
    //     }
    //     if intermediate.name.is_none() {
    //         bail!(
    //             "Job {} in queue {} did not contain name.",
    //             self.id,
    //             self.queue_name.as_str()
    //         );
    //     }
    //     let job_state = JobState {
    //         atm: intermediate.atm,
    //         data: intermediate.data.unwrap(),
    //         delay: intermediate.delay,
    //         failed_reason: intermediate.failed_reason,
    //         finished_on: intermediate.finished_on,
    //         name: intermediate.name.unwrap(),
    //         opts: intermediate.opts,
    //         priority: intermediate.priority,
    //         progress: intermediate.progress,
    //         result: intermediate.result,
    //         stc: intermediate.stc,
    //         timestamp: intermediate.timestamp.unwrap(),
    //         stack_trace: intermediate.stack_trace,
    //     };
    //     Ok(JobHandle {
    //         queue_name: self.queue_name,
    //         pool: self.pool,
    //         id: self.id,
    //         job_state,
    //         semaphore: self.semaphore_permit,
    //         lock_refresh_handle: todo!(),
    //         phantom: PhantomData,
    //     })
    // }
}
