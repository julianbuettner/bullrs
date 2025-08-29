use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_redis::Pool;
use log::warn;
use serde::Serialize;
use tokio::{sync::OwnedSemaphorePermit, task::JoinHandle};

use crate::ProgressPercent;
use crate::{
    job::JobOptions,
    luacommands::{
        AddLog, InvokeLuaScript, KeepJobsConfig, MoveToFinished, MoveToFinishedOptions,
        UpdateProgess,
    },
    queue::QueueName,
};

/// A unit of work obtained from the worker instance to be
/// processed. Call done() or failed() to store results.
/// Dropping will cause the job to be stale (no AsyncDrop).
pub struct JobWorkHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    name: String,
    _semaphore_permit: OwnedSemaphorePermit,
    data: D,
    phantom: PhantomData<R>, // Result
    lock_refresh_handle: JoinHandle<()>,
    has_been_finished: bool,
    lock_token: String,
    worker_name: String,
}

impl<D, R> JobWorkHandle<D, R> {
    pub fn new(
        queue_name: QueueName,
        pool: Pool,
        id: String,
        name: String,
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
            name,
            _semaphore_permit: semaphore_permit,
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
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn name(&self) -> &str {
        &self.name
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
                max_metrics_size: 10_000,
                fail_parent_on_fail: None,
                continue_parent_on_failure: None,
                ignore_dependency_on_fail: None,
                remove_dependency_on_fail: None,
                worker_name: self.worker_name.clone(),
                limiter: None,
            },
            job_fields: None,
        };
        let mut con = self.pool.get().await.expect("TODO");
        move_to_finished.call(&mut con).await.expect("TODO");
    }
    pub async fn done(self, value: &R)
    where
        R: Serialize,
    {
        self.finished(Ok(value)).await
    }
    pub async fn failed(self, error: &str)
    where
        R: Serialize,
    {
        self.finished(Err(error)).await
    }
    pub async fn log(&self, log_line: &str) {
        let add_log = AddLog {
            queue: &self.queue_name,
            job_id: &self.id,
            log_line,
            keep_logs: None,
        };
        let mut con = self.pool.get().await.expect("TODO");
        add_log.call(&mut con).await.expect("TODO");
    }
    pub async fn log_ts(&self, log_line: &str) {
        let new_log = format!(
            "{} {log_line}",
            humantime::format_rfc3339_millis(SystemTime::now())
        );
        self.log(&new_log).await
    }
    pub async fn set_progress(&self, progress: ProgressPercent) {
        let update_progress = UpdateProgess {
            queue: &self.queue_name,
            job_id: &self.id,
            progress,
        };
        let mut con = self.pool.get().await.expect("TODO");
        update_progress.call(&mut con).await.expect("TODO");
    }
}

impl<D, R> Drop for JobWorkHandle<D, R> {
    fn drop(&mut self) {
        if !self.has_been_finished {
            self.lock_refresh_handle.abort();
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
    progress: Option<ProgressPercent>,
    result: Option<R>,
    stc: Option<usize>,
    timestamp: DateTime<Utc>,
    stack_trace: Option<String>,
}
