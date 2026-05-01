use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use deadpool_redis::Pool;
use serde::Serialize;
use tokio::sync::OwnedSemaphorePermit;
use tracing::warn;

use crate::{
    ProgressPercent, Repeat, SchedulerId,
    error::{AddLogError, MoveToFinishedErr, UpdateProgressError},
};
use crate::{
    job::JobOptions,
    luacommands::{
        AddLog, FinishOptions, GetJobScheduler, InvokeLuaScript, KeepCount, MoveToFinished,
        UpdateJobScheduler, UpdateProgess,
    },
    queue::QueueName,
    scheduler::{compute_cron_next_millis, compute_next_millis},
};

/// A unit of work obtained from the worker instance to be processed.
/// Call done() or failed() to store results.
/// Dropping will cause the job to be stale (no AsyncDrop).
pub struct JobWorkHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    name: String,
    data: D,
    phantom: PhantomData<R>,
    has_been_finished: bool,
    lock_token: Arc<str>,
    worker_name: String,
    _semaphore_permit: OwnedSemaphorePermit,
    /// Scheduler that produced this job, if any.
    scheduled_by: Option<SchedulerId>,
    /// Job options as stored on the job hash, used for scheduler updates.
    options: Option<JobOptions>,
}

impl<D, R> JobWorkHandle<D, R> {
    pub fn new(
        queue_name: QueueName,
        pool: Pool,
        id: String,
        name: String,
        semaphore_permit: OwnedSemaphorePermit,
        data: D,
        lock_token: Arc<str>,
        worker_name: String,
        scheduled_by: Option<SchedulerId>,
        options: Option<JobOptions>,
    ) -> Self {
        Self {
            queue_name,
            pool,
            id,
            name,
            _semaphore_permit: semaphore_permit,
            data,
            phantom: PhantomData,
            lock_token,
            worker_name,
            has_been_finished: false,
            scheduled_by,
            options,
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

    /// Mark a job as done or failed, depending on the `Result` value.
    pub async fn finished<'a>(
        mut self,
        result: Result<&'a R, &'a str>,
    ) -> Result<(), MoveToFinishedErr>
    where
        R: Serialize,
    {
        self.has_been_finished = true;
        let move_to_finished = MoveToFinished {
            queue: &self.queue_name,
            job_id: &self.id,
            timestamp: Utc::now(),
            result,
            options: FinishOptions {
                lock_token: self.lock_token.to_string(),
                keep: KeepCount {
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
        let mut con = self.pool.get().await?;
        move_to_finished.call(&mut con).await?;

        if let Some(ref scheduler_id) = self.scheduled_by
            && let Err(e) = self.update_scheduler_next_job(scheduler_id).await {
                warn!(
                    "Failed to update scheduler {} after job {}: {e:?}",
                    scheduler_id.as_ref(),
                    self.id
                );
            }

        Ok(())
    }

    async fn update_scheduler_next_job(
        &self,
        scheduler_id: &SchedulerId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut con = self.pool.get().await?;

        let info = GetJobScheduler {
            queue: &self.queue_name,
            scheduler_id,
        }
        .call(&mut con)
        .await?
        .ok_or("scheduler not found")?;

        let now = Utc::now();
        let now_ms = now.timestamp_millis();

        if let Some(end) = info.window.end
            && now > end {
                return Ok(());
            }
        if let (Some(limit), Some(ic)) = (info.window.limit, info.iteration_count)
            && ic >= limit {
                return Ok(());
            }

        let next_millis = match info.repeat {
            Some(Repeat::Every { .. }) => {
                // Lua script realigns "every" schedules.
                now_ms + 1
            }
            Some(ref r @ Repeat::Cron { .. }) => match compute_next_millis(r, now) {
                Ok(ms) => ms,
                Err(_) => match r {
                    Repeat::Cron { pattern, tz } => compute_cron_next_millis(pattern, *tz, now)?,
                    _ => unreachable!(),
                },
            },
            None => return Err("scheduler has no repeat rule".into()),
        };

        let delayed_opts = self.options.clone().unwrap_or_default();

        let _ = UpdateJobScheduler {
            queue: &self.queue_name,
            scheduler_id,
            next_millis,
            delayed_data_json: "{}",
            delayed_opts: &delayed_opts,
            timestamp: now_ms,
            prefix: self.queue_name.prefix(),
            producer_id: Some(&self.id),
        }
        .call(&mut con)
        .await?;

        Ok(())
    }

    /// Mark job as done.
    pub async fn done(self, value: &R) -> Result<(), MoveToFinishedErr>
    where
        R: Serialize,
    {
        self.finished(Ok(value)).await
    }
    /// Mark job as failed.
    pub async fn failed(self, error: &str) -> Result<(), MoveToFinishedErr>
    where
        R: Serialize,
    {
        self.finished(Err(error)).await
    }
    /// Add a log line, get the new log count.
    pub async fn log(&self, log_line: &str) -> Result<usize, AddLogError> {
        let add_log = AddLog {
            queue: &self.queue_name,
            job_id: &self.id,
            log_line,
            keep_logs: None,
        };
        let mut con = self.pool.get().await?;
        Ok(add_log.call(&mut con).await?.new_count)
    }
    /// Add a log line with timestamp, get the new log count.
    pub async fn log_ts(&self, log_line: &str) -> Result<usize, AddLogError> {
        let new_log = format!(
            "{} {log_line}",
            humantime::format_rfc3339_millis(SystemTime::now())
        );
        self.log(&new_log).await
    }
    /// Set the progress of the current job in percent.
    pub async fn set_progress(&self, progress: ProgressPercent) -> Result<(), UpdateProgressError> {
        let update_progress = UpdateProgess {
            queue: &self.queue_name,
            job_id: &self.id,
            progress,
        };
        let mut con = self.pool.get().await?;
        update_progress.call(&mut con).await
    }
}

impl<D, R> Drop for JobWorkHandle<D, R> {
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

// TODO: allow to fetch those datapoints from job
#[allow(dead_code)]
struct JobState<D, R> {
    atm: Option<usize>,
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
