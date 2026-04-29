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
    ProgressPercent,
    error::{AddLogError, MoveToFinishedErr, UpdateProgressError},
};
use crate::{
    job::JobOptions,
    luacommands::{
        AddLog, GetJobScheduler, InvokeLuaScript, KeepJobsConfig, MoveToFinished,
        MoveToFinishedOptions, UpdateJobScheduler, UpdateProgess,
    },
    queue::QueueName,
    scheduler::compute_cron_next_millis,
};

/// A unit of work obtained from the worker instance to be
/// processed. Call done() or failed() to store results.
/// Dropping will cause the job to be stale (no AsyncDrop).
pub struct JobWorkHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    name: String,
    data: D,
    phantom: PhantomData<R>, // Result
    has_been_finished: bool,
    // The lock token should never be leaked. It will be
    // refreshed by the worker as long as it is referenced.
    lock_token: Arc<str>,
    worker_name: String,
    _semaphore_permit: OwnedSemaphorePermit,
    /// If this job was produced by a scheduler, this is the scheduler id.
    repeat_job_key: Option<String>,
    /// Raw JSON of the job options, used for scheduler updates.
    opts_json: Option<String>,
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
        repeat_job_key: Option<String>,
        opts_json: Option<String>,
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
            repeat_job_key,
            opts_json,
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
    /// If a job is marked as failed, it might be retried, depending on the Job options.
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
            options: MoveToFinishedOptions {
                lock_token: self.lock_token.to_string(),
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
        let mut con = self.pool.get().await?;
        move_to_finished.call(&mut con).await?;

        // If this job belongs to a scheduler, schedule the next iteration (best-effort).
        if let Some(ref scheduler_id) = self.repeat_job_key {
            if let Err(e) = self.update_scheduler_next_job(scheduler_id).await {
                warn!(
                    "Failed to update scheduler {scheduler_id} after job {}: {e:?}",
                    self.id
                );
            }
        }

        Ok(())
    }

    async fn update_scheduler_next_job(
        &self,
        scheduler_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut con = self.pool.get().await?;

        // 1. Fetch scheduler metadata.
        let info = GetJobScheduler {
            queue: &self.queue_name,
            scheduler_id,
        }
        .call(&mut con)
        .await?
        .ok_or("scheduler not found")?;

        // 2. Stop if endDate reached or limit exceeded (optional early-exit).
        let now = Utc::now().timestamp_millis();
        if let Some(end_date) = info.fields.iter().find(|(k, _)| k == "endDate").and_then(|(_, v)| v.parse::<i64>().ok()) {
            if now > end_date {
                return Ok(());
            }
        }
        if let Some(limit) = info.fields.iter().find(|(k, _)| k == "ic").and_then(|(_, v)| v.parse::<u64>().ok()) {
            let max_limit = info.fields.iter().find(|(k, _)| k == "limit").and_then(|(_, v)| v.parse::<u64>().ok());
            if let Some(max) = max_limit {
                if limit >= max {
                    return Ok(());
                }
            }
        }

        // 3. Compute next_millis.
        let every = info.fields.iter().find(|(k, _)| k == "every").and_then(|(_, v)| v.parse::<u64>().ok());
        let next_millis = if let Some(every) = every {
            // Lua script recalculates for 'every' internally; any positive value is fine.
            now + every as i64
        } else if let Some(pattern) = info.fields.iter().find(|(k, _)| k == "pattern").map(|(_, v)| v.clone()) {
            let tz = info.fields.iter().find(|(k, _)| k == "tz").map(|(_, v)| v.as_str());
            compute_cron_next_millis(&pattern, tz, DateTime::from_timestamp_millis(now).unwrap_or_else(Utc::now))?
        } else {
            return Err("scheduler has no every or pattern".into());
        };

        // 4. Parse the stored opts so we can pass them to the update script.
        let delayed_opts: JobOptions = if let Some(ref opts_json) = self.opts_json {
            serde_json::from_str(opts_json).unwrap_or_default()
        } else {
            JobOptions::default()
        };

        // 5. Call updateJobScheduler.
        let _ = UpdateJobScheduler {
            queue: &self.queue_name,
            scheduler_id,
            next_millis,
            delayed_data_json: "{}",
            delayed_opts: &delayed_opts,
            timestamp: now,
            prefix: self.queue_name.prefix(),
            producer_id: Some(&self.id),
        }
        .call(&mut con)
        .await?;

        Ok(())
    }
    /// Mark job as done, by providing an `Ok()` value.
    pub async fn done(self, value: &R) -> Result<(), MoveToFinishedErr>
    where
        R: Serialize,
    {
        self.finished(Ok(value)).await
    }
    /// Mark job as failed, by providing an `Err()` value.
    /// Depending on the job options, a job might be rescheduled,
    pub async fn failed(self, error: &str) -> Result<(), MoveToFinishedErr>
    where
        R: Serialize,
    {
        self.finished(Err(error)).await
    }
    /// Add a log line without timestamp, get the number of log lines.
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
    /// Add a log line with timestamp, get the number of log lines.
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
