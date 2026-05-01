use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    RateLimit,
    bullmq::{
        move_to_finished::{WireKeepJobsConfig, WireMoveToFinishedOpts},
        rate_limiter::WireRateLimiter,
    },
    error::MoveToFinishedErr,
    luacommands::{InvokeLuaScript, MOVE_TO_FINISHED},
    queue::QueueName,
};

/// Domain-side options for `move_to_finished`. Cryptic BullMQ field names
/// (`fpof`/`cpof`/`idof`/`rdof`) are replaced with descriptive ones; the
/// wire form is built at call time.
#[derive(Debug, Clone)]
pub struct FinishOptions {
    /// Lock token issued when the job was moved to active.
    pub lock_token: String,
    /// How long to keep finished jobs.
    pub keep: KeepCount,
    /// Lock duration to refresh on the job.
    pub lock_duration: Duration,
    /// Maximum number of attempts the job is allowed.
    pub attempts: usize,
    /// Maximum size of the metrics ring buffer.
    pub max_metrics_size: usize,
    /// Whether to fail the parent on this job's failure.
    pub fail_parent_on_fail: Option<bool>,
    /// Whether the parent should continue on this job's failure.
    pub continue_parent_on_failure: Option<bool>,
    /// Whether to ignore this dependency on failure.
    pub ignore_dependency_on_fail: Option<bool>,
    /// Whether to remove this dependency on failure.
    pub remove_dependency_on_fail: Option<bool>,
    /// Worker name (for events).
    pub worker_name: String,
    /// Optional rate limiter.
    pub limiter: Option<RateLimit>,
}

/// Bound on how many finished jobs to retain.
#[derive(Debug, Clone, Copy)]
pub struct KeepCount {
    /// `-1` keeps all jobs.
    pub count: i64,
    /// Optional age bound.
    pub age: Option<Duration>,
}

pub struct MoveToFinished<'a, R> {
    /// Name of the queue we are moving the job from
    pub queue: &'a QueueName,
    /// ID of the job to move to finished status
    pub job_id: &'a str,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
    /// Result value (for completed jobs) or failure reason (for failed jobs)
    pub result: Result<&'a R, &'a str>,
    /// Options for the operation
    pub options: FinishOptions,
    /// Job fields to update
    pub job_fields: Option<HashMap<String, String>>,
}

impl<'a, R> InvokeLuaScript for MoveToFinished<'a, R>
where
    R: Serialize,
{
    type DomainOk = ();
    type DomainErr = MoveToFinishedErr;
    type RedisOutput = i64;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let job_fields_bytes = if let Some(jf) = &self.job_fields {
            rmp_serde::to_vec_named(&jf).expect("rmp serde string hashmap should always succeed")
        } else {
            Vec::new()
        };
        let target = if self.result.is_ok() {
            "completed"
        } else {
            "failed"
        };

        let wire = WireMoveToFinishedOpts {
            token: self.options.lock_token.clone(),
            keep_jobs: WireKeepJobsConfig {
                count: self.options.keep.count,
                age: self.options.keep.age,
            },
            lock_duration: self.options.lock_duration,
            attempts: self.options.attempts,
            max_metrics_size: self.options.max_metrics_size,
            fail_parent_on_fail: self.options.fail_parent_on_fail,
            continue_parent_on_failure: self.options.continue_parent_on_failure,
            ignore_dependency_on_fail: self.options.ignore_dependency_on_fail,
            remove_dependency_on_fail: self.options.remove_dependency_on_fail,
            name: self.options.worker_name.clone(),
            limiter: self.options.limiter.as_ref().map(WireRateLimiter::from),
        };

        let mut invocation = MOVE_TO_FINISHED.prepare_invoke();
        invocation
            .key(self.queue.wait())
            .key(self.queue.active())
            .key(self.queue.prioritized())
            .key(self.queue.events())
            .key(self.queue.stalled())
            .key(self.queue.limiter())
            .key(self.queue.delayed())
            .key(self.queue.paused())
            .key(self.queue.meta())
            .key(self.queue.priority_counter())
            .key(if self.result.is_ok() {
                self.queue.completed()
            } else {
                self.queue.failed()
            })
            .key(self.queue.job(self.job_id))
            .key(self.queue.metrics())
            .key(self.queue.marker())
            .arg(self.job_id)
            .arg(self.timestamp.timestamp_millis())
            .arg(if self.result.is_ok() {
                "returnvalue"
            } else {
                "failedReason"
            })
            .arg(match self.result {
                Err(e) => e.to_string(),
                Ok(v) => serde_json::to_string(&v)?,
            })
            .arg(target)
            .arg("0") // Don't fetch next job
            .arg(self.queue.prefix())
            .arg(rmp_serde::to_vec_named(&wire).expect("Should always be able so serialize"))
            .arg(job_fields_bytes);
        Ok(invocation)
    }

    fn map_redis_error(&self, error: redis::RedisError) -> Self::DomainErr {
        error.into()
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            0 => Ok(()),
            -1 => Err(MoveToFinishedErr::MissingKey),
            -2 => Err(MoveToFinishedErr::MissingLock),
            -3 => Err(MoveToFinishedErr::JobNotActive),
            -4 => Err(MoveToFinishedErr::JobHasPendingChildren),
            -6 => Err(MoveToFinishedErr::LockNotOwned),
            -9 => Err(MoveToFinishedErr::JobHasFailedChildren),
            v => panic!("Unexpected lua exit code: {v:#?}"),
        }
    }
}
