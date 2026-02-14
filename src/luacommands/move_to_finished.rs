use std::{collections::HashMap, time::Duration};

use crate::error::MoveToFinishedErr;
use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;

use crate::{
    luacommands::{InvokeLuaScript, MOVE_TO_FINISHED},
    queue::QueueName,
};

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
    pub options: MoveToFinishedOptions,
    /// Job fields to update
    pub job_fields: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum MoveToFinishedTarget {
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
}

impl MoveToFinishedTarget {
    fn as_str(&self) -> &'static str {
        match self {
            MoveToFinishedTarget::Completed => "completed",
            MoveToFinishedTarget::Failed => "failed",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MoveToFinishedOptions {
    /// Lock token for the job
    #[serde(rename = "token")]
    pub lock_token: String,
    /// How many jobs to keep after processing
    #[serde(rename = "keepJobs")]
    pub keep_jobs: KeepJobsConfig,
    /// Lock duration in milliseconds
    #[serde(with = "crate::milliserde::duration_millis", rename = "lockDuration")]
    pub lock_duration: Duration,
    /// Maximum attempts for the job
    pub attempts: usize,
    /// Maximum metrics size
    #[serde(rename = "maxMetricsSize")]
    pub max_metrics_size: usize,
    /// Whether to fail parent on failure
    #[serde(rename = "fpof")]
    pub fail_parent_on_fail: Option<bool>,
    /// Whether to continue parent on failure
    #[serde(rename = "cpof")]
    pub continue_parent_on_failure: Option<bool>,
    /// Whether to ignore dependency on failure
    #[serde(rename = "idof")]
    pub ignore_dependency_on_fail: Option<bool>,
    /// Whether to remove dependency on failure
    #[serde(rename = "rdof")]
    pub remove_dependency_on_fail: Option<bool>,
    /// Name of the processing worker
    #[serde(rename = "name")]
    pub worker_name: String,
    /// Rate limiter configuration
    pub limiter: Option<RateLimiter>,
}

#[derive(Debug, Serialize)]
pub struct KeepJobsConfig {
    pub count: i64,
    #[serde(with = "crate::milliserde::duration_millis_option")]
    pub age: Option<Duration>,
}

#[derive(Debug, Serialize)]
pub struct RateLimiter {
    pub max: usize,
    #[serde(with = "crate::milliserde::duration_millis")]
    pub duration: Duration,
}

#[derive(Debug, Error)]
pub enum MoveToFinishedErrXXXXXXXXX {
    /// Missing key
    #[error("job has not been found")]
    MissingKey,
    /// Missing lock
    #[error("job lock doesn't exist (anymore?)")]
    MissingLock,
    /// Job not in active set
    #[error("job was not in the active set")]
    JobNotActive,
    /// Job has pending children
    #[error("job has pending children")]
    JobHasPendingChildren,
    /// Lock is not owned by this client
    #[error("job lock is owned by other worker")]
    LockNotOwned,
    /// Job has failed children
    #[error("job has failed children")]
    JobHasFailedChildren,
    /// Unexpected return value from redis script
    #[error("lua script returned unexpected value: {0}")]
    UnexpectedLuaExitCode(i64),
    /// Failed to serialize job result
    #[error("failed to serialize job result: {0:?}")]
    Serialize(#[from] serde_json::Error),
    /// Some error occured in the Redis protocol
    #[error("something went wrong with redis: {0:?}")]
    RedisError(#[from] redis::RedisError),
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
            // Very empty object
            Vec::new()
        };
        let target = if self.result.is_ok() {
            "completed"
        } else {
            "failed"
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
            .arg(
                rmp_serde::to_vec_named(&self.options)
                    .expect("Should always be able so serialize job options"),
            )
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
