use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use redis::{RedisResult, Value, aio::ConnectionLike};
use serde::Serialize;

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
    pub keep_jobs: KeepJobsConfig,
    /// Lock duration in milliseconds
    #[serde(with = "crate::milliserde::duration_millis", rename = "lockDuration")]
    pub lock_duration: Duration,
    /// Maximum attempts for the job
    pub attempts: usize,
    /// Maximum metrics size
    pub max_metrics_size: Option<usize>,
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

#[derive(Debug)]
pub enum MoveToFinishedResult {
    /// Operation successful
    Ok,
    /// Missing key
    MissingKey,
    /// Missing lock
    MissingLock,
    /// Job not in active set
    JobNotActive,
    /// Job has pending children
    JobHasPendingChildren,
    /// Lock is not owned by this client
    LockNotOwned,
    /// Job has failed children
    JobHasFailedChildren,
}

impl<'a, R> InvokeLuaScript for MoveToFinished<'a, R>
where
    R: Serialize,
{
    type Return = MoveToFinishedResult;

    async fn call(self, con: &mut impl ConnectionLike) -> RedisResult<Self::Return> {
        let job_fields = self.job_fields.unwrap_or_default();
        let job_fields_bytes = rmp_serde::to_vec(&job_fields).unwrap();
        let target = if self.result.is_ok() {
            "completed"
        } else {
            "failed"
        };

        let result: Value = MOVE_TO_FINISHED
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
                Ok(v) => serde_json::to_string(&v).expect("TODO"),
            })
            .arg(target)
            .arg("0") // Don't fetch next job
            .arg(self.queue.prefix())
            .arg(rmp_serde::to_vec_named(&self.options).unwrap())
            .arg(job_fields_bytes)
            .invoke_async(con)
            .await?;

        match result {
            Value::Int(code) => match code {
                0 => Ok(MoveToFinishedResult::Ok),
                -1 => Ok(MoveToFinishedResult::MissingKey),
                -2 => Ok(MoveToFinishedResult::MissingLock),
                -3 => Ok(MoveToFinishedResult::JobNotActive),
                -4 => Ok(MoveToFinishedResult::JobHasPendingChildren),
                -6 => Ok(MoveToFinishedResult::LockNotOwned),
                -9 => Ok(MoveToFinishedResult::JobHasFailedChildren),
                _ => Err(redis::RedisError::from((
                    redis::ErrorKind::ResponseError,
                    "Unexpected exit code from moveToFinished",
                    format!("Exit code: {}", code),
                ))),
            },
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Unexpected response format from moveToFinished",
                format!("Response: {:?}", result),
            ))),
        }
    }
}

