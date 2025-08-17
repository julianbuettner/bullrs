use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::{RedisResult, aio::ConnectionLike, Value};
use serde::Serialize;

use crate::{
    luacommands::{InvokeLuaScript, MOVE_TO_FINISHED},
    queue::QueueName,
};

pub struct MoveToFinished<'a> {
    /// Name of the queue we are moving the job from
    pub queue: &'a QueueName,
    /// ID of the job to move to finished status
    pub job_id: &'a str,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
    /// Return value (for completed jobs) or failure reason (for failed jobs)
    pub message: &'a str,
    /// Target status: "completed" or "failed"
    pub target: MoveToFinishedTarget,
    /// Whether to fetch the next job after finishing this one
    pub fetch_next: bool,
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
    pub token: String,
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
    /// Worker name
    pub name: String,
    /// Rate limiter configuration
    pub limiter: Option<RateLimiter>,
}

#[derive(Debug, Serialize)]
pub struct KeepJobsConfig {
    pub count: usize,
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
    /// Next job data (when fetch_next is true)
    NextJob {
        job_id: String,
        data: HashMap<String, String>,
        expire_time: Option<Duration>,
        next_delayed: Option<DateTime<Utc>>,
    },
    /// Rate limited
    RateLimited {
        expire_time: Duration,
    },
    /// Queue is paused or maxed
    QueuePausedOrMaxed,
    /// Next delayed job timestamp
    NextDelayed {
        timestamp: DateTime<Utc>,
    },
}

impl<'a> InvokeLuaScript for MoveToFinished<'a> {
    type Return = MoveToFinishedResult;

    async fn call(
        self,
        con: &mut impl ConnectionLike,
    ) -> RedisResult<Self::Return> {
        let job_fields = self.job_fields.unwrap_or_default();
        let job_fields_bytes = rmp_serde::to_vec(&job_fields).unwrap();

        let result: RedisResult<Value> = MOVE_TO_FINISHED
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
            .key(match self.target {
                MoveToFinishedTarget::Completed => self.queue.completed(),
                MoveToFinishedTarget::Failed => self.queue.failed(),
            })
            .key(self.queue.job(self.job_id))
            .key(self.queue.metrics())
            .key(self.queue.marker())
            .arg(self.job_id)
            .arg(self.timestamp.timestamp_millis())
            .arg(match self.target {
                MoveToFinishedTarget::Completed => "returnvalue",
                MoveToFinishedTarget::Failed => "failedReason",
            })
            .arg(self.message)
            .arg(self.target.as_str())
            .arg(if self.fetch_next { "1" } else { "0" })
            .arg(self.queue.prefix())
            .arg(rmp_serde::to_vec_named(&self.options).unwrap())
            .arg(job_fields_bytes)
            .invoke_async(con)
            .await;

        let result = result?;
        
        match result {
            Value::Int(code) => {
                match code {
                    0 => Ok(MoveToFinishedResult::Ok),
                    -1 => Ok(MoveToFinishedResult::MissingKey),
                    -2 => Ok(MoveToFinishedResult::MissingLock),
                    -3 => Ok(MoveToFinishedResult::JobNotActive),
                    -4 => Ok(MoveToFinishedResult::JobHasPendingChildren),
                    -6 => Ok(MoveToFinishedResult::LockNotOwned),
                    -9 => Ok(MoveToFinishedResult::JobHasFailedChildren),
                    _ => Err(redis::RedisError::from((
                        redis::ErrorKind::ResponseError,
                        "Unexpected exit code",
                        format!("Exit code: {}", code),
                    ))),
                }
            }
            redis::Value::Array(items) if self.fetch_next && items.len() == 4 => {
                // Parse next job data
                let job_id = items[1].as_str().unwrap_or("").to_string();
                let data = parse_job_data(&items[0])?;
                let expire_time = if items[2].as_i64().unwrap_or(0) > 0 {
                    Some(Duration::from_millis(items[2].as_i64().unwrap() as u64))
                } else {
                    None
                };
                let next_delayed = if items[3].as_i64().unwrap_or(0) > 0 {
                    Some(DateTime::from_timestamp_millis(items[3].as_i64().unwrap()).unwrap())
                } else {
                    None
                };

                Ok(MoveToFinishedResult::NextJob {
                    job_id,
                    data,
                    expire_time,
                    next_delayed,
                })
            }
            redis::Value::Array(items) if items.len() == 4 => {
                // Parse rate limiting or other status
                let expire_time = items[2].as_i64().unwrap_or(0);
                let next_delayed = items[3].as_i64().unwrap_or(0);

                if expire_time > 0 {
                    Ok(MoveToFinishedResult::RateLimited {
                        expire_time: Duration::from_millis(expire_time as u64),
                    })
                } else if next_delayed > 0 {
                    Ok(MoveToFinishedResult::NextDelayed {
                        timestamp: DateTime::from_timestamp_millis(next_delayed).unwrap(),
                    })
                } else {
                    Ok(MoveToFinishedResult::QueuePausedOrMaxed)
                }
            }
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Unexpected response format",
                format!("Response: {:?}", result),
            ))),
        }
    }
}

fn parse_job_data(value: &Value) -> RedisResult<HashMap<String, String>> {
    let mut data = HashMap::new();
    
    if let redis::Value::Array(items) = value {
        for chunk in items.chunks(2) {
            if chunk.len() == 2 {
                if let (Some(key), Some(value)) = (chunk[0].as_str(), chunk[1].as_str()) {
                    data.insert(key.to_string(), value.to_string());
                }
            }
        }
    }
    
    Ok(data)
}

use std::collections::HashMap; 