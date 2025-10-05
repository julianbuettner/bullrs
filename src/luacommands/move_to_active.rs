use core::marker::PhantomData;
use std::{collections::HashMap, string::FromUtf8Error, time::Duration};

use chrono::{DateTime, Utc};
use redis::{RedisError, Value};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::{
    luacommands::{InvokeLuaScript, MOVE_TO_ACTIVE},
    queue::QueueName,
    redisext::{RedisHashMapError, RedisHashMapExt},
};

pub struct MoveToActive<'a, D: DeserializeOwned> {
    pub queue: &'a QueueName,
    pub worker_id: &'a str,
    pub limiter: RateLimiter,
    pub lock_duration: Duration,
    pub token: &'a str, // should be random
    pub phantom: PhantomData<D>,
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct RateLimiter {
    pub max: usize,
    #[serde(with = "crate::milliserde::duration_millis")]
    pub duration: Duration,
}

/// Object obtain from MoveToActive
#[derive(Debug)]
pub struct ActiveJob<D: DeserializeOwned> {
    pub name: String,
    pub data: D,
    pub priority: Option<usize>,
    pub timestamp: DateTime<Utc>,
    pub processed_on: Option<DateTime<Utc>>,
    pub delay: Option<Duration>,
    // pub opts: Option<JobOptions>,
    // Moved from active to stalled to ready
    pub stc: Option<usize>,
}

#[derive(Debug)]
enum JobDataOrExitCode {
    JobData(HashMap<String, String>),
    ExitCode(i64),
}

#[derive(Debug)]
pub enum MoveToActiveOk<D> {
    JobData {
        id: String,
        data: D,
    },
    /// Named expireTime in lua script
    Delay {
        delay: Duration,
    },
    /// Named nextTimestamp in lua script
    WaitUntil {
        timestamp: DateTime<Utc>,
    },
    /// No (delayed) jobs there, queue is paused, or reached maximal concurrency
    NothingToDo,
}

#[derive(Debug, Error)]
pub enum MoveToActiveErr {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),
    #[error("failed to load job data (payload?): {0}")]
    RedisHashMapError(#[from] RedisHashMapError),
    #[error("expected valid utf8-string from redis: {0:?}")]
    RedisStringInvalid(#[from] FromUtf8Error),
    #[error("lua job did not return hash map as expected: {0:?}")]
    UnexpectedRedisValue(Value),
    #[error("Unexpected lua script return values: {1} {2} {3} - {0:?}")]
    UnexpectedLuaOutput(Value, String, u64, i64),
    #[error("Bad timestamp: {0}")]
    BadTimestamp(i64),
}

impl<'a, D> InvokeLuaScript for MoveToActive<'a, D>
where
    D: DeserializeOwned,
{
    type DomainOk = MoveToActiveOk<ActiveJob<D>>;
    type DomainErr = MoveToActiveErr;
    type RedisOutput = (Value, String, u64, i64);

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = MOVE_TO_ACTIVE.prepare_invoke();
        #[derive(Debug, Serialize)]
        struct Opts<'a> {
            token: &'a str,
            #[serde(with = "crate::milliserde::duration_millis", rename = "lockDuration")]
            lock_duration: Duration,
            limiter: RateLimiter,
            name: &'a str,
        }

        let opts = Opts {
            token: self.token,
            lock_duration: self.lock_duration,
            limiter: self.limiter,
            name: self.worker_id,
        };

        let now = Utc::now();

        invocation
            .key(self.queue.wait())
            .key(self.queue.active())
            .key(self.queue.prioritized())
            .key(self.queue.events()) // check if correct
            .key(self.queue.stalled())
            .key(self.queue.limiter())
            .key(self.queue.delayed())
            .key(self.queue.paused())
            .key(self.queue.meta())
            .key(self.queue.priority_counter())
            .key(self.queue.marker())
            .arg(self.queue.prefix())
            .arg(now.timestamp_millis())
            .arg(rmp_serde::to_vec_named(&opts).unwrap());
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        let (job_data, job_id, expire_time, next_timestamp) = value;

        let res = match (&job_data, expire_time, next_timestamp) {
            (Value::Int(0), et, 0) if et != 0 => MoveToActiveOk::Delay {
                delay: Duration::from_millis(et),
            },
            (Value::Int(0), 0, nt) if nt != 0 => MoveToActiveOk::WaitUntil {
                timestamp: DateTime::from_timestamp_millis(nt)
                    .ok_or(MoveToActiveErr::BadTimestamp(nt))?,
            },
            (Value::Int(0), 0, 0) => MoveToActiveOk::NothingToDo,
            (value, 0, 0) => MoveToActiveOk::JobData {
                id: job_id,
                data: active_job_from_hashmap(job_data_map(value)?)?,
            },
            _ => Err(MoveToActiveErr::UnexpectedLuaOutput(
                job_data,
                job_id,
                expire_time,
                next_timestamp,
            ))?,
        };
        Ok(res)
    }
}

fn active_job_from_hashmap<D: DeserializeOwned>(
    data: HashMap<String, String>,
) -> Result<ActiveJob<D>, RedisHashMapError> {
    Ok(ActiveJob {
        name: data.get_v("name")?.into(),
        data: data.extract("data")?,
        priority: data.extract_opt("priority")?,
        timestamp: data.extract_timestamp_ms("timestamp")?,
        processed_on: None,
        delay: data
            .extract_opt::<i64>("delay")?
            .map(|d| Duration::from_millis(std::cmp::max(0, d) as u64)),
        stc: None,
    })
}

fn job_data_map(v: &Value) -> Result<HashMap<String, String>, MoveToActiveErr> {
    match &v {
        redis::Value::Array(m) => {
            let mut res = HashMap::new();
            let mut values_iter = m.into_iter();
            loop {
                let (a, b) = (values_iter.next(), values_iter.next());
                match (a, b) {
                    (None, None) => return Ok(res),
                    (Some(Value::BulkString(a)), Some(Value::BulkString(b))) => {
                        res.insert(String::from_utf8(a.clone())?, String::from_utf8(b.clone())?);
                    }
                    _ => return Err(MoveToActiveErr::UnexpectedRedisValue(v.clone())),
                }
            }
        }
        _ => Err(MoveToActiveErr::UnexpectedRedisValue(v.clone())),
    }
}
