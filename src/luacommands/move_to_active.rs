use core::{marker::PhantomData, todo};
use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use redis::{FromRedisValue, RedisResult};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    luacommands::{InvokeLuaScript, MOVE_TO_ACTIVE},
    queue::QueueName,
    redisext::RedisHashMapExt,
};

pub struct MoveToActive<'a, D: DeserializeOwned> {
    pub queue: &'a QueueName,
    pub worker_id: &'a str,
    pub limiter: RateLimiter,
    pub lock_duration: Duration,
    pub token: &'a str, // should be random
    pub phantom: PhantomData<D>,
}

pub struct MoveToActiveReturn<D: DeserializeOwned> {
    job_data: Option<ActiveJob<D>>,
    job_id: Option<String>,
    expire: Option<Duration>,
    next_delayed: Option<DateTime<Utc>>,
    pub phantom: PhantomData<D>,
}

#[derive(Debug, Serialize)]
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
pub enum MoveToActiveResult<D> {
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

impl<'a, D> InvokeLuaScript for MoveToActive<'a, D>
where
    D: DeserializeOwned,
{
    type Return = MoveToActiveResult<ActiveJob<D>>;

    async fn call(
        self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
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

        let x: RedisResult<(JobDataOrExitCode, String, u64, i64)> = MOVE_TO_ACTIVE
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
            .arg(rmp_serde::to_vec_named(&opts).unwrap())
            .invoke_async(con)
            .await;
        let (job_data, job_key, expire_time, next_timestamp) = x?;

        let res = match (job_data, expire_time, next_timestamp) {
            (_, et, _) if et != 0 => MoveToActiveResult::Delay {
                delay: Duration::from_millis(et),
            },
            (_, _, nt) if nt != 0 => MoveToActiveResult::WaitUntil {
                timestamp: DateTime::from_timestamp_millis(nt).expect("TODO"),
            },
            (JobDataOrExitCode::JobData(hm), _, _) => MoveToActiveResult::JobData {
                id: job_key,
                data: active_job_from_hashmap::<D>(hm),
            },
            _ => MoveToActiveResult::NothingToDo,
        };
        Ok(res)
    }
}

fn active_job_from_hashmap<D: DeserializeOwned>(data: HashMap<String, String>) -> ActiveJob<D> {
    ActiveJob {
        name: data.get("name").expect("TODO").into(),
        data: data.extract("data").expect("TODO"),
        priority: data.extract_opt("priority").expect("TODO"),
        timestamp: DateTime::from_timestamp_millis(data.extract::<i64>("timestamp").expect("TODO"))
            .expect("TODO"),
        processed_on: None,
        delay: data
            .extract_opt::<i64>("delay")
            .expect("TODO")
            .map(|d| Duration::from_millis(std::cmp::max(0, d) as u64)),
        stc: None,
    }
}

impl FromRedisValue for JobDataOrExitCode {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        match v {
            redis::Value::Int(i) => Ok(Self::ExitCode(*i)),
            redis::Value::Array(m) => {
                let mut res = HashMap::new();
                for a in m.windows(2).step_by(2) {
                    assert_eq!(a.len(), 2);
                    let (key, value) = (&a[0], &a[1]);
                    if let (redis::Value::BulkString(kk), redis::Value::BulkString(vv)) =
                        (key, value)
                    {
                        res.insert(
                            String::from_utf8(kk.clone()).expect("TODO"),
                            String::from_utf8(vv.clone()).expect("TODO"),
                        );
                    } else {
                        todo!("Raise propper error");
                    }
                }
                Ok(Self::JobData(res))
            }
            s => panic!("Unexpected Redis Value: {s:?}"),
        }
    }
}
