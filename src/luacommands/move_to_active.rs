use core::{marker::PhantomData, todo};
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::RedisResult;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    luacommands::{InvokeLuaScript, MOVE_TO_ACTIVE},
    queue::QueueName,
};

pub struct MoveToActive<'a, D: DeserializeOwned> {
    pub queue: &'a QueueName,
    pub worker_id: &'a str,
    pub limiter: RateLimiter,
    pub lock_duration: Duration,
    pub token: &'a str, // should be random
    pub phantom: PhantomData<D>,
}

pub struct MoveToActiveReturn<D> {
    job_data: Option<D>,
    job_id: Option<String>,
    expire: Option<Duration>,
    next_delayed: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct RateLimiter {
    pub max: usize,
    #[serde(with = "crate::milliserde::duration_millis")]
    pub duration: Duration,
}

impl<'a, D> InvokeLuaScript for MoveToActive<'a, D>
where
    D: DeserializeOwned,
{
    type Return = (Option<String>, String, String, String);
    async fn call(
        self: Self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        #[derive(Debug, Serialize)]
        struct Opts<'a> {
            token: &'a str,
            #[serde(with = "crate::milliserde::duration_millis")]
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

        let x: RedisResult<(String, String, i64, i64)> = MOVE_TO_ACTIVE
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
        let p: PhantomData<D> = PhantomData;
        dbg!(&x);
        let (job_data_str, job_key, expire_time, nextTimestamp) = x?;
        todo!()
    }
}
