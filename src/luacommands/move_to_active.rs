use core::marker::PhantomData;
use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use redis::Value;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    ActiveJob, JobOptions, RateLimit, SchedulerId,
    bullmq::{options::WireJobOptions, rate_limiter::WireRateLimiter},
    error::MoveToActiveErr,
    luacommands::{InvokeLuaScript, MOVE_TO_ACTIVE},
    queue::QueueName,
    redisext::{RedisHashMapError, RedisHashMapExt},
};

/// Pull the next available job from `wait` / `prioritized` / `delayed` into `active`.
pub struct MoveToActive<'a, D: DeserializeOwned> {
    pub queue: &'a QueueName,
    pub worker_id: &'a str,
    pub limiter: RateLimit,
    pub lock_duration: Duration,
    pub token: &'a str,
    pub phantom: PhantomData<D>,
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
            limiter: WireRateLimiter,
            name: &'a str,
        }

        let opts = Opts {
            token: self.token,
            lock_duration: self.lock_duration,
            limiter: WireRateLimiter::from(&self.limiter),
            name: self.worker_id,
        };

        let now = Utc::now();

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
                    .ok_or(MoveToActiveErr::BadTimestamp { ts: nt })?,
            },
            (Value::Int(0), 0, 0) => MoveToActiveOk::NothingToDo,
            (value, 0, 0) => MoveToActiveOk::JobData {
                id: job_id,
                data: active_job_from_hashmap(job_data_map(value)?)?,
            },
            _ => panic!("Are we sure the script can't output that value combination?"),
        };
        Ok(res)
    }
}

fn active_job_from_hashmap<D: DeserializeOwned>(
    data: HashMap<String, String>,
) -> Result<ActiveJob<D>, RedisHashMapError> {
    let options = match data.get("opts") {
        Some(opts_json) => serde_json::from_str::<WireJobOptions>(opts_json)
            .ok()
            .map(JobOptions::from),
        None => None,
    };
    let scheduled_by = data
        .get("rjk")
        .cloned()
        .and_then(|s| SchedulerId::try_new(s).ok());
    Ok(ActiveJob {
        name: data.get_v("name")?,
        data: data.extract("data")?,
        priority: data.extract_opt("priority")?,
        timestamp: data.extract_timestamp_ms("timestamp")?,
        processed_on: None,
        delay: data
            .extract_opt::<i64>("delay")?
            .map(|d| Duration::from_millis(std::cmp::max(0, d) as u64)),
        stalled_count: None,
        scheduled_by,
        options,
    })
}

fn job_data_map(v: &Value) -> Result<HashMap<String, String>, MoveToActiveErr> {
    match &v {
        redis::Value::Array(m) => {
            let mut res = HashMap::new();
            let mut values_iter = m.iter();
            loop {
                let (a, b) = (values_iter.next(), values_iter.next());
                match (a, b) {
                    (None, None) => return Ok(res),
                    (Some(Value::BulkString(a)), Some(Value::BulkString(b))) => {
                        res.insert(String::from_utf8(a.clone())?, String::from_utf8(b.clone())?);
                    }
                    _ => return Err(MoveToActiveErr::UnexpectedRedisValue { value: v.clone() }),
                }
            }
        }
        _ => Err(MoveToActiveErr::UnexpectedRedisValue { value: v.clone() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Payload {
        n: i64,
    }

    fn base_hash() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("name".into(), "job-name".into());
        m.insert(
            "data".into(),
            serde_json::to_string(&Payload { n: 7 }).unwrap(),
        );
        m.insert("timestamp".into(), "1700000000000".into());
        m
    }

    #[test]
    fn parses_minimal_active_job() {
        let job: ActiveJob<Payload> = active_job_from_hashmap(base_hash()).unwrap();
        assert_eq!(job.name, "job-name");
        assert_eq!(job.data, Payload { n: 7 });
        assert_eq!(job.scheduled_by, None);
        assert!(job.options.is_none());
        assert_eq!(job.timestamp.timestamp_millis(), 1_700_000_000_000);
    }

    #[test]
    fn missing_name_is_an_error() {
        let mut h = base_hash();
        h.remove("name");
        assert!(active_job_from_hashmap::<Payload>(h).is_err());
    }

    #[test]
    fn missing_data_is_an_error() {
        let mut h = base_hash();
        h.remove("data");
        assert!(active_job_from_hashmap::<Payload>(h).is_err());
    }

    #[test]
    fn rjk_field_is_parsed_as_scheduler_id() {
        let mut h = base_hash();
        h.insert("rjk".into(), "daily-report".into());
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        assert_eq!(
            job.scheduled_by.as_ref().map(|s| s.as_ref()),
            Some("daily-report")
        );
    }

    #[test]
    fn invalid_rjk_is_silently_dropped() {
        // A scheduler id containing a colon is invalid in the domain, but the
        // parser must not fail the whole job — it just leaves `scheduled_by`
        // empty.
        let mut h = base_hash();
        h.insert("rjk".into(), "has:colon".into());
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        assert!(job.scheduled_by.is_none());
    }

    #[test]
    fn opts_field_is_parsed_into_job_options() {
        let mut h = base_hash();
        h.insert(
            "opts".into(),
            r#"{"attempts":2,"kl":5,"cpof":true,"de":"d"}"#.into(),
        );
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        let opts = job.options.expect("options parsed");
        assert_eq!(opts.attempts, Some(2));
        assert_eq!(opts.limit_logs, Some(5));
        assert_eq!(opts.continue_parent_on_failure, Some(true));
        assert_eq!(opts.deduplication.as_deref(), Some("d"));
    }

    #[test]
    fn unparseable_opts_yields_none_not_error() {
        let mut h = base_hash();
        h.insert("opts".into(), "{not valid json".into());
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        assert!(job.options.is_none());
    }

    #[test]
    fn delay_field_is_decoded_as_duration() {
        let mut h = base_hash();
        h.insert("delay".into(), "1500".into());
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        assert_eq!(job.delay, Some(Duration::from_millis(1500)));
    }

    #[test]
    fn negative_delay_is_clamped_to_zero() {
        let mut h = base_hash();
        h.insert("delay".into(), "-50".into());
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        assert_eq!(job.delay, Some(Duration::from_millis(0)));
    }

    #[test]
    fn priority_field_is_optional() {
        let job: ActiveJob<Payload> = active_job_from_hashmap(base_hash()).unwrap();
        assert_eq!(job.priority, None);
        let mut h = base_hash();
        h.insert("priority".into(), "42".into());
        let job: ActiveJob<Payload> = active_job_from_hashmap(h).unwrap();
        assert_eq!(job.priority, Some(42));
    }
}
