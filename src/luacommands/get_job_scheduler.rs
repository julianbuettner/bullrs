use std::{str::FromStr, time::Duration};

use chrono::DateTime;
use chrono_tz::Tz;
use croner::Cron;
use redis::Value;

use crate::{
    Repeat, SchedulerId, SchedulerInfo, SchedulerWindow,
    error::JobSchedulerError,
    luacommands::{GET_JOB_SCHEDULER, InvokeLuaScript},
    queue::QueueName,
};

pub struct GetJobScheduler<'a> {
    pub queue: &'a QueueName,
    pub scheduler_id: &'a SchedulerId,
}

impl<'a> InvokeLuaScript for GetJobScheduler<'a> {
    type RedisOutput = Value;
    type DomainOk = Option<SchedulerInfo>;
    type DomainErr = JobSchedulerError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = GET_JOB_SCHEDULER.prepare_invoke();
        invocation
            .key(self.queue.repeat())
            .arg(self.scheduler_id.as_ref());
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        let Value::Array(mut parts) = value else {
            return Ok(None);
        };
        if parts.len() != 2 {
            return Ok(None);
        }
        let score = match parts.pop().unwrap() {
            Value::Int(s) => s,
            Value::BulkString(s) => String::from_utf8_lossy(&s).parse().unwrap_or(0),
            _ => return Ok(None),
        };
        let fields_val = parts.pop().unwrap();
        let Value::Array(items) = fields_val else {
            return Ok(None);
        };

        let mut name = None;
        let mut tz_str: Option<String> = None;
        let mut pattern = None;
        let mut every: Option<u64> = None;
        let mut offset: Option<i64> = None;
        let mut start_date: Option<i64> = None;
        let mut end_date: Option<i64> = None;
        let mut limit: Option<u64> = None;
        let mut iteration_count: Option<u64> = None;

        let mut it = items.into_iter();
        while let (Some(Value::BulkString(k)), Some(Value::BulkString(v))) = (it.next(), it.next())
        {
            let k = String::from_utf8_lossy(&k).into_owned();
            let v = String::from_utf8_lossy(&v).into_owned();
            match k.as_str() {
                "name" => name = Some(v),
                "tz" => tz_str = Some(v),
                "pattern" => {
                    pattern = Some(Cron::from_str(&v).map_err(|e| Self::DomainErr::CronError {
                        error: e,
                        pattern: v.to_string(),
                    })?)
                }
                "every" => every = v.parse().ok(),
                "offset" => offset = v.parse().ok(),
                "startDate" => start_date = v.parse().ok(),
                "endDate" => end_date = v.parse().ok(),
                "limit" => limit = v.parse().ok(),
                "ic" => iteration_count = v.parse().ok(),
                _ => {}
            }
        }

        let tz = tz_str.as_deref().and_then(|s| s.parse::<Tz>().ok());
        let repeat = match (every, pattern) {
            (Some(ms), _) => Some(Repeat::Every {
                interval: Duration::from_millis(ms),
                offset: offset.map(|o| Duration::from_millis(o.max(0) as u64)),
            }),
            (None, Some(p)) => Some(Repeat::Cron { pattern: p, tz }),
            (None, None) => None,
        };
        let window = SchedulerWindow {
            start: start_date.and_then(DateTime::from_timestamp_millis),
            end: end_date.and_then(DateTime::from_timestamp_millis),
            limit,
            immediately: None,
        };

        let next_fire = DateTime::from_timestamp_millis(score).unwrap_or_default();
        Ok(Some(SchedulerInfo {
            id: self.scheduler_id.clone(),
            name,
            repeat,
            window,
            iteration_count,
            next_fire,
        }))
    }
}
