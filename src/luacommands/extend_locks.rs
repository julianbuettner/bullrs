use std::time::Duration;

use redis::Value;

use crate::{
    error::BasicRedisError,
    luacommands::{EXTEND_LOCKS, InvokeLuaScript},
    queue::QueueName,
};

pub struct ExtendLocks<'a> {
    pub queue: &'a QueueName,
    pub job_ids: &'a [String],
    pub tokens: &'a [String],
    pub lock_duration: Duration,
}

impl<'a> InvokeLuaScript for ExtendLocks<'a> {
    type RedisOutput = Value;
    type DomainOk = Vec<String>;
    type DomainErr = BasicRedisError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invoc = EXTEND_LOCKS.prepare_invoke();
        invoc
            .key(self.queue.stalled())
            .arg(self.queue.base())
            .arg(rmp_serde::to_vec(self.tokens).expect("serializing tokens should never fail"))
            .arg(rmp_serde::to_vec(self.job_ids).expect("serializing job_ids should never fail"))
            .arg(self.lock_duration.as_millis() as usize);
        Ok(invoc)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            Value::Array(items) => {
                let failed: Vec<String> = items
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::BulkString(bytes) => String::from_utf8(bytes).ok(),
                        Value::SimpleString(s) => Some(s),
                        _ => None,
                    })
                    .collect();
                Ok(failed)
            }
            Value::Nil => Ok(Vec::new()),
            _ => Ok(Vec::new()),
        }
    }
}
