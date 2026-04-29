use redis::Value;

use crate::{
    error::BasicRedisError,
    luacommands::{InvokeLuaScript, GET_JOB_SCHEDULER},
    queue::QueueName,
};

pub struct GetJobScheduler<'a> {
    pub queue: &'a QueueName,
    pub scheduler_id: &'a str,
}

pub struct GetJobSchedulerOk {
    /// Raw hash fields from the scheduler hash.
    pub fields: Vec<(String, String)>,
    /// Score (next millis) from the repeat zset.
    pub next_millis: i64,
}

impl<'a> InvokeLuaScript for GetJobScheduler<'a> {
    type RedisOutput = Value;
    type DomainOk = Option<GetJobSchedulerOk>;
    type DomainErr = BasicRedisError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = GET_JOB_SCHEDULER.prepare_invoke();
        invocation
            .key(self.queue.repeat()) // KEYS[1]
            .arg(self.scheduler_id); // ARGV[1]
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            Value::Array(mut parts) if parts.len() == 2 => {
                let score = match parts.pop().unwrap() {
                    Value::Int(s) => s,
                    Value::BulkString(s) => String::from_utf8_lossy(&s).parse().unwrap_or(0),
                    _ => return Ok(None),
                };
                let fields_val = parts.pop().unwrap();
                match fields_val {
                    Value::Array(items) => {
                        let mut fields = Vec::new();
                        let mut it = items.into_iter();
                        while let (Some(Value::BulkString(k)), Some(Value::BulkString(v))) =
                            (it.next(), it.next())
                        {
                            fields.push((
                                String::from_utf8_lossy(&k).into_owned(),
                                String::from_utf8_lossy(&v).into_owned(),
                            ));
                        }
                        Ok(Some(GetJobSchedulerOk { fields, next_millis: score }))
                    }
                    Value::Nil => Ok(None),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}
