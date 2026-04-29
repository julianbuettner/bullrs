use redis::Value;

use crate::{
    error::AddJobSchedulerError,
    luacommands::{InvokeLuaScript, UPDATE_JOB_SCHEDULER},
    queue::QueueName,
};

pub struct UpdateJobScheduler<'a> {
    pub queue: &'a QueueName,
    pub scheduler_id: &'a str,
    pub next_millis: i64,
    pub delayed_data_json: &'a str,
    pub delayed_opts: &'a crate::JobOptions,
    pub timestamp: i64,
    pub prefix: String,
    pub producer_id: Option<&'a str>,
}

impl<'a> InvokeLuaScript for UpdateJobScheduler<'a> {
    type RedisOutput = Value;
    type DomainOk = String;
    type DomainErr = AddJobSchedulerError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = UPDATE_JOB_SCHEDULER.prepare_invoke();
        invocation
            .key(self.queue.repeat()) // KEYS[1]
            .key(self.queue.delayed()) // KEYS[2]
            .key(self.queue.wait()) // KEYS[3]
            .key(self.queue.paused()) // KEYS[4]
            .key(self.queue.meta()) // KEYS[5]
            .key(self.queue.prioritized()) // KEYS[6]
            .key(self.queue.marker()) // KEYS[7]
            .key(self.queue.id()) // KEYS[8]
            .key(self.queue.events()) // KEYS[9]
            .key(self.queue.priority_counter()) // KEYS[10]
            .key(self.queue.active()) // KEYS[12]
            .arg(self.next_millis) // ARGV[1]
            .arg(self.scheduler_id) // ARGV[2]
            .arg(self.delayed_data_json) // ARGV[3]
            .arg(
                rmp_serde::to_vec_named(self.delayed_opts)
                    .expect("serializing job options should never fail"),
            ) // ARGV[4]
            .arg(self.timestamp) // ARGV[5]
            .arg(self.prefix.clone()) // ARGV[6]
            .arg(self.producer_id.unwrap_or("")); // ARGV[7]
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            Value::BulkString(s) => Ok(String::from_utf8_lossy(&s).into()),
            Value::SimpleString(s) => Ok(s),
            // Script returns nil when scheduler doesn't exist — that's fine, swallow it.
            Value::Nil => Ok(String::new()),
            x => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Unexpected response from updateJobScheduler lua script",
                format!("Response was {x:?}"),
            )))?,
        }
    }
}
