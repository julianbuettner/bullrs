use redis::Value;

use crate::{
    JobOptions, SchedulerId,
    bullmq::options::WireJobOptions,
    error::AddJobSchedulerError,
    luacommands::{InvokeLuaScript, UPDATE_JOB_SCHEDULER},
    queue::QueueName,
};

pub struct UpdateJobScheduler<'a> {
    pub queue: &'a QueueName,
    pub scheduler_id: &'a SchedulerId,
    pub next_millis: i64,
    pub delayed_data_json: &'a str,
    pub delayed_opts: &'a JobOptions,
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
            .key(self.queue.repeat())
            .key(self.queue.delayed())
            .key(self.queue.wait())
            .key(self.queue.paused())
            .key(self.queue.meta())
            .key(self.queue.prioritized())
            .key(self.queue.marker())
            .key(self.queue.id())
            .key(self.queue.events())
            .key(self.queue.priority_counter())
            .key("") // KEYS[11] producer key (unused — empty string matches lua guard)
            .key(self.queue.active()) // KEYS[12]
            .arg(self.next_millis)
            .arg(self.scheduler_id.as_ref())
            .arg(self.delayed_data_json)
            .arg(
                rmp_serde::to_vec_named(&WireJobOptions::from(self.delayed_opts))
                    .expect("serializing job options should never fail"),
            )
            .arg(self.timestamp)
            .arg(self.prefix.clone())
            .arg(self.producer_id.unwrap_or(""));
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            Value::BulkString(s) => Ok(String::from_utf8_lossy(&s).into()),
            Value::SimpleString(s) => Ok(s),
            Value::Nil => Ok(String::new()),
            x => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Unexpected response from updateJobScheduler lua script",
                format!("Response was {x:?}"),
            )))?,
        }
    }
}
