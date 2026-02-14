use chrono::Utc;
use redis::{ErrorKind, RedisError, Value};
use serde::Serialize;

use crate::{
    JobOptions,
    error::AddJobErr,
    luacommands::{ADD_PRIORITIZED_JOB, InvokeLuaScript},
    queue::QueueName,
};

pub struct AddPrioritizedJob<'a, D> {
    pub queue: &'a QueueName,
    pub job_name: &'a str,
    pub data: &'a D,
    pub job_options: &'a JobOptions,
}

impl<'a, D> InvokeLuaScript for AddPrioritizedJob<'a, D>
where
    D: Serialize,
{
    type RedisOutput = Value;
    type DomainOk = String;
    type DomainErr = AddJobErr;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let key_prefix = self.queue.prefix();
        let custom_id: &str = self.job_options.job_id.as_deref().unwrap_or("");
        let parent_key: Option<String> = None;
        let wait_children_key = "";
        let parent_dependencies_key = "";
        let parent: Option<String> = None;
        let repeat_job_key = "";
        let deduplication_key = "";
        let job_name = self.job_name;
        let timestamp = self
            .job_options
            .timestamp
            .unwrap_or_else(Utc::now)
            .timestamp_millis();
        let arguments = (
            key_prefix,
            custom_id,
            job_name,
            timestamp,
            parent_key,
            parent_dependencies_key,
            parent, // {id, queueKey}
            repeat_job_key,
            deduplication_key,
        );

        let payload_serialized = serde_json::to_string(self.data)?;

        let mut invocation = ADD_PRIORITIZED_JOB.prepare_invoke();
        invocation
            .key(self.queue.marker())
            .key(self.queue.meta())
            .key(self.queue.id())
            .key(self.queue.prioritized())
            .key(self.queue.delayed())
            .key(self.queue.completed())
            .key(self.queue.active())
            .key(self.queue.events())
            .key(self.queue.priority_counter())
            .arg(rmp_serde::to_vec(&arguments).expect("should never fail"))
            .arg(payload_serialized)
            .arg(rmp_serde::to_vec_named(self.job_options).expect("serializing never fails"));
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            Value::Int(-5) => Err(AddJobErr::MissingParentKey),
            Value::BulkString(s) => Ok(String::from_utf8_lossy(&s).into()),
            Value::SimpleString(s) => Ok(s),
            x => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Unexpected response from AddPrioritizedJob lua script",
                format!("Response was {x:?}"),
            )))?,
        }
    }
}
