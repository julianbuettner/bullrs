use chrono::Utc;
use redis::{ErrorKind, RedisError, Value};
use serde::{de::Error, Deserialize, Serialize};
use thiserror::Error;

use crate::{
    luacommands::{InvokeLuaScript, ADD_PRIORITIZED_JOB},
    queue::QueueName,
    JobOptions,
};

pub struct AddPrioritizedJob<'a, D> {
    pub queue: &'a QueueName,
    pub job_name: &'a str,
    pub data: &'a D,
    pub job_options: &'a JobOptions,
}

#[derive(Debug, Deserialize)]
pub enum AddPrioritizedJobOk {
    JobId(String),
}

#[derive(Debug, Error)]
pub enum AddPrioritizedJobErr {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),
    #[error("failed to serialize job payload to json: {0}")]
    SerializationFailed(#[from] serde_json::Error),
    #[error("parent key is missing")]
    MissingParentKey,
}

impl<'a, D> InvokeLuaScript for AddPrioritizedJob<'a, D>
where
    D: Serialize,
{
    type RedisOutput = Value;
    type DomainOk = AddPrioritizedJobOk;
    type DomainErr = AddPrioritizedJobErr;

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
            wait_children_key,
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
            Value::Int(-5) => Err(AddPrioritizedJobErr::MissingParentKey),
            Value::BulkString(s) => Ok(AddPrioritizedJobOk::JobId(
                String::from_utf8_lossy(&s).into(),
            )),
            Value::SimpleString(s) => Ok(AddPrioritizedJobOk::JobId(s)),
            x => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Unexpected response from AddPrioritizedJob lua script",
                format!("Response was {x:?}"),
            )))?,
        }
    }
}
