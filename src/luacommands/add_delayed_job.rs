use chrono::Utc;
use redis::{ErrorKind, RedisError, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    JobOptions,
    luacommands::{ADD_DELAYED_JOB, InvokeLuaScript},
    queue::QueueName,
};

pub struct AddDelayedJob<'a, D> {
    pub queue: &'a QueueName,
    pub job_name: &'a str,
    pub data: &'a D,
    pub job_options: &'a JobOptions,
}

#[derive(Debug, Deserialize)]
pub enum AddDelayedJobReturn {
    JobId(String),
}

#[derive(Debug, Error)]
pub enum AddDelayedJobError {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),
    #[error("failed to serialize job payload to json: {0}")]
    SerializationFailed(#[from] serde_json::Error),
    #[error("parent key is missing")]
    MissingParentKey,
}

impl<'a, D> InvokeLuaScript for AddDelayedJob<'a, D>
where
    D: Serialize,
{
    type Result = Result<AddDelayedJobReturn, AddDelayedJobError>;

    async fn call(self, con: &mut impl redis::aio::ConnectionLike) -> Self::Result {
        let key_prefix = self.queue.prefix();
        let custom_id: &str = self.job_options.job_id.as_deref().unwrap_or("");
        let parent_key: Option<String> = None;
        let parent_dependencies_key = "";
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
            repeat_job_key,
            deduplication_key,
        );

        let payload_serialized = serde_json::to_string(self.data)?;

        let inner_result: Value = ADD_DELAYED_JOB
            .key(self.queue.marker())
            .key(self.queue.meta())
            .key(self.queue.id())
            .key(self.queue.delayed())
            .key(self.queue.completed())
            .key(self.queue.events())
            .arg(rmp_serde::to_vec(&arguments).expect("should never fails"))
            .arg(payload_serialized)
            .arg(rmp_serde::to_vec_named(self.job_options).expect("serializing never fails"))
            .invoke_async(con)
            .await?;
        match inner_result {
            Value::Int(-5) => Err(AddDelayedJobError::MissingParentKey),
            Value::BulkString(s) => Ok(AddDelayedJobReturn::JobId(
                String::from_utf8_lossy(&s).into(),
            )),
            Value::SimpleString(s) => Ok(AddDelayedJobReturn::JobId(s)),
            x => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Unexpected response from AddDelayedJob lua script",
                format!("Response was {x:?}"),
            )))?,
        }
    }
}
