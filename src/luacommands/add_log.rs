use redis::{ErrorKind, RedisError, RedisResult};
use thiserror::Error;

use crate::{
    luacommands::{InvokeLuaScript, ADD_LOG},
    queue::QueueName,
};

pub struct AddLog<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    pub log_line: &'a str,
    pub keep_logs: Option<usize>,
}

pub struct AddLogOut {
    new_count: usize,
}

#[derive(Debug, Error)]
pub enum AddLogError {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),
    #[error("job \"{job_id}\" in queue \"{}\" doesn't exist (anymore)", queue_name.as_str())]
    JobNotFound {
        job_id: String,
        queue_name: QueueName,
    },
    #[error("bullmq protocol error, expected return value -1 or positive, got {0}.")]
    UnexpectedValue(i64),
}

impl<'a> InvokeLuaScript for AddLog<'a> {
    type RedisOutput = i64;
    type DomainOutput = AddLogOut;
    type DomainError = AddLogError;

    fn generate_invocation(&self) -> redis::ScriptInvocation<'static> {
        let keep_logs = self.keep_logs.map(|v| v.to_string()).unwrap_or_default();
        let mut invocation = ADD_LOG.prepare_invoke();
        invocation
            .key(self.queue.job(self.job_id))
            .key(self.queue.job_logs(self.job_id))
            .arg(self.job_id)
            .arg(self.log_line)
            .arg(keep_logs);
        invocation
    }

    fn map_redis_error(&self, input: RedisError) -> Self::DomainError {
        input.into()
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOutput, Self::DomainError> {
        match value {
            0..=i64::MAX => Ok(AddLogOut {
                new_count: value as usize,
            }),
            -1 => Err(AddLogError::JobNotFound {
                job_id: self.job_id.into(),
                queue_name: self.queue.clone(),
            }),
            i64::MIN..-1 => Err(AddLogError::UnexpectedValue(value)),
        }
    }
}
