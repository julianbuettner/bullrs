use anyhow::Error;
use redis::{ErrorKind, RedisError};
use thiserror::Error;

use crate::{
    luacommands::{InvokeLuaScript, UPDATE_PROGRESS},
    queue::QueueName,
    ProgressPercent,
};

pub struct UpdateProgess<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    pub progress: ProgressPercent,
}

#[derive(Debug, Error)]
pub enum UpdateProgressErr {
    /// Could not find job in queue
    #[error("could not find job in queue")]
    JobNotFound,
    /// Lua script returned unexpected exit code
    #[error("unexpected lua script return value: {0}")]
    UnexpectedLuaExitCode(i32),
    /// Some error occured in the Redis protocol
    #[error("something went wrong with redis: {0:?}")]
    RedisError(#[from] redis::RedisError),
}

impl<'a> InvokeLuaScript for UpdateProgess<'a> {
    type RedisOutput = i32;
    type DomainOk = ();
    type DomainErr = UpdateProgressErr;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invoc = UPDATE_PROGRESS.prepare_invoke();
        invoc
            .key(self.queue.job(self.job_id))
            .key(self.queue.events())
            .key(self.queue.meta())
            .arg(self.job_id)
            .arg(self.progress.into_inner());
        Ok(invoc)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            0 => Ok(()),
            -1 => Err(UpdateProgressErr::JobNotFound),
            x => Err(UpdateProgressErr::UnexpectedLuaExitCode(x)),
        }
    }

    fn map_redis_error(&self, error: RedisError) -> Self::DomainErr {
        error.into()
    }
}
