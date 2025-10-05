use core::error;

use redis::{ErrorKind, RedisError};
use thiserror::Error;

use crate::{
    luacommands::{InvokeLuaScript, OBLITERATE},
    queue::QueueName,
};

pub struct Obliterate<'a> {
    pub queue: &'a QueueName,
    pub batch_size: usize,
    pub force: bool,
}

pub enum ObliterateOk {
    /// A batch of jobs have been purged
    Progress,
    /// The last batch has been purged, the queue is obliterated
    Obliterated,
}

#[derive(Debug, Error)]
pub enum ObliterateErr {
    /// Jobs are active, use force to overwrite anyways
    #[error("queue can only be obliterated after all jobs are done")]
    ActiveJobs,
    /// Queue is not paused
    #[error("queue can only be obliterated if paused")]
    NotPaused,
    /// Lua script returned unexpected exit code
    #[error("unexpected lua script return value: {0}")]
    UnexpectedLuaExitCode(i32),
    /// Some error occured in the Redis protocol
    #[error("something went wrong with redis: {0:?}")]
    RedisError(#[from] redis::RedisError),
}

impl<'a> InvokeLuaScript for Obliterate<'a> {
    type DomainOk = ObliterateOk;
    type DomainErr = ObliterateErr;
    type RedisOutput = i32;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invoc = OBLITERATE.prepare_invoke();
        invoc
            .key(self.queue.meta())
            .key(self.queue.base())
            .arg(self.batch_size)
            .arg(if self.force { "force" } else { "" });
        Ok(invoc)
    }

    fn map_redis_error(&self, error: RedisError) -> Self::DomainErr {
        error.into()
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            0 => Ok(ObliterateOk::Obliterated),
            1 => Ok(ObliterateOk::Progress),
            -1 => Err(ObliterateErr::NotPaused),
            -2 => Err(ObliterateErr::ActiveJobs),
            x => Err(ObliterateErr::UnexpectedLuaExitCode(x)),
        }
    }
}
