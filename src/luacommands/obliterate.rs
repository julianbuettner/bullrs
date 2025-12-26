use redis::RedisError;

use crate::{
    error::ObliterateError,
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

impl<'a> InvokeLuaScript for Obliterate<'a> {
    type DomainOk = ObliterateOk;
    type DomainErr = ObliterateError;
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
            -1 => Err(ObliterateError::NotPaused),
            -2 => Err(ObliterateError::ActiveJobs),
            x => panic!("Lua script should never return: {x:#?}"),
        }
    }
}
