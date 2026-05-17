use redis::RedisError;

use crate::{
    error::RemoveJobError,
    luacommands::{InvokeLuaScript, REMOVE_JOB},
    queue::QueueName,
};

/// Remove a single job from any state it may be in.
///
/// An active job that is currently held by a worker cannot be removed.
pub struct RemoveJob<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    /// When `true`, child jobs are recursively removed as well.
    pub remove_children: bool,
}

impl<'a> InvokeLuaScript for RemoveJob<'a> {
    type RedisOutput = i32;
    type DomainOk = ();
    type DomainErr = RemoveJobError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invoc = REMOVE_JOB.prepare_invoke();
        invoc
            .key(self.queue.job(self.job_id)) // KEYS[1]: jobKey
            .key(self.queue.repeat()) // KEYS[2]: repeat key
            .arg(self.job_id) // ARGV[1]: jobId
            .arg(if self.remove_children { "1" } else { "0" }) // ARGV[2]: removeChildren
            .arg(self.queue.prefix()); // ARGV[3]: queue prefix
        Ok(invoc)
    }

    fn map_redis_error(&self, error: RedisError) -> Self::DomainErr {
        error.into()
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            1 => Ok(()),
            0 => Err(RemoveJobError::JobLocked),
            -8 => Err(RemoveJobError::IsSchedulerJob),
            x => panic!("removeJob script returned unexpected value: {x}"),
        }
    }
}
