use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    error::MoveStalledToWaitError,
    luacommands::{InvokeLuaScript, MOVE_STALLED_JOBS_TO_WAIT},
    queue::QueueName,
};

pub struct MoveStalledJobsToWait<'a> {
    /// Name of the queue we are doing maintenance work (stalled jobs to waiting) for
    pub queue: &'a QueueName,
    /// If a job stalls this often, mark it failed
    pub max_stalled_before_failed: usize,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
    /// A worker is supposed to refresh the lease on jobs
    /// it's currently working on. If the worker crashes or the
    /// event loop is blocked, how much time is allowed to pass,
    /// before it's moved back into the ready set.
    pub max_duration: Duration,
}

impl<'a> InvokeLuaScript for MoveStalledJobsToWait<'a> {
    type RedisOutput = Vec<String>;
    type DomainOk = Vec<String>;
    type DomainErr = MoveStalledToWaitError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = MOVE_STALLED_JOBS_TO_WAIT.prepare_invoke();
        invocation
            .key(self.queue.stalled())
            .key(self.queue.wait())
            .key(self.queue.active())
            .key(self.queue.stalled_check())
            .key(self.queue.meta())
            .key(self.queue.paused())
            .key(self.queue.marker())
            .key(self.queue.events())
            .arg(self.max_stalled_before_failed)
            .arg(self.queue.prefix())
            .arg(self.timestamp.timestamp_millis())
            .arg(self.max_duration.as_millis() as u64);
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        Ok(value)
    }
}
