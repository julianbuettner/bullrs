use crate::{
    error::RemoveJobSchedulerError,
    luacommands::{InvokeLuaScript, REMOVE_JOB_SCHEDULER},
    queue::QueueName,
};

pub struct RemoveJobScheduler<'a> {
    pub queue: &'a QueueName,
    pub job_scheduler_id: &'a str,
}

impl<'a> InvokeLuaScript for RemoveJobScheduler<'a> {
    type RedisOutput = i32;
    type DomainOk = ();
    type DomainErr = RemoveJobSchedulerError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = REMOVE_JOB_SCHEDULER.prepare_invoke();
        invocation
            .key(self.queue.repeat())  // KEYS[1]
            .key(self.queue.delayed()) // KEYS[2]
            .key(self.queue.events())  // KEYS[3]
            .arg(self.job_scheduler_id)  // ARGV[1]
            .arg(self.queue.prefix());   // ARGV[2]
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            0 => Ok(()),
            1 => Err(RemoveJobSchedulerError::NotFound),
            x => panic!("removeJobScheduler script returned unexpected value: {x}"),
        }
    }
}
