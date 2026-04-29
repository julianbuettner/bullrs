use crate::{
    SchedulerId,
    error::RemoveJobSchedulerError,
    luacommands::{InvokeLuaScript, REMOVE_JOB_SCHEDULER},
    queue::QueueName,
};

pub struct RemoveJobScheduler<'a> {
    pub queue: &'a QueueName,
    pub scheduler_id: &'a SchedulerId,
}

impl<'a> InvokeLuaScript for RemoveJobScheduler<'a> {
    type RedisOutput = i32;
    type DomainOk = ();
    type DomainErr = RemoveJobSchedulerError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = REMOVE_JOB_SCHEDULER.prepare_invoke();
        invocation
            .key(self.queue.repeat())
            .key(self.queue.delayed())
            .key(self.queue.events())
            .arg(self.scheduler_id.as_ref())
            .arg(self.queue.prefix());
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
