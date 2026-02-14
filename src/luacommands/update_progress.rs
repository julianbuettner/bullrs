use redis::RedisError;

use crate::{
    ProgressPercent,
    error::UpdateProgressError,
    luacommands::{InvokeLuaScript, UPDATE_PROGRESS},
    queue::QueueName,
};

pub struct UpdateProgess<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    pub progress: ProgressPercent,
}

impl<'a> InvokeLuaScript for UpdateProgess<'a> {
    type RedisOutput = i32;
    type DomainOk = ();
    type DomainErr = UpdateProgressError;

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
            -1 => Err(UpdateProgressError::JobNotFound {
                job_id: self.job_id.into(),
                queue_name: self.queue.clone(),
            }),
            _x => panic!("Script should never return that value"),
        }
    }

    fn map_redis_error(&self, error: RedisError) -> Self::DomainErr {
        error.into()
    }
}
