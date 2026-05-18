use redis::RedisError;

use crate::{
    error::PauseResumeError,
    luacommands::{InvokeLuaScript, PAUSE},
    queue::QueueName,
};

pub enum PauseAction {
    Pause,
    Resume,
}

pub struct Pause<'a> {
    pub queue: &'a QueueName,
    pub action: PauseAction,
}

impl<'a> InvokeLuaScript for Pause<'a> {
    type DomainOk = ();
    type DomainErr = PauseResumeError;
    type RedisOutput = ();

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let mut invocation = PAUSE.prepare_invoke();
        invocation
            .key(match self.action {
                PauseAction::Pause => self.queue.wait(),
                PauseAction::Resume => self.queue.paused(),
            })
            // Set to move jobs to
            .key(match self.action {
                PauseAction::Pause => self.queue.paused(),
                PauseAction::Resume => self.queue.wait(),
            })
            .key(self.queue.meta())
            .key(self.queue.prioritized())
            .key(self.queue.events())
            .key(self.queue.delayed())
            .key(self.queue.marker())
            .arg(match self.action {
                PauseAction::Pause => "paused",
                PauseAction::Resume => "resumed",
            });
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        Ok(value)
    }

    fn map_redis_error(&self, error: RedisError) -> Self::DomainErr {
        error.into()
    }
}
