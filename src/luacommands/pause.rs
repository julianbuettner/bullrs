use crate::{
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
    type Return = ();

    async fn call(
        self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        PAUSE
            // Set to pull jobs from
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
            })
            .invoke_async(con)
            .await
    }
}
