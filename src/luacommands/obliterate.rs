use crate::{
    luacommands::{InvokeLuaScript, OBLITERATE},
    queue::QueueName,
};

pub struct Obliterate<'a> {
    pub queue: &'a QueueName,
}

pub enum ObliterateReturn {
    Obliterated,
    QueuePaused,
}

impl<'a> InvokeLuaScript for Obliterate<'a> {
    type Return = ();

    async fn call(
        self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        OBLITERATE.key(self.queue.meta()).key(self.queue.base());
        todo!()
    }
}
