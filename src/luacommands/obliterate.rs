use redis::{ErrorKind, RedisError};

use crate::{
    luacommands::{InvokeLuaScript, OBLITERATE},
    queue::QueueName,
};

pub struct Obliterate<'a> {
    pub queue: &'a QueueName,
    pub batch_size: usize,
    pub force: bool,
}

pub enum ObliterateReturn {
    Progress,
    Obliterated,
    /// Use force to overwrite
    ActiveJobs,
    NotPaused,
}

impl<'a> InvokeLuaScript for Obliterate<'a> {
    type Return = ObliterateReturn;

    async fn call(
        self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        let exit_code: i32 = OBLITERATE
            .key(self.queue.meta())
            .key(self.queue.base())
            .arg(self.batch_size)
            .arg(if self.force { "force" } else { "" })
            .invoke_async(con)
            .await?;
        match exit_code {
            0 => Ok(ObliterateReturn::Obliterated),
            1 => Ok(ObliterateReturn::Progress),
            -1 => Ok(ObliterateReturn::NotPaused),
            -2 => Ok(ObliterateReturn::ActiveJobs),
            x => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Unexpected return code from obliterate job",
                format!("Exit code {x}"),
            ))),
        }
    }
}
