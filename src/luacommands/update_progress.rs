use redis::{ErrorKind, RedisError};

use crate::{
    ProgressPercent,
    luacommands::{InvokeLuaScript, UPDATE_PROGRESS},
    queue::QueueName,
};

pub struct UpdateProgess<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    pub progress: ProgressPercent,
}

impl<'a> InvokeLuaScript for UpdateProgess<'a> {
    type Return = ();

    async fn call(
        self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        let v: i32 = UPDATE_PROGRESS
            .key(self.queue.job(self.job_id))
            .key(self.queue.events())
            .key(self.queue.meta())
            .arg(self.job_id)
            .arg(self.progress.into_inner())
            .invoke_async(con)
            .await?;
        match v {
            0 => Ok(()),
            -1 => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Could not find job to set progress for",
                format!("Job {} in queue {}", self.job_id, self.queue.as_str()),
            ))),
            _ => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Unexpected exit code from progress setting script",
                format!("Job {} in queue {}", self.job_id, self.queue.as_str()),
            ))),
        }
    }
}
