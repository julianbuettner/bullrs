use redis::RedisError;

use crate::{
    luacommands::{ADD_LOG, InvokeLuaScript},
    queue::{Queue, QueueName},
};

pub struct AddLog<'a> {
    pub queue: &'a QueueName,
    pub job_id: &'a str,
    pub log_line: &'a str,
    pub keep_logs: Option<usize>,
}

impl<'a> InvokeLuaScript for AddLog<'a> {
    type Return = u64;

    async fn call(
        self: Self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        let keep_logs = self.keep_logs.map(|v| v.to_string()).unwrap_or_default();

        let v: i64 = ADD_LOG
            .key(self.queue.job(self.job_id))
            .key(self.queue.job_logs(self.job_id))
            .arg(self.job_id)
            .arg(self.log_line)
            .arg(keep_logs)
            .invoke_async(con)
            .await?;
        match v {
            0..=i64::MAX => Ok(v as u64),
            -1 => Err(RedisError::from((
                redis::ErrorKind::ResponseError,
                "Could not append log to job as job was not found anymore.",
                format!("Job {} in queue {}", self.job_id, self.queue.as_str()),
            ))),
            i64::MIN..-1 => Err(RedisError::from((
                redis::ErrorKind::ResponseError,
                "Expected return value of -1 or positive from addLog call.",
                format!(
                    "Value was {}, job {} in queue {}",
                    v,
                    self.job_id,
                    self.queue.as_str()
                ),
            ))),
        }
    }
}
