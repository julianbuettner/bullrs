use redis::AsyncCommands;

use crate::{error::BasicRedisError, queue::Queue};

impl<D, R: std::fmt::Debug + Clone> Queue<D, R> {
    pub async fn get_global_concurrency(&mut self) -> Result<Option<usize>, BasicRedisError> {
        Ok(self
            .pool
            .get()
            .await?
            .hget(self.name.meta(), "concurrency")
            .await?)
    }
}
