use redis::AsyncCommands;

use crate::queue::Queue;

impl<D, R> Queue<D, R> {
    pub async fn get_global_concurrency(&mut self) -> anyhow::Result<Option<usize>> {
        Ok(self
            .pool
            .get()
            .await?
            .hget(self.name.meta(), "concurrency")
            .await?)
    }
}
