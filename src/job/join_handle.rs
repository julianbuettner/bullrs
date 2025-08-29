use chrono::Duration;
use deadpool_redis::Pool;
use std::marker::PhantomData;

use crate::queue::QueueName;

pub struct JobJoinHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    phantom: PhantomData<(D, R)>,
}

impl<D, R> JobJoinHandle<D, R> {
    pub async fn change_delay(&self, duration: Duration) {}
}
