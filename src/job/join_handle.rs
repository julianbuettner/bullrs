use deadpool_redis::Pool;
use std::marker::PhantomData;

use crate::queue::QueueName;

/// This will be returned when enqueing a job.
/// It can be used for awaiting it's return value, chaging it's parameters
/// (at least before being picked up by a worker) or even to cancel the job.
pub struct JobJoinHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    phantom: PhantomData<(D, R)>,
}

impl<D, R> JobJoinHandle<D, R> {
    // pub async fn change_delay(&self, duration: Duration) {}
}
