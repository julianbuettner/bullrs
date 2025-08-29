use deadpool_redis::Pool;
use std::marker::PhantomData;

use crate::queue::QueueName;

/// Can be obtained by enqueuing a job and can be used for awaiting
/// it's result or changing it's parameters and data as long as it is not yet processed.
pub struct JobJoinHandle<D, R> {
    queue_name: QueueName,
    pool: Pool,
    id: String,
    phantom: PhantomData<(D, R)>,
}

impl<D, R> JobJoinHandle<D, R> {
    // pub async fn change_delay(&self, duration: Duration) {}
}
