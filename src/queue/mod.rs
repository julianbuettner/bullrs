mod basics;
mod getset;
mod name;

use std::marker::PhantomData;

use deadpool_redis::Pool;

pub use name::{QueueName, InvalidQueueName};

/// Represents a single queue of jobs of the same format.
///
/// A queue has two generics. `D` is the type of the Data or Payload and
/// has to implement [serde::Serialize] to be enqueuable.
/// `R` is the type of the Result, which has to implement [serde::Deserialize]
/// to be retrieved.
#[derive(Clone)]
pub struct Queue<D, R> {
    name: QueueName,
    pool: Pool,
    phantom: PhantomData<(D, R)>, // Data, Result
}

impl<D, R> Queue<D, R> {
    /// Construct a Queue from a [deadpool_redis::Pool] and a [crate::QueueName].
    pub fn new(pool: Pool, name: QueueName) -> Self {
        Self {
            name,
            pool,
            phantom: PhantomData,
        }
    }
    /// Access the name of the queue.
    pub fn name(&self) -> &QueueName {
        &self.name
    }
}
