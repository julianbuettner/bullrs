mod basics;
mod getset;
mod name;

use std::{fmt::Debug, marker::PhantomData};

use deadpool_redis::Pool;

pub use name::QueueName;

/// Performance note: cloning the queue is
/// cheap, not performing heap allocations.
#[derive(Clone)]
pub struct Queue<D, R> {
    name: QueueName,
    pool: Pool,
    phantom: PhantomData<(D, R)>, // Data, Result
}

impl<D, R> Queue<D, R> {
    pub fn new(pool: Pool, name: impl ToString) -> Self {
        Self {
            name: QueueName::new(name),
            pool,
            phantom: PhantomData,
        }
    }
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}
