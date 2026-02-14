mod basics;
mod flowjob;
mod getset;
mod name;

use std::marker::PhantomData;

use deadpool_redis::Pool;

pub use flowjob::PreparedFlowJob;
pub use name::{InvalidQueueName, QueueName};

/**
Represents a queue of jobs of the same kind.

It contains jobs in multiple sets (technically multiple different data structures).
- **Waiting**: jobs to be picked up by a worker. Jobs are ordered, but you can push
  jobs to the front.
- **Active**: jobs being actively worked
- **Completed**: jobs that succeeded and have a return value
- **Delayed**: jobs that are scheduled for a later point in time. This might include
  jobs that failed and are scheduled for retrying. Once the time is reached, they
  are moved to waiting.
- **Failed**: once jobs have exceeded their configures retry count, they will be put here.
- **Prioritized**: jobs with assigned priority, but a job with a priority has always lower priority
  than a job without priority (waiting set).
- **Waiting children**: those jobs are waiting, until all their children are completed.
- **Paused**: this is a special set, where all jobs are put, if the queue is paused.

A queue has two generics. `D` is the type of the Data or Payload and
has to implement [serde::Serialize] to be enqueuable.
`R` is the type of the Result, which has to implement [serde::Deserialize]
to be retrieved.
*/
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
