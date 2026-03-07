mod basics;
mod flowjob;
mod getset;
mod name;

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use deadpool_redis::Pool;
use serde::de::DeserializeOwned;

use crate::event_system::EventSystem;

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
pub struct Queue<D, R: Debug + Clone> {
    name: QueueName,
    pool: Pool,
    event_system: Arc<EventSystem<R>>,
    phantom: PhantomData<D>,
}

impl<D, R: Debug + Clone> Clone for Queue<D, R> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            pool: self.pool.clone(),
            event_system: self.event_system.clone(),
            phantom: PhantomData,
        }
    }
}

impl<D, R> Queue<D, R>
where
    R: Debug + Clone + Send + DeserializeOwned + 'static,
{
    /// Construct a Queue from a [deadpool_redis::Pool] and a [crate::QueueName].
    pub fn new(pool: Pool, name: QueueName) -> Self {
        let event_system = Arc::new(EventSystem::new(pool.clone(), name.clone()));
        Self {
            name,
            pool,
            event_system,
            phantom: PhantomData,
        }
    }
    /// Access the name of the queue.
    pub fn name(&self) -> &QueueName {
        &self.name
    }
}
