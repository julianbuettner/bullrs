use serde::Serialize;

use crate::{JobOptions, Queue, QueueName};

/// A FlowJob is a temporary job, ready to be placed in a flow hierarchy.
#[allow(dead_code)]
pub struct PreparedFlowJob {
    queue_name: QueueName,
    job_name: String,
    /// For type safety, we only need to ensure, that the input data
    /// has the right type for all jobs in the queue. As flow jobs
    /// can target multiple queues, we need to maintain that invariant
    /// manually, by simply serializing now and remembering to which queue it has to go to.
    data_json: String,
    job_options: JobOptions,
}

impl<D, R> Queue<D, R>
where
    D: Serialize,
    R: std::fmt::Debug + Clone,
{
    /// WIP: Don't use yet
    pub fn flow_job(&self, job_name: &str, data: &D) -> serde_json::Result<PreparedFlowJob> {
        Ok(PreparedFlowJob {
            queue_name: self.name.clone(),
            job_name: job_name.into(),
            data_json: serde_json::to_string(data)?,
            job_options: JobOptions::default(),
        })
    }
}
