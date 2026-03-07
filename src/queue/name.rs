use std::sync::Arc;
use thiserror::Error;

/// There are multiple reasons why a queue name is invalid.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum InvalidQueueName {
    /// Queue name contained colon, which is forbidden, as it
    /// is handled specially by Redis.
    #[error("queue name can't contain a colon, as that's a separator used by redis")]
    Colon,
    /// A queue name can't be empty and I think you know that.
    #[error("queue name can't be empty")]
    Empty,
}

/// A validated, cheaply cloneable queue name. Colons and empty strings are rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueName(Arc<str>);

impl QueueName {
    /// Name of a queue. Will be trimmed and then has to be non-empty
    /// and not contain colons.
    ///
    /// ```
    /// use bullrs::{QueueName, InvalidQueueName};
    ///
    /// let q = QueueName::new("   Send Email ").unwrap();
    /// assert_eq!(q.as_str(), "Send Email");
    ///
    /// assert_eq!(QueueName::new("not:allowed"), Err(InvalidQueueName::Colon));
    /// ```
    pub fn new(name: impl ToString) -> Result<Self, InvalidQueueName> {
        let name = name.to_string().trim().to_string();
        if name.contains(":") {
            return Err(InvalidQueueName::Colon);
        }
        if name.is_empty() {
            return Err(InvalidQueueName::Empty);
        }
        Ok(Self(Arc::from(name)))
    }
    /// Dereference the name as a &str.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub(crate) fn active(&self) -> String {
        format!("bull:{}:active", self.0)
    }
    pub(crate) fn completed(&self) -> String {
        format!("bull:{}:completed", self.0)
    }
    pub(crate) fn delayed(&self) -> String {
        format!("bull:{}:delayed", self.0)
    }
    pub(crate) fn events(&self) -> String {
        format!("bull:{}:events", self.0)
    }
    pub(crate) fn failed(&self) -> String {
        format!("bull:{}:failed", self.0)
    }
    pub(crate) fn id(&self) -> String {
        format!("bull:{}:id", self.0)
    }
    pub(crate) fn job(&self, job_id: &str) -> String {
        format!("bull:{}:{}", self.0, job_id)
    }
    pub(crate) fn job_lock(&self, job_id: &str) -> String {
        format!("bull:{}:{}:lock", self.0, job_id)
    }
    pub(crate) fn job_logs(&self, job_id: &str) -> String {
        format!("bull:{}:{}:logs", self.0, job_id)
    }
    pub(crate) fn limiter(&self) -> String {
        format!("bull:{}:limiter", self.0)
    }
    pub(crate) fn marker(&self) -> String {
        // A sorted set containing
        // key value pairs about delayed jobs.
        // jobId: targetTimestampMs
        // It also contains a special key value pair,
        // 0: 0, if one or more jobs have been added to
        // jobs.
        format!("bull:{}:marker", self.0)
    }
    pub(crate) fn meta(&self) -> String {
        // A hashmap to contain global configuration
        // about queue, like if it is paused or rate limits.
        format!("bull:{}:meta", self.0)
    }
    pub(crate) fn paused(&self) -> String {
        format!("bull:{}:paused", self.0)
    }
    pub(crate) fn prefix(&self) -> String {
        format!("bull:{}:", self.0)
    }
    pub(crate) fn prioritized(&self) -> String {
        format!("bull:{}:prioritized", self.0)
    }
    pub(crate) fn priority_counter(&self) -> String {
        format!("bull:{}:pc", self.0)
    }
    pub(crate) fn stalled(&self) -> String {
        format!("bull:{}:stalled", self.0)
    }
    pub(crate) fn stalled_check(&self) -> String {
        format!("bull:{}:stalled-check", self.0)
    }
    pub(crate) fn wait(&self) -> String {
        // Set containing IDs of jobs,
        // ready to be picked up by a worker.
        format!("bull:{}:wait", self.0)
    }
    pub(crate) fn metrics(&self) -> String {
        format!("bull:{}:metrics", self.0)
    }
    pub(crate) fn base(&self) -> String {
        format!("bull:{}:", self.0)
    }
}
