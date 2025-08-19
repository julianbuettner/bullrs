use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct QueueName(Arc<String>);

impl QueueName {
    pub fn new(name: impl ToString) -> Self {
        Self(Arc::from(name.to_string()))
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
    pub fn active(&self) -> String {
        format!("bull:{}:active", self.0)
    }
    pub fn completed(&self) -> String {
        format!("bull:{}:completed", self.0)
    }
    pub fn delayed(&self) -> String {
        format!("bull:{}:delayed", self.0)
    }
    pub fn events(&self) -> String {
        format!("bull:{}:events", self.0)
    }
    pub fn failed(&self) -> String {
        format!("bull:{}:failed", self.0)
    }
    pub fn id(&self) -> String {
        format!("bull:{}:id", self.0)
    }
    pub fn job(&self, job_id: &str) -> String {
        format!("bull:{}:{}", self.0, job_id)
    }
    pub fn job_lock(&self, job_id: &str) -> String {
        format!("bull:{}:{}:lock", self.0, job_id)
    }
    pub fn job_logs(&self, job_id: &str) -> String {
        format!("bull:{}:{}:logs", self.0, job_id)
    }
    pub fn limiter(&self) -> String {
        format!("bull:{}:limiter", self.0)
    }
    pub fn marker(&self) -> String {
        // A sorted set containing
        // key value pairs about delayed jobs.
        // jobId: targetTimestampMs
        // It also contains a special key value pair,
        // 0: 0, if one or more jobs have been added to
        // jobs.
        format!("bull:{}:marker", self.0)
    }
    pub fn meta(&self) -> String {
        // A hashmap to contain global configuration
        // about queue, like if it is paused or rate limits.
        format!("bull:{}:meta", self.0)
    }
    pub fn paused(&self) -> String {
        format!("bull:{}:paused", self.0)
    }
    pub fn prefix(&self) -> String {
        format!("bull:{}:", self.0)
    }
    pub fn prioritized(&self) -> String {
        format!("bull:{}:prioritized", self.0)
    }
    pub fn priority_counter(&self) -> String {
        format!("bull:{}:pc", self.0)
    }
    pub fn stalled(&self) -> String {
        format!("bull:{}:stalled", self.0)
    }
    pub fn stalled_check(&self) -> String {
        format!("bull:{}:stalled-check", self.0)
    }
    pub fn wait(&self) -> String {
        // Set containing IDs of jobs,
        // ready to be picked up by a worker.
        format!("bull:{}:wait", self.0)
    }
    pub fn metrics(&self) -> String {
        format!("bull:{}:metrics", self.0)
    }
    pub fn base(&self) -> String {
        format!("bull:{}:", self.0)
    }
}
