use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{JobOptions, SchedulerId};

/// A job picked up from a queue, ready to be processed by a worker.
#[derive(Debug)]
pub struct ActiveJob<D> {
    /// Job name (the template name set at enqueue time).
    pub name: String,
    /// Decoded job payload.
    pub data: D,
    /// Priority assigned at enqueue time (`None` ≡ highest priority).
    pub priority: Option<usize>,
    /// Time the job was created.
    pub timestamp: DateTime<Utc>,
    /// Time the job was first moved to active, if known.
    pub processed_on: Option<DateTime<Utc>>,
    /// Configured delay, if any.
    pub delay: Option<Duration>,
    /// Stalled-counter (incremented when the worker fails to refresh the lock).
    pub stalled_count: Option<usize>,
    /// Scheduler that produced this job, if any.
    pub scheduled_by: Option<SchedulerId>,
    /// Job options as stored on the job hash, when present.
    pub options: Option<JobOptions>,
}
