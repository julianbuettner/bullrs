use std::time::Duration;

use bon::Builder;
use chrono::{DateTime, Utc};

/// Retry-backoff strategy for failed jobs.
#[derive(Debug, Clone)]
pub enum Backoff {
    /// Wait `delay` before each retry.
    Fixed {
        /// Constant delay between retries.
        delay: Duration,
    },
    /// Exponential backoff: `base * 2^(attempt-1)`. Optional jitter as a fraction
    /// (e.g. `0.1` ⇒ ±10%).
    Exponential {
        /// Base delay used for the first retry.
        base: Duration,
        /// Optional jitter factor (0.0–1.0).
        jitter: Option<f32>,
    },
}

/// Reference to a parent job in a flow.
#[derive(Debug, Clone)]
pub struct ParentRef {
    /// Parent job ID.
    pub id: String,
    /// Fully-qualified queue key of the parent (BullMQ-style).
    pub queue: String,
}

/// How long completed / failed jobs are retained.
#[derive(Debug, Clone)]
pub enum Retain {
    /// Keep all jobs forever.
    Forever,
    /// Drop jobs as soon as they finish.
    Drop,
    /// Keep the most recent N jobs.
    LastN(usize),
    /// Keep jobs newer than `age`.
    OlderThan(Duration),
    /// Keep jobs satisfying both bounds (count and age).
    Both {
        /// Maximum number of jobs to keep.
        count: usize,
        /// Maximum age of jobs to keep.
        age: Duration,
    },
}

/// Configure enqueue and retry behaviour of a job.
///
/// Repeating / scheduled jobs are configured via
/// [`Queue::upsert_job_scheduler`](crate::Queue::upsert_job_scheduler) — they
/// are not part of `JobOptions`.
#[derive(Default, Debug, Clone, Builder)]
pub struct JobOptions {
    /// Maximum tries before considering a job failed. Will be tried at least once,
    /// even for `Some(0)`.
    pub attempts: Option<usize>,

    /// Describe _when_ a job should be retried on failure (attempts > 1).
    /// With more than one attempt configured and no backoff defined, the job is
    /// directly retried.
    pub backoff: Option<Backoff>,

    /// Initial delay before the job becomes available to a worker.
    pub delay: Option<Duration>,

    /// Overwrite the auto-generated job ID. If a job with the given id already
    /// exists, it is not added.
    pub job_id: Option<String>,

    /// Keep only the N newest log entries of a job. `None` keeps all logs.
    pub limit_logs: Option<usize>,

    /// Last In First Out — push to the front of the wait list instead of the back.
    pub lifo: Option<bool>,

    /// Configure parent job relation.
    pub parent: Option<ParentRef>,

    /// No priority means highest priority. Higher numbers mean lower priority.
    /// Using priority comes at a cost (sorted-set maintenance — `O(log n)` instead
    /// of `O(1)`). `None` is highest priority; `Some(0)` next; max is
    /// `Some(2_097_152)`.
    pub priority: Option<usize>,

    /// When and how to keep jobs after completing.
    pub remove_on_complete: Option<Retain>,

    /// When and how to keep jobs after failing and exceeding all attempts.
    pub remove_on_fail: Option<Retain>,

    /// Maximum payload size in bytes.
    pub size_limit: Option<usize>,

    /// Maximum line count for stack traces.
    pub stack_trace_limit: Option<usize>,

    /// Creation timestamp; defaults to now when the job is enqueued.
    pub timestamp: Option<DateTime<Utc>>,

    /// Whether the parent should continue on failure of this job.
    pub continue_parent_on_failure: Option<bool>,

    /// Deduplication key (BullMQ debounce/dedup feature).
    pub deduplication: Option<String>,
}

/// Rate-limit configuration: at most `max` jobs per `window`.
#[derive(Debug, Clone, Copy)]
pub struct RateLimit {
    /// Maximum number of jobs allowed in `window`.
    pub max: usize,
    /// Sliding window duration.
    pub window: Duration,
}
