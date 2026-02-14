use std::time::Duration;

use bon::Builder;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum BackoffType {
    /// After try n fails, wait delay * (n ^ 2) before retrying.
    Exponential,
    /// After try n fails, wait delay * n before retrying.
    Fixed,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BackoffOptions {
    /// Delay after next try for fixed backoff, or basis duration
    /// for calculating exponential backoff. It makes no sense to not
    /// provide a delay, but having it optional maintains compatibility with BullMQ.
    /// In this case, retries would happen instant.
    #[serde(with = "crate::milliserde::duration_millis_option")]
    pub delay: Option<Duration>,
    /// Choose between exponential and fixed backoff.
    pub r#type: BackoffType,
    /// Add a random factor to the delay, for a jitter
    /// of 0.1 (10%) the delay will be between 0.9 * original delay
    /// and 1.1 * original delay. This can be useful to distribute
    /// retries over time, if many jobs failed at once (thundering herd problem).
    pub jitter: Option<f32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Backoff {
    #[serde(with = "crate::milliserde::duration_millis")]
    Number(Duration),
    BackoffOptions(BackoffOptions),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ParentOptions {
    /// ID of the parent
    pub id: String,
    // Queue name in which the parent is placed,
    // including namespace separator, colon.
    queue: String,
}

#[derive(Debug, Serialize)]
pub enum KeepJobs {
    /// How many jobs to keep after processing
    Count(usize),
    /// Just keep or delete job after processing
    Bool(bool),
    /// If providing both values,
    /// jobs will be cleared for both configurations.
    Config {
        #[serde(with = "crate::milliserde::duration_millis_option")]
        age: Option<Duration>,
        count: Option<usize>,
    },
}

/// Configure enqueue and retry behaviour of a job.
#[derive(Default, Debug, Serialize, Builder)]
pub struct JobOptions {
    /// Maximum tries before considering a job failed. Will be tried at least once, even for `Some(0)`.
    pub attempts: Option<usize>,

    /// Describe _when_ a job should be retried on failure (attempts > 1),
    /// With more than one attempt configured and no backoff defined, the job is directly retried.
    /// This is rarely what you would want, so consider configuring a backoff.
    pub backoff: Option<Backoff>,

    /// Basis delay for exponential backoff or delay between linear retries.
    #[serde(with = "crate::milliserde::duration_millis_option")]
    pub delay: Option<Duration>,

    /// Overwrite JobID. By default, every job gets an auto incremented
    /// integer (compare to PostgreSQL Serial), but you can define any string.
    /// If a job with the given id already exists, it is not added. Use
    /// the deduplication feature for deduplication.
    pub job_id: Option<String>,

    /// Keep only the N newest logs of a job. `None` keeps all logs.
    #[serde(rename = "kl")]
    pub limit_logs: Option<usize>,

    /// Last In First Out, makes rarely sense.
    pub lifo: Option<bool>,

    /// Configure parent job relation
    pub parent: Option<ParentOptions>,

    /// No priority means highest priority. Higher numbers
    /// mean lower priority. Using priority comes at a cost though.
    /// A sorted set (compare to datastructure heap) has to be maintained.
    /// Adding and popping jobs is in _O(log(n))_ instead of _O(1)_.
    /// `None` has highest priority, then Some(0) and the lowest possible
    /// priority is Some(2_097_152).
    pub priority: Option<usize>,

    /// When and how to keep jobs after completing
    pub remove_on_complete: Option<KeepJobs>,

    /// When and how to keep jobs after failing and exceeding all attempts
    pub remove_on_fail: Option<KeepJobs>,

    // repeat skipped for now
    // repeatJobKey skipped for now
    /// How many bytes is the job data allowed to have
    pub size_limit: Option<usize>,

    /// Maximum line count the stack is allowed to have
    pub stack_trace_limit: Option<usize>,

    /// When was job created, usually set automatically.
    #[serde(with = "crate::milliserde::timestamp_millis_option")]
    pub timestamp: Option<DateTime<Utc>>,

    /// Whether or not it's parent should continue on failure.
    /// If set to false and the job fails, it's parent is marked as failed as well.
    /// Failing parents can propagate recursively until the entire anchestor tree is
    /// disappointed. Note that parents are only moved to failed once they are picked up
    /// by a worker and the worker sees about the failed child.
    #[serde(rename = "cpof")]
    pub continue_parent_on_failure: Option<bool>,

    /// I haven't yet looked into what this one does.
    #[serde(rename = "de")]
    pub deduplication_something: Option<String>,
}
