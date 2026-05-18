#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
mod bullmq;
mod job;
mod luacommands;
mod scheduler;

/// Error types for all queue, worker and job operations.
pub mod error;
mod event_system;
mod flowproducer;
/// Multiple serde modules for converting between milliseconds (as in JavaScript Dates once
/// strigified) and rust equivalents, namely `Duration`, `Option<Duration>`,
/// `chrono::DateTime<Utc>` and `Option<chrono::DateTime<Utc>>`.
pub mod milliserde;
mod progress;
mod queue;
mod redisext;
mod worker;

pub use event_system::QueueEvent;
pub use job::{ActiveJob, Backoff, JobJoinHandle, JobOptions, ParentRef, RateLimit, Retain};
pub use progress::*;
pub use queue::{InvalidQueueName, PreparedFlowJob, Queue, QueueName};
pub use scheduler::{Repeat, SchedulerId, SchedulerInfo, SchedulerTemplate, SchedulerWindow};
pub use worker::{Worker, WorkerArgs};

pub use deadpool_redis;

// stable: 437
// nightly: 471
