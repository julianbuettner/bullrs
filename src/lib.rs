#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
mod job;
mod luacommands;

mod flowproducer;
/// Multiple serde modules for converting between milliseconds (as in JavaScript Dates once
/// strigified) and rust equivalents, namely `Duration`, `Option<Duration>`,
/// `chrono::DateTime<Utc>` and `Option<chrono::DateTime<Utc>>`.
pub mod milliserde;
mod progress;
mod queue;
mod redisext;
mod worker;

pub use job::{JobJoinHandle, JobOptions};
pub use progress::*;
pub use queue::{InvalidQueueName, PreparedFlowJob, Queue, QueueName};
pub use worker::{Worker, WorkerArgs};

// stable: 437
// nightly: 471
