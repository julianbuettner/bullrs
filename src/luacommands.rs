use std::time::Duration;

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use redis::{RedisResult, Script, ScriptInvocation, aio::ConnectionLike};

use crate::{job::JobId, queue::QueueName};

macro_rules! load_script {
    ($filename:expr) => {
        Script::new(include_str!(concat!(env!("OUT_DIR"), "/lua/", $filename)))
    };
}

lazy_static! {
    pub static ref ADD_LOG: Script = load_script!("addLog-2.lua");
    pub static ref ADD_STANDARD_JOB: Script = load_script!("addStandardJob-8.lua");
    pub static ref UPDATE_DATA: Script = load_script!("updateData-1.lua");
    static ref MOVE_STALLED_JOBS_TO_WAIT: Script = load_script!("moveStalledJobsToWait-9.lua");
}

pub struct MoveStalledJobsToWait<'a> {
    /// Name of the queue we are doing maintenance work (stalled jobs to waiting) for
    pub queue: &'a QueueName,
    /// If a job stalls this often, mark it failed
    pub max_stalled_before_failed: usize,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
    /// A worker is supposed to refresh the lease on jobs
    /// it's currently working on. If the worker crashes or the
    /// event loop is blocked, how much time is allowed to pass,
    /// before it's moved back into the ready set.
    pub max_duration: Duration,
}

impl<'a> MoveStalledJobsToWait<'a> {
    pub async fn call<'b>(
        self,
        con: &mut impl ConnectionLike,
    ) -> RedisResult<(Vec<String>, Vec<String>)> {
        MOVE_STALLED_JOBS_TO_WAIT
            .key(self.queue.stalled())
            .key(self.queue.wait())
            .key(self.queue.active())
            .key(self.queue.failed())
            .key(self.queue.stalled_check())
            .key(self.queue.meta())
            .key(self.queue.paused())
            .key(self.queue.marker())
            .key(self.queue.events())
            .arg(self.max_stalled_before_failed)
            .arg(self.queue.prefix())
            .arg(self.timestamp.timestamp_millis())
            .arg(self.max_duration.as_millis() as u64)
            .invoke_async(con)
            .await
    }
}
