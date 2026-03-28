use chrono::Utc;
use redis::Value;
use serde::Serialize;

use crate::{
    JobOptions,
    error::AddJobSchedulerError,
    luacommands::{ADD_JOB_SCHEDULER, InvokeLuaScript},
    queue::QueueName,
};

/// Options describing the schedule (cron pattern or fixed interval).
#[derive(Debug, Serialize)]
pub struct JobSchedulerOpts {
    /// Name of the jobs created by this scheduler
    pub name: String,
    /// Timezone for cron pattern evaluation (e.g. "Europe/Berlin")
    pub tz: Option<String>,
    /// Cron pattern string
    pub pattern: Option<String>,
    /// End date as millisecond timestamp — scheduler stops producing jobs after this
    #[serde(rename = "endDate")]
    pub end_date: Option<i64>,
    /// Fixed interval in milliseconds (alternative to cron pattern)
    pub every: Option<u64>,
    /// Offset in milliseconds applied to the "every" schedule
    pub offset: Option<i64>,
    /// Start date as millisecond timestamp for "every" mode
    #[serde(rename = "startDate")]
    pub start_date: Option<i64>,
    /// Maximum number of jobs to produce
    pub limit: Option<u64>,
}

/// Successful result of adding a job scheduler.
pub struct AddJobSchedulerOk {
    /// The ID of the next scheduled job
    pub job_id: String,
    /// Delay in milliseconds until the next job fires
    pub delay: i64,
}

pub struct AddJobScheduler<'a, D> {
    pub queue: &'a QueueName,
    /// Unique identifier for this scheduler
    pub job_scheduler_id: &'a str,
    /// Next fire time in milliseconds since epoch.
    /// For cron patterns, computed by the caller.
    /// For "every" mode, may be recomputed by the Lua script.
    pub next_millis: i64,
    /// Schedule configuration (cron pattern or fixed interval)
    pub scheduler_opts: &'a JobSchedulerOpts,
    /// Template data for jobs created by this scheduler
    pub template_data: &'a D,
    /// Template job options stored with the scheduler
    pub template_opts: &'a JobOptions,
    /// Job options applied when creating each scheduled job
    pub delayed_opts: &'a JobOptions,
    /// Optional producer key (for flow producers)
    pub producer_key: Option<&'a str>,
}

impl<'a, D> InvokeLuaScript for AddJobScheduler<'a, D>
where
    D: Serialize,
{
    type RedisOutput = Value;
    type DomainOk = AddJobSchedulerOk;
    type DomainErr = AddJobSchedulerError;

    fn generate_invocation(&self) -> Result<redis::ScriptInvocation<'static>, Self::DomainErr> {
        let template_data_json = serde_json::to_string(self.template_data)?;
        let now = Utc::now().timestamp_millis();

        let mut invocation = ADD_JOB_SCHEDULER.prepare_invoke();
        invocation
            // KEYS
            .key(self.queue.repeat()) // KEYS[1]
            .key(self.queue.delayed()) // KEYS[2]
            .key(self.queue.wait()) // KEYS[3]
            .key(self.queue.paused()) // KEYS[4]
            .key(self.queue.meta()) // KEYS[5]
            .key(self.queue.prioritized()) // KEYS[6]
            .key(self.queue.marker()) // KEYS[7]
            .key(self.queue.id()) // KEYS[8]
            .key(self.queue.events()) // KEYS[9]
            .key(self.queue.priority_counter()) // KEYS[10]
            .key(self.queue.active()) // KEYS[11]
            // ARGV
            .arg(self.next_millis) // ARGV[1]
            .arg(rmp_serde::to_vec_named(self.scheduler_opts).expect("serializing never fails")) // ARGV[2]
            .arg(self.job_scheduler_id) // ARGV[3]
            .arg(&template_data_json) // ARGV[4]
            .arg(rmp_serde::to_vec_named(self.template_opts).expect("serializing never fails")) // ARGV[5]
            .arg(rmp_serde::to_vec_named(self.delayed_opts).expect("serializing never fails")) // ARGV[6]
            .arg(now) // ARGV[7]
            .arg(self.queue.prefix()) // ARGV[8]
            .arg(self.producer_key.unwrap_or("")); // ARGV[9]
        Ok(invocation)
    }

    fn map_value(&self, value: Self::RedisOutput) -> Result<Self::DomainOk, Self::DomainErr> {
        match value {
            Value::Int(-10) => Err(AddJobSchedulerError::JobIdCollision),
            Value::Int(-11) => Err(AddJobSchedulerError::JobSlotsBusy),
            Value::Array(ref items) if items.len() == 2 => {
                let job_id = match &items[0] {
                    Value::BulkString(s) => String::from_utf8_lossy(s).into(),
                    Value::SimpleString(s) => s.clone(),
                    other => {
                        return Err(redis::RedisError::from((
                            redis::ErrorKind::ResponseError,
                            "Unexpected job_id type from addJobScheduler",
                            format!("{other:?}"),
                        )))?;
                    }
                };
                let delay = match &items[1] {
                    Value::Int(d) => *d,
                    other => {
                        return Err(redis::RedisError::from((
                            redis::ErrorKind::ResponseError,
                            "Unexpected delay type from addJobScheduler",
                            format!("{other:?}"),
                        )))?;
                    }
                };
                Ok(AddJobSchedulerOk { job_id, delay })
            }
            x => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Unexpected response from addJobScheduler lua script",
                format!("Response was {x:?}"),
            )))?,
        }
    }
}
