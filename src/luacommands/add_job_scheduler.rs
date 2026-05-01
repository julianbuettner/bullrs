use chrono::Utc;
use redis::Value;
use serde::Serialize;

use crate::{
    JobOptions, Repeat, SchedulerId, SchedulerTemplate, SchedulerWindow,
    bullmq::{options::WireJobOptions, scheduler::WireSchedulerOpts},
    error::AddJobSchedulerError,
    luacommands::{ADD_JOB_SCHEDULER, InvokeLuaScript},
    queue::QueueName,
};

/// Successful result of adding a job scheduler.
pub struct AddJobSchedulerOk {
    /// The ID of the next scheduled job
    pub job_id: String,
    /// Delay in milliseconds until the next job fires
    pub delay: i64,
}

/// Add or update a job scheduler.
pub struct AddJobScheduler<'a, D> {
    pub queue: &'a QueueName,
    pub scheduler_id: &'a SchedulerId,
    /// Next fire time in milliseconds since epoch.
    pub next_millis: i64,
    /// Repetition rule.
    pub repeat: &'a Repeat,
    /// Optional bounds (start/end/limit/immediately).
    pub window: &'a SchedulerWindow,
    /// Template applied to every produced job.
    pub template: SchedulerTemplate<'a, D>,
    /// Job options applied when creating each scheduled job.
    pub delayed_opts: &'a JobOptions,
    /// Optional producer key (for flow producers).
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
        let template_data_json = serde_json::to_string(self.template.data)?;
        let now = Utc::now().timestamp_millis();
        let scheduler_opts =
            WireSchedulerOpts::from_domain(self.template.name, self.repeat, self.window);

        let mut invocation = ADD_JOB_SCHEDULER.prepare_invoke();
        invocation
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
            .arg(self.next_millis) // ARGV[1]
            .arg(rmp_serde::to_vec_named(&scheduler_opts).expect("serializing never fails")) // ARGV[2]
            .arg(self.scheduler_id.as_ref()) // ARGV[3]
            .arg(&template_data_json) // ARGV[4]
            .arg(
                rmp_serde::to_vec_named(&WireJobOptions::from(self.template.opts))
                    .expect("serializing never fails"),
            ) // ARGV[5]
            .arg(
                rmp_serde::to_vec_named(&WireJobOptions::from(self.delayed_opts))
                    .expect("serializing never fails"),
            ) // ARGV[6]
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
