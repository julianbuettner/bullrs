use chrono::Utc;
use serde::Serialize;

use crate::{
    job::JobOptions,
    luacommands::{ADD_STANDARD_JOB, InvokeLuaScript},
    queue::QueueName,
};

pub struct AddStandardJob<'a, D> {
    // Name of the queue we want to add the job to
    pub queue: &'a QueueName,
    pub job_name: &'a str,
    pub data: &'a D,
    pub job_options: &'a JobOptions,
}

impl<'a, D> InvokeLuaScript for AddStandardJob<'a, D>
where
    D: Serialize,
{
    type Return = String;
    async fn call(
        self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        let key_prefix = self.queue.prefix();
        let custom_id: &str = self.job_options.job_id.as_deref().unwrap_or("");
        let parent_key: Option<String> = None;
        let wait_children_key = "";
        let parent_dependencies_key = "";
        let parent: Option<String> = None;
        let repeat_job_key = "";
        let deduplication_key = "";
        let job_name = self.job_name;
        let timestamp = self
            .job_options
            .timestamp
            .unwrap_or_else(Utc::now)
            .timestamp_millis();
        let arguments_tuple = (
            key_prefix,
            custom_id,
            job_name,
            timestamp,
            parent_key,
            wait_children_key,
            parent_dependencies_key,
            parent,
            repeat_job_key,
            deduplication_key,
        );
        Ok(ADD_STANDARD_JOB
            .key(self.queue.wait())
            .key(self.queue.paused())
            .key(self.queue.meta())
            .key(self.queue.id())
            .key(self.queue.completed())
            .key(self.queue.delayed())
            .key(self.queue.active())
            .key(self.queue.events())
            .key(self.queue.marker())
            .arg(rmp_serde::to_vec(&arguments_tuple).unwrap())
            .arg(serde_json::to_string(self.data).unwrap())
            .arg(rmp_serde::to_vec_named(self.job_options).unwrap())
            .invoke_async(con)
            .await
            .unwrap())
    }
}
