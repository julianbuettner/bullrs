use chrono::Utc;
use serde::Serialize;

use crate::{
    job::JobOptions,
    luacommands::{ADD_DELAYED_JOB, InvokeLuaScript},
    queue::QueueName,
};

pub struct AddDelayedJob<'a, D> {
    pub queue: &'a QueueName,
    pub job_name: &'a str,
    pub data: &'a D,
    pub job_options: &'a JobOptions,
}

impl<'a, D> InvokeLuaScript for AddDelayedJob<'a, D>
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
        let parent_dependencies_key = "";
        let repeat_job_key = "";
        let deduplication_key = "";
        let job_name = self.job_name;
        let timestamp = self
            .job_options
            .timestamp
            .unwrap_or_else(Utc::now)
            .timestamp_millis();
        let arguments = (
            key_prefix,
            custom_id,
            job_name,
            timestamp,
            parent_key,
            parent_dependencies_key,
            repeat_job_key,
            deduplication_key,
        );

        ADD_DELAYED_JOB
            .key(self.queue.marker())
            .key(self.queue.meta())
            .key(self.queue.id())
            .key(self.queue.delayed())
            .key(self.queue.completed())
            .key(self.queue.events())
            .arg(rmp_serde::to_vec(&arguments).expect("serializing never fails"))
            .arg(serde_json::to_string(self.data).expect("TODO: might fail"))
            .arg(rmp_serde::to_vec_named(self.job_options).expect("serializing never fails"))
            .invoke_async(con)
            .await
    }
}
