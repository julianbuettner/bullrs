use std::env::args;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    job_options::JobOptions,
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
        self: Self,
        con: &mut impl redis::aio::ConnectionLike,
    ) -> redis::RedisResult<Self::Return> {
        let key_prefix = self.queue.prefix();
        let custom_id: &str = self
            .job_options
            .job_id
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or(&"");
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
            .unwrap_or_else(|| Utc::now())
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

        let res = ADD_DELAYED_JOB
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
            .await;
        res
    }
}
