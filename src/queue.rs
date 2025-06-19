use std::{marker::PhantomData, time::SystemTime};

use chrono::{DateTime, Utc};
use deadpool_redis::{Manager, Pool};
use redis::{AsyncCommands, Client, RedisResult, aio::MultiplexedConnection};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    job::{JobScheduling, JobState},
    luacommands::ADD_STANDARD_JOB,
};

#[derive(Clone)]
pub struct Queue<D, R> {
    name: String,
    data: PhantomData<D>, // job payload
    response: PhantomData<R>,
    pool: Pool,
}

impl<D, R> Queue<D, R> {
    pub fn new(pool: Pool, name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            data: Default::default(),
            response: Default::default(),
            pool,
        }
    }

    pub async fn get_global_concurrency(&mut self) -> anyhow::Result<Option<usize>> {
        Ok(self
            .pool
            .get()
            .await?
            .hget(format!("bull:{}:meta", self.name), "concurrency")
            .await?)
    }

    pub async fn schedule(&mut self, j: JobScheduling<D>) -> anyhow::Result<String>
    where
        D: Serialize,
    {
        #[derive(Debug, Serialize)]
        struct LocalOpts {
            de: String,
            lifo: bool,
        }

        let key_prefix = format!("bull:{}:", self.name);
        let custom_id: String = "".into();
        let parent_key: Option<String> = None;
        let wait_children_key: Option<String> = None;
        let parent_dependencies_key: Option<String> = None;
        let parent: Option<String> = None;
        let repeat_job_key: Option<String> = None;
        let deduplication_key: Option<String> = None;
        let arguments_tuple = (
            key_prefix,
            custom_id,
            j.name,
            Utc::now().timestamp_millis(),
            parent_key,
            wait_children_key,
            parent_dependencies_key,
            parent,
            repeat_job_key,
            deduplication_key,
        );

        let opts = LocalOpts {
            de: "".to_string(),
            lifo: false,
        };

        let mut connection = self.pool.get().await?;
        let result: String = ADD_STANDARD_JOB
            .key(format!("bull:{}:wait", self.name))
            .key(format!("bull:{}:paused", self.name))
            .key(format!("bull:{}:meta", self.name))
            .key(format!("bull:{}:id", self.name))
            .key(format!("bull:{}:completed", self.name))
            .key(format!("bull:{}:active", self.name))
            .key(format!("bull:{}:events", self.name))
            .key(format!("bull:{}:marker", self.name))
            .arg(rmp_serde::to_vec(&arguments_tuple).unwrap())
            .arg(serde_json::to_string(&j.data).unwrap())
            .arg(rmp_serde::to_vec_named(&opts).unwrap())
            .invoke_async(&mut connection)
            .await?;

        Ok(result)
    }

    pub async fn get_job_state<P>(&mut self, job_id: &str) -> anyhow::Result<JobState<D, R, P>>
    where
        D: DeserializeOwned,
        R: DeserializeOwned,
        P: DeserializeOwned,
    {
        Ok(self
            .pool
            .get()
            .await?
            .hgetall(format!("bull:{}:{}", self.name, job_id))
            .await?)
    }
}

pub struct QueueName(pub String);

impl QueueName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn prefix(&self) -> String {
        format!("bull:{}:", self.0)
    }
    pub fn stalled(&self) -> String {
        format!("bull:{}:stalled", self.0)
    }
    pub fn wait(&self) -> String {
        format!("bull:{}:wait", self.0)
    }

    pub fn active(&self) -> String {
        format!("bull:{}:active", self.0)
    }

    pub fn failed(&self) -> String {
        format!("bull:{}:failed", self.0)
    }

    pub fn completed(&self) -> String {
        format!("bull:{}:completed", self.0)
    }

    pub fn paused(&self) -> String {
        format!("bull:{}:paused", self.0)
    }

    pub fn delayed(&self) -> String {
        format!("bull:{}:delayed", self.0)
    }

    pub fn prioritized(&self) -> String {
        format!("bull:{}:prioritized", self.0)
    }

    pub fn meta(&self) -> String {
        format!("bull:{}:meta", self.0)
    }

    pub fn marker(&self) -> String {
        format!("bull:{}:marker", self.0)
    }

    pub fn stalled_check(&self) -> String {
        format!("bull:{}:stalled-check", self.0)
    }

    pub fn events(&self) -> String {
        format!("bull:{}:events", self.0)
    }

    pub fn id(&self) -> String {
        format!("bull:{}:id", self.0)
    }
}
