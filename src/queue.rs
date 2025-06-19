use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use deadpool_redis::{Manager, Pool};
use redis::{AsyncCommands, Client, RedisResult, aio::MultiplexedConnection};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    job_depre::{JobScheduling, JobState},
    luacommands::ADD_STANDARD_JOB,
};

/// Performance note: cloning the queue is
/// cheap, performing not heap allocations.
#[derive(Clone)]
pub struct Queue<D, R, P = String, E = String> {
    name: QueueName,
    pool: Pool,
    phantom: PhantomData<(D, R, P, E)>, // Data, Result, Progress, Error
}

impl<D, R> Queue<D, R> {
    pub fn new(pool: Pool, name: impl ToString) -> Self {
        Self {
            name: QueueName::new(name),
            pool,
            phantom: PhantomData,
        }
    }

    pub async fn get_global_concurrency(&mut self) -> anyhow::Result<Option<usize>> {
        Ok(self
            .pool
            .get()
            .await?
            .hget(self.name.meta(), "concurrency")
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

        let key_prefix = self.name.prefix();
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
            .key(self.name.wait())
            .key(self.name.paused())
            .key(self.name.meta())
            .key(self.name.id())
            .key(self.name.completed())
            .key(self.name.active())
            .key(self.name.events())
            .key(self.name.marker())
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
            .hgetall(format!("bull:{}:{}", self.name.as_str(), job_id))
            .await?)
    }
}

#[derive(Debug, Clone)]
pub struct QueueName(Arc<String>);

impl QueueName {
    pub fn new(name: impl ToString) -> Self {
        Self(Arc::from(name.to_string()))
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

    pub fn stalled(&self) -> String {
        format!("bull:{}:stalled", self.0)
    }

    pub fn events(&self) -> String {
        format!("bull:{}:events", self.0)
    }

    pub fn id(&self) -> String {
        format!("bull:{}:id", self.0)
    }

    pub fn job(&self, job_id: &str) -> String {
        format!("bull:{}:{}", self.0, job_id)
    }

    pub fn job_lock(&self, job_id: &str) -> String {
        format!("bull:{}:{}:lock", self.0, job_id)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
