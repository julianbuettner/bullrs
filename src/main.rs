use anyhow::Result;
use deadpool_redis::{Config, Runtime};
use job::{JobScheduling, JobState};
use luacommands::UPDATE_DATA;
use queue::Queue;
use redis::{Commands, Connection, aio::MultiplexedConnection};
use std::{any::Any, collections::HashMap, marker::PhantomData, time::Duration};
use tokio::time::sleep;
use worker::{CallbackWorker, JobHandle};

use chrono::{DateTime, Utc};
use redis::{AsyncCommands as _, FromRedisValue, JsonAsyncCommands, RedisError, from_redis_value};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
mod job;
mod luacommands;
mod queue;
mod worker;

#[derive(Debug, Deserialize)]
struct Payload {
    a: i32,
    b: i32,
}

#[derive(Debug, Deserialize)]
struct JobOptions {
    attempts: usize,
}

#[derive(Deserialize)]
struct Data(pub i64);

#[derive(Serialize, Deserialize)]
struct Return(pub i64);

#[derive(Deserialize, Serialize)]
struct Progress(pub f32);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let worker = CallbackWorker::new(pool.clone(), "myqueue", 2);
    println!("Block now waiting...");
    worker
        .await
        .work_blocking_callback(async |j: JobHandle<Data, Return, Progress>| {
            println!("Received job {} with name {}", j.id, j.name);
            Ok(Return(32))
        })
        .await
        .unwrap();

    let mut q: Queue<usize, usize> = Queue::new(pool, "myqueue");
    println!(
        "Global concurrency: {:?}",
        q.get_global_concurrency().await?
    );

    let job_id = q
        .schedule(JobScheduling {
            name: "Heyo".to_string(),
            data: 1024,
            priority: None,
        })
        .await?;
    println!("Scheduled Job with id: {:?}", job_id);

    sleep(Duration::from_secs_f32(0.5)).await;

    let job_state: JobState<_, _> = q.get_job_state(&job_id).await.unwrap();
    println!("Job state after 500ms: {job_state:#?}");

    Ok(())
}
