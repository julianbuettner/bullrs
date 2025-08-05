use anyhow::Result;
use deadpool_redis::{Config, Runtime};
use queue::Queue;
use std::{any::Any, collections::HashMap, marker::PhantomData, time::Duration};
use tokio::time::sleep;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{job_options::JobOptions, worker::Worker};

mod job;
mod job_lock;
mod job_options;
mod luacommands;
mod milliserde;
mod queue;
mod worker;

#[derive(Debug, Deserialize)]
struct Payload {
    a: i32,
    b: i32,
    c: i32,
}

#[derive(Deserialize, Serialize)]
struct Data(pub i64);

#[derive(Serialize, Deserialize)]
struct Return(pub i64);

#[derive(Deserialize, Serialize)]
struct Progress(pub f32);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let q: Queue<Data, Return> = Queue::new(pool, "pinkpony");
    let id = q
        .add("Somejob", &Data(99), &JobOptions::default())
        .await
        .unwrap();
    println!("Added job with id: {id}");

    let worker = q.worker();
    // let job = worker
    sleep(Duration::from_millis(1000)).await;

    Ok(())
}
