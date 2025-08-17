use deadpool_redis::{Config, Runtime};
use queue::Queue;
use std::env::args;

use serde::{Deserialize, Serialize};

use crate::job_options::JobOptions;

mod job;
mod job_lock;
mod job_options;
mod luacommands;
mod milliserde;
mod progress;
mod queue;
mod redisext;
mod worker;

#[derive(Debug, Deserialize)]
struct Payload {
    a: i32,
    b: i32,
    c: i32,
}

#[derive(Deserialize, Serialize, Debug)]
struct Data {
    vehicle: String,
}

#[derive(Serialize, Deserialize)]
struct Return(pub i64);

#[derive(Deserialize, Serialize)]
struct Progress(pub f32);

async fn create_job(q: &Queue<Data, Return>) {
    let id = q
        .add(
            "Somejob",
            &Data {
                vehicle: "Boat".into(),
            },
            &JobOptions::default(),
        )
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    pool.resize(32);

    let q: Queue<Data, Return> = Queue::new(pool, "pinkpony");

    if args().find(|w| w == "j").is_some() {
        println!("Job");
        create_job(&q).await;
    }
    if args().find(|w| w == "w").is_some() {
        println!("Work");
        let mut worker = q.worker();
        loop {
            let job = worker.pop().await.expect("Worker not stopped");
            println!("Hooray: {:?}", job.data());
            job.done(&Return(999)).await;
        }
    }

    Ok(())
}
