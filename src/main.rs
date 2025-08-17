use deadpool_redis::{Config, Runtime};
use log::info;
use queue::Queue;
use std::{
    env::args,
    thread::spawn,
    time::{Duration, Instant},
};
use tokio::{io::DuplexStream, time::sleep};

use serde::{Deserialize, Serialize};

use crate::{job_options::JobOptions, worker::WorkerArgs};

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

async fn create_job(q: &Queue<Data, Return>, name: &str) {
    let id = q
        .add(
            name,
            &Data {
                vehicle: "Boat".into(),
            },
            &JobOptions::builder()
                .attempts(99)
                .delay(Duration::from_secs(5))
                .build(),
        )
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    pool.resize(128);

    let q: Queue<Data, Return> = Queue::new(pool, "pinkpony");

    let c = 10;

    if args().find(|w| w == "j").is_some() {
        info!("Enqueue {c} jobs");
        let start = Instant::now();
        for i in 0..c {
            create_job(&q, format!("Job {i}").as_str()).await;
        }
        info!("Elapsed: {:?}", start.elapsed());
    }
    if args().find(|w| w == "w").is_some() {
        info!("Work {c} jobs");
        let mut worker = q.worker(WorkerArgs {
            parallel_jobs: 2,
            parallel_connections: 1,
            stalled_after: Duration::from_secs(2),
            ..Default::default()
        });
        info!("Do the worky work");
        let start = Instant::now();
        for _ in 0..c {
            let job = worker.pop().await.expect("Worker not stopped");
            tokio::spawn(async {
                job.log_ts("Huiiii").await;
                job.done(&Return(999)).await
            });
        }
        info!("Done after {:?}", start.elapsed());
    }
    // Let all jobs set their done values
    sleep(Duration::from_millis(500)).await;

    Ok(())
}
