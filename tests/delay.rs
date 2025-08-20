use std::time::{Duration, Instant};

use bullrs::{JobOptions, Queue, WorkerArgs};
use deadpool_redis::{Config, Pool, Runtime};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Input {
    input: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Output {
    output: i64,
}

fn random_queue() -> Queue<Input, Output> {
    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let name = format!("test-{}", nanoid!());
    Queue::new(pool, name)
}

#[tokio::test]
#[test_log::test]
async fn redis_delayed() {
    let q = random_queue();
    let mut w = q.worker(WorkerArgs::default());
    q.add_with(
        "A",
        &Input { input: 11 },
        &JobOptions::builder()
            .delay(Duration::from_millis(10))
            .build(),
    )
    .await
    .unwrap();
    let start = Instant::now();
    sleep(Duration::from_millis(5)).await;
    assert!(!w.has_work());

    let j = w.pop().await.unwrap();
    let diff = start.elapsed();
    assert!(diff < Duration::from_millis(11));
    assert!(diff > Duration::from_millis(10));

    assert_eq!(j.name(), "A");
    assert_eq!(j.data(), &Input { input: 11 });
}
