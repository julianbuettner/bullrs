use bullrs::{JobOptions, Queue, WorkerArgs};
use deadpool_redis::{Config, Pool, Runtime};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};

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
async fn redis_fifo__order() {
    let mut q = random_queue();
    q.add("A", &Input { input: 11 }).await.unwrap();
    q.add("B", &Input { input: 22 }).await.unwrap();
    q.add("C", &Input { input: 33 }).await.unwrap();

    let mut w = q.worker(WorkerArgs::default());

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "A");
    assert_eq!(j.data(), &Input { input: 11 });

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "B");
    assert_eq!(j.data(), &Input { input: 22 });

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "C");
    assert_eq!(j.data(), &Input { input: 33 });
}

#[tokio::test]
#[test_log::test]
async fn redis_fofo_lifo_mix() {
    let mut q = random_queue();
    q.add("A", &Input { input: 11 }).await.unwrap();
    q.add("B", &Input { input: 22 }).await.unwrap();
    q.add_with(
        "C",
        &Input { input: 33 },
        &JobOptions::builder().lifo(true).build(),
    )
    .await
    .unwrap();
    q.add_with(
        "D",
        &Input { input: 44 },
        &JobOptions::builder().lifo(true).build(),
    )
    .await
    .unwrap();

    let mut w = q.worker(WorkerArgs::default());

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "D");
    assert_eq!(j.data(), &Input { input: 44 });

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "C");
    assert_eq!(j.data(), &Input { input: 33 });

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "A");
    assert_eq!(j.data(), &Input { input: 11 });

    let j = w.pop().await.unwrap();
    assert_eq!(j.name(), "B");
    assert_eq!(j.data(), &Input { input: 22 });
}
