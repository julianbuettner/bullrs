use bullrs::{JobOptions, Queue, WorkerArgs};
use deadpool_redis::{Config, Pool, Runtime};
use nanoid::nanoid;
use ntest::timeout;
use serde::{Deserialize, Serialize};
mod setup;
use setup::*;

#[tokio::test]
#[test_log::test]
#[timeout(3_000)]
async fn redis_fifo_order() {
    let tq = setup::TestQueue::new("order");
    let q = &tq.queue;
    q.add("A", &Input { input: 11 }).await.unwrap();
    q.add("B", &Input { input: 22 }).await.unwrap();
    q.add("C", &Input { input: 33 }).await.unwrap();

    let mut w = q.worker(WorkerArgs::default());

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "A");
    assert_eq!(j.data(), &Input { input: 11 });

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "B");
    assert_eq!(j.data(), &Input { input: 22 });

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "C");
    assert_eq!(j.data(), &Input { input: 33 });
}

#[tokio::test]
#[test_log::test]
#[timeout(3_000)]
async fn redis_fofo_lifo_mix() {
    let tq = setup::TestQueue::new("lifo");
    let q = &tq.queue;
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

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "D");
    assert_eq!(j.data(), &Input { input: 44 });

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "C");
    assert_eq!(j.data(), &Input { input: 33 });

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "A");
    assert_eq!(j.data(), &Input { input: 11 });

    let j = w.next().await.unwrap().unwrap();
    assert_eq!(j.name(), "B");
    assert_eq!(j.data(), &Input { input: 22 });
}
