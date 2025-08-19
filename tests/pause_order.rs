use bullrs::Queue;
use deadpool_redis::{Config, Pool, Runtime};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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
async fn fifo_default_order() {
    let mut q = random_queue();
    q.add_default("A", &Input { input: 11 }).await.unwrap();
    q.add_default("B", &Input { input: 22 }).await.unwrap();
    q.add_default("C", &Input { input: 33 }).await.unwrap();
}
