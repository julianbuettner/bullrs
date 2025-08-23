use std::thread::spawn;

use bullrs::Queue;
use deadpool_redis::{Config, Pool, Runtime};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use tokio::runtime;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Input {
    pub input: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub output: i64,
}

pub struct TestQueue {
    pub queue: Queue<Input, Output>,
}

fn get_pool() -> Pool {
    let cfg = Config::from_url("redis://127.0.0.1/");
    cfg.create_pool(Some(Runtime::Tokio1)).unwrap()
}

impl TestQueue {
    pub fn new(pref: &str) -> Self {
        let name = format!("test-{}-{}", pref, nanoid!());
        let pool = get_pool();
        Self {
            queue: Queue::new(pool, name),
        }
    }
}

fn uglydrop(name: String) {
    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = get_pool();
    rt.block_on(async move {
        let q = Queue::<Input, Output>::new(pool, name.clone());
        q.obliterate().await;
    });
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        let name = self.queue.name().to_string();
        let jh = spawn(|| uglydrop(name));
        jh.join().unwrap();
    }
}
