# ⚠️ Deprecated: BullRS

> **This project is deprecated and no longer maintained.**
>
> BullRS was built as a Rust port of [BullMQ](https://github.com/taskforcesh/bullmq)'s Redis protocol. Since BullMQ has expanded to multiple languages and remains very actively maintained, I recommend using the official project instead:
>
> - **BullMQ** — [github.com/taskforcesh/bullmq](https://github.com/taskforcesh/bullmq)
> - **Homepage** — [bullmq.io](https://bullmq.io)
> - **Docs** — [docs.bullmq.io](https://docs.bullmq.io)
> - **Supported languages** — Node.js/TypeScript, Python, Elixir, PHP
>
> If you need a Redis-backed job queue in Rust, see [apalis](https://github.com/geofmureithi/apalis) or [faktory-rs](https://github.com/jonhoo/faktory-rs) as alternatives.

---

## BullRS (historical reference)

BullRS is a BullMQ compatible message queue for highly reliable job processing.

BullRS uses Redis to manage jobs in a highly reliable and scalable manner.
Distribute jobs across workers, with retrials, result values, inspecting logs per job and much more.  
It's a great choice for distributed, event driven systems with fallible units of work.

BullRS is async and builds on the tokio runtime.

Priorities:
- 1. **Reliability** - everything should work exactly as expected and no job should ever be lost
- 2. **Ease of use** - beginner friendly, sensible defaults and hard to misuse API
- 3. **Performance** - reduce round trips, maximize concurrence

The documentation is hosted on [docs.rs/bullrs](https://docs.rs/bullrs/latest/bullrs/).

## Example

A queue that squares `f32` values. The producer and worker share the same queue instance
here. In production they often run in separate processes.

```rust,no_run
use bullrs::{Queue, QueueName, WorkerArgs};
use deadpool_redis::{Config, Runtime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Input {
    value: f32,
}

// Output needs Debug + Clone in addition to the serde traits.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Output {
    result: f32,
}

#[tokio::main]
async fn main() {
    let pool = Config::from_url("redis://127.0.0.1/")
        .create_pool(Some(Runtime::Tokio1))
        .unwrap();

    let queue = Queue::<Input, Output>::new(
        pool,
        QueueName::new("square-queue".to_string()).unwrap(),
    );

    // Enqueue a job and keep the handle to await its result later.
    let handle = queue
        .add("square", &Input { value: 3.1 })
        .await
        .unwrap();

    // Pull the next job and process it.
    let mut worker = queue.worker(WorkerArgs::default());
    let job = worker.next().await.unwrap().unwrap();
    let squared = job.data().value * job.data().value;
    job.done(&Output { result: squared }).await.unwrap();

    // Wait for the result on the producer side.
    let output = handle.result().await.unwrap();
    println!("3.1 ^ 2 = {}", output.result);
}
```

## Features (WIP)
BullMQ has many features. The list below keeps track, which of them are imeplemented in BullRS:

- Managing Jobs
    - [x] Adding immediate Jobs, LIFO and FIFO
    - [x] Awaiting Job Results
    - [ ] Remove Jobs
    - [x] Adding delayed Jobs
    - [x] Adding priority Jobs
    - [ ] Repeatable Jobs
    - [ ] Job Hiearchy
- Worker
    - [x] Dequeue immediate Jobs
    - [x] Requeue stalled jobs (e.g. worker went offline during processing)
    - [x] Retry jobs with backoff
    - [ ] Repeatable Jobs
    - [ ] Job Hiearchy
- Queue
    - [x] Pause / unpause entire queue
    - [x] Obliterate queue
