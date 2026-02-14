# BullRS

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
```
use bullrs::deadpool_redis::{self, Pool, Runtime};
use bullrs::{JobOptions, ProgressPercent, Queue, QueueName, Worker, WorkerArgs};

let pool_config = Config::from_url("redis://127.0.0.1/");
let pool = pool_config.create_pool(None).unwrap();
let queue_name = QueueName::new("my-queue").unwrap();

let queue: Queue<i64, i64> = Queue::new(pool, queue_name);
queue.add("My Job", 123);

let worker = queue.worker();
// Job is Option<Result<Job, bullrs::BullrsError>>
let next_job = worker.next().await.unwrap().unwrap();
next_job.log_ts("Starting calculation...").await.unwrap();
next_job.
```

## Features (WIP)
BullMQ has many features. The list below keeps track, which of them are imeplemented in BullRS:

- Managing Jobs
    - [x] Adding immediate Jobs, LIFO and FIFO
    - [ ] Awaiting Job Results
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
