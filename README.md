# BullRS

A BullMQ compatible message queue for highly reliable job processing.

## Why use BullRS or BullMQ
BullMQ and BullRS use Redis to manage jobs in a highly reliable and scalable manner,
distribute them across workers, with retrials, inspectability and much more.  
It's a great choice for distributed, event driven systems with fallible units of work.

Some things I love about BullMQ:
- Leveraging the efficiency and reliability of Redis
- Retrial of jobs, with configured backoff
- Taking a look at failed and completed jobs, with logs and failure reason
- Ease of use

## Relation to BullMQ
This library can be used completely without a BullMQ instance. However, it works
exactly the same way, thereby using established, well tested
patterns and it also reuses the same lua of BullMQ to be executed on the
Redis server. It also ensures interoperability with BullMQ.
Projects having producers and workers in BullMQ (TypeScript / JavaScript) can slowly
migrate to Rust based BullRS producers and workers.

## BullRS
BullRS is async and builds on the tokio runtime. We always target interoperability
with the newest BullMQ version, but most things are expected to be backwards compatible
in both ways.

Priorities of our values:
- 1. Reliability
    - everything should work exactly as expected and no job should ever be dropped
- 2. Ease of use
    - beginner friendly, sensible defaults and hard to misuse
- 3. Performance
    - Minimize roundtrips to Redis

## Features
BullMQ has many features. The list below keeps track, which of them are yet to be imeplemented:

- Managing Jobs
    - [x] Adding immediate Jobs, LIFO and FIFO
    - [ ] Awaiting Job Results
    - [ ] Remove Jobs
    - [ ] Adding delayed Jobs
    - [ ] Adding priority Jobs
    - [ ] Repeatable Jobs
    - [ ] Job Hiearchy
- Worker
    - [x] Dequeue immediate Jobs
    - [x] Requeue stalled jobs (e.g. worker went offline during processing)
    - [ ] Retry jobs with backoff
    - [ ] Repeatable Jobs
    - [ ] Job Hiearchy
