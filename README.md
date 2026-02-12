# BullRS

A BullMQ compatible message queue for highly reliable job processing.

## State of this project

> [!WARNING]  
> This project is not production ready. It has incomplete error handling, documentation and is constantly refactored.

The project currently only implements the most basic features of BullMQ and is much less tested.
Also the API is expected to change over the next few versions.

## Why use BullRS or BullMQ
BullMQ and BullRS use Redis to manage jobs in a highly reliable and scalable manner.
Distribute jobs across workers, with retrials, inspecting logs and much more.  
It's a great choice for distributed, event driven systems with fallible units of work.

## Relation to BullMQ
This library can be used completely without a BullMQ instance. However, it works
exactly the same way, thereby using established, well tested
patterns and it also uses the lua script of BullMQ to be executed on the
Redis server. This way it ensures interoperability with BullMQ.
Projects having producers and workers in BullMQ (TypeScript / JavaScript) can slowly
migrate to Rust based BullRS producers and workers.

## BullRS
BullRS is async and builds on the tokio runtime. I always target interoperability
with the newest BullMQ version, but usually different BullMQ worker/producers are compatible
across many versions.

Priorities:
- 1. Reliability
    - everything should work exactly as expected and no job should ever be lost
- 2. Ease of use
    - beginner friendly, sensible defaults and hard to misuse API
- 3. Performance
    - reduce round trips, maximize concurrence

## Features
BullMQ has many features. The list below keeps track, which of them are yet to be imeplemented:

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
