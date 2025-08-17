use core::time;
use nanoid::nanoid;
use std::{
    cmp,
    fmt::Display,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{Date, DateTime, Utc};
use deadpool_redis::Pool;
use log::trace;
use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::to_string;
use tokio::{
    spawn,
    sync::{
        OwnedSemaphorePermit, Semaphore, SemaphorePermit,
        mpsc::{self, Receiver, Sender, channel},
        watch,
    },
    task::JoinHandle,
    time::sleep,
};

use crate::{
    job::LightJobHandle,
    luacommands::{
        InvokeLuaScript as _, MoveStalledJobsToWait, MoveToActive, MoveToActiveResult,
        MoveToActiveReturn, RateLimiter,
    },
    queue::QueueName,
};

pub async fn pull_job_thread<D, R>(
    pool: Pool,
    queue_name: QueueName,
    job_sender: Sender<LightJobHandle<D, R>>,
    semaphore: Arc<Semaphore>,
) where
    D: DeserializeOwned + std::fmt::Debug,
{
    let (marker_send, mut marker_recv) = mpsc::channel(1);
    spawn(pull_marker(pool.clone(), queue_name.clone(), marker_send));

    let worker_id = nanoid!();
    let mut counter: usize = 0;
    loop {
        println!("Semaphore");
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore crash");
        println!("...acquired. Getting connection {worker_id}.");
        let start = Instant::now();
        let mut con = pool.get().await.expect("TODO");
        println!(
            "...having connection {worker_id} after {:?}. Dequque.",
            start.elapsed()
        );
        let token = format!("{worker_id}-{counter}");
        counter += 1;
        let mts = MoveToActive::<D> {
            queue: &queue_name,
            worker_id: &worker_id,
            limiter: RateLimiter {
                max: 0,
                duration: Duration::from_millis(0),
            },
            lock_duration: Duration::from_secs(30),
            token: &token,
            phantom: PhantomData, // TODO without
        };
        println!("Dequeue what I can get");
        let get_job = mts.call(&mut con).await.unwrap();
        let sleep_timer =
            match get_job {
                MoveToActiveResult::JobData { id, data } => {
                    let lock_refresh_handle = tokio::spawn(lock_refresh());
                    job_sender
                        .send(LightJobHandle::new(
                            queue_name.clone(),
                            pool.clone(),
                            id,
                            permit,
                            data.data,
                            lock_refresh_handle,
                        ))
                        .await
                        .expect("TODO");
                    None
                }
                MoveToActiveResult::Delay { delay } => Some(delay),
                MoveToActiveResult::WaitUntil { timestamp } => Some(Duration::from_millis(
                    cmp::max(0, (timestamp - Utc::now()).num_milliseconds()) as u64,
                )),
                MoveToActiveResult::NothingToDo => Some(Duration::from_secs(10)),
            };
        if let Some(sleep_timer) = sleep_timer {
            println!("Got sleep: {:?}", sleep_timer);
            // Sleep until known job is ready, but also wake up if new job comes in
            let timeout = sleep(sleep_timer);
            let marker = marker_recv.recv();
            tokio::select! {
                t = timeout => println!("Classical timeout: {:?}",t),
                event = marker => {
                    let (member, score) = event.expect("TODO");
                    println!("EVENT: {}, {}", member, score);
                },
            };
        }
    }
}

async fn pull_marker(
    pool: Pool,
    queue_name: QueueName,
    sender: mpsc::Sender<(String, DateTime<Utc>)>,
) {
    let mut con = pool.get().await.expect("TODO");
    let marker_name = queue_name.marker();
    loop {
        let res: Option<(String, String, i64)> =
            con.bzpopmin(&marker_name, 30.).await.expect("TODO");
        if res.is_none() {
            continue;
        }
        let (_key, job_id, timestamp) = res.unwrap();
        let ts: DateTime<Utc> = DateTime::from_timestamp_millis(timestamp).expect("TODO");
        sender.send((job_id, ts)).await.expect("TODO");
    }
}
async fn lock_refresh() {}
