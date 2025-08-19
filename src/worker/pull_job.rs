use log::trace;
use nanoid::nanoid;
use std::{
    cmp,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use deadpool_redis::Pool;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use tokio::{
    spawn,
    sync::{
        Semaphore,
        mpsc::{self, Sender},
    },
    time::sleep,
};

use crate::{
    job::JobWorkHandle,
    luacommands::{InvokeLuaScript as _, MoveToActive, MoveToActiveResult, RateLimiter},
    queue::QueueName,
};

pub async fn pull_job_thread<D, R>(
    pool: Pool,
    queue_name: QueueName,
    job_sender: Sender<JobWorkHandle<D, R>>,
    semaphore: Arc<Semaphore>,
) where
    D: DeserializeOwned + std::fmt::Debug,
{
    let (marker_send, mut marker_recv) = mpsc::channel(1);
    spawn(pull_marker(pool.clone(), queue_name.clone(), marker_send));

    let worker_id = nanoid!();
    let mut counter: usize = 0;
    loop {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore crash");
        let start = Instant::now();
        let mut con = pool.get().await.expect("TODO");
        trace!(
            "Worker thread {worker_id} acquired connection after {:?}",
            start.elapsed()
        );
        let lock_token = format!("{worker_id}-{counter}");
        counter += 1;
        let mts = MoveToActive::<D> {
            queue: &queue_name,
            worker_id: &worker_id,
            limiter: RateLimiter {
                max: 0,
                duration: Duration::from_millis(0),
            },
            lock_duration: Duration::from_secs(30),
            token: &lock_token,
            phantom: PhantomData, // TODO without
        };
        let get_job = mts.call(&mut con).await.unwrap();
        let sleep_timer =
            match get_job {
                MoveToActiveResult::JobData { id, data } => {
                    let lock_refresh_handle = tokio::spawn(lock_refresh());
                    trace!(
                        "Worker thread {worker_id} fetched job {id} from queue {}.",
                        queue_name.as_str()
                    );
                    job_sender
                        .send(JobWorkHandle::new(
                            queue_name.clone(),
                            pool.clone(),
                            id,
                            data.name,
                            permit,
                            data.data,
                            lock_refresh_handle,
                            lock_token.clone(),
                            worker_id.clone(),
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
            trace!(
                "Worker thread {worker_id} equeued nothing, sleep for {:?}",
                sleep_timer
            );
            // Sleep until known job is ready, but also wake up if new job comes in
            let timeout = sleep(sleep_timer);
            let marker = marker_recv.recv();
            tokio::select! {
                _ = timeout => (),
                event = marker => {
                    let (_member, _score) = event.expect("TODO");
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
        if let Err(e) = sender.send((job_id, ts)).await {
            // Pull thread terminated and can't receive updates
            return;
        }
    }
}
async fn lock_refresh() {}
