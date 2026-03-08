use dashmap::DashMap;
use std::{
    cmp,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{Instrument, Level, debug, info, span, trace, warn};

use chrono::{DateTime, Utc};
use deadpool_redis::{Pool, PoolError};
use redis::{AsyncCommands, RedisError};
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
    luacommands::{InvokeLuaScript as _, MoveToActive, MoveToActiveOk, RateLimiter},
    queue::QueueName,
    worker::{
        lock_refresh::lock_refresh, shutdown_switch::ShutdownSwitch, workererror::WorkerError,
    },
};

pub async fn pull_job_thread<D, R>(
    pool: Pool,
    queue_name: QueueName,
    shutdown_switch: ShutdownSwitch,
    pulling_switch: ShutdownSwitch,
    job_sender: Sender<Result<JobWorkHandle<D, R>, WorkerError>>,
    semaphore: Arc<Semaphore>,
    failure_cooldown: Duration,
    pull_worker_id: String,
) where
    D: DeserializeOwned + std::fmt::Debug,
{
    let (marker_send, mut marker_recv) = mpsc::channel(1);
    let poll_span = span!(Level::TRACE, "poll-marker");
    spawn(poll_marker(pool.clone(), queue_name.clone(), marker_send).instrument(poll_span));

    let lock_duration = Duration::from_secs(30);
    let refresh_lock_map = Arc::new(DashMap::new());
    let refresh_span = span!(Level::TRACE, "refresh-lock");
    spawn(
        lock_refresh(
            pool.clone(),
            queue_name.clone(),
            refresh_lock_map.clone(),
            shutdown_switch.clone(),
            lock_duration,
        )
        .instrument(refresh_span),
    );

    let mut counter: usize = 0;
    while shutdown_switch.running() && pulling_switch.running() {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let start = Instant::now();
        let con = pool.get().await;
        if let Err(e) = con {
            warn!("Failed to get Redis connection: {}", e);
            let fatal = match &e {
                &PoolError::Closed => true,
                _ => false,
            };
            if job_sender.send(Err(e.into())).await.is_err() {
                // Received is closed anyways. Terminate.
                warn!("Receiver dropped, terminate");
                return;
            }
            if fatal {
                warn!("Error is fatal, terminate");
                return;
            }
            sleep(failure_cooldown).await;
            continue;
        }
        let mut con = con.unwrap();
        trace!("Acquired connection after {:?}", start.elapsed());
        let lock_token: Arc<str> = format!("{pull_worker_id}-{counter}").into();
        counter += 1;
        let mts = MoveToActive::<D> {
            queue: &queue_name,
            worker_id: &pull_worker_id,
            limiter: RateLimiter {
                max: 0,
                duration: Duration::from_millis(0),
            },
            lock_duration,
            token: &lock_token,
            phantom: PhantomData, // TODO without
        };
        let get_job = mts.call(&mut con).await.unwrap();
        let sleep_timer: Option<Duration> = match get_job {
            MoveToActiveOk::JobData { id, data } => {
                info!(
                    "Fetched job {id}, preload channel has capacity of {}",
                    job_sender.capacity()
                );
                // refresh_lock_map is maintained by lock_refresh.rs
                refresh_lock_map.insert(id.clone(), Arc::downgrade(&lock_token));
                let _closed = job_sender
                    .send(Ok(JobWorkHandle::new(
                        queue_name.clone(),
                        pool.clone(),
                        id,
                        data.name,
                        permit,
                        data.data,
                        lock_token,
                        pull_worker_id.clone(),
                    )))
                    .await
                    .is_err();
                // TODO move back to waiting if `closed`
                None
            }
            MoveToActiveOk::Delay { delay } => Some(delay),
            MoveToActiveOk::WaitUntil { timestamp } => {
                Some(Duration::from_millis(
                    cmp::max(0, (timestamp - Utc::now()).num_milliseconds()) as u64,
                ))
            }
            MoveToActiveOk::NothingToDo => Some(Duration::from_secs(10)),
        };
        if let Some(sleep_timer) = sleep_timer {
            trace!(
                "Nothing in queue, sleep for {:?} or until marker event",
                sleep_timer
            );
            // Sleep until known job is ready, but also wake up if new job comes in
            let timeout = sleep(sleep_timer);
            let marker = marker_recv.recv();
            tokio::select! {
                _ = timeout => (),
                event = marker => {
                    let (job_id, ts) = event.expect("poll never terminates first");
                    debug!("Received marker {} {}", job_id, ts);
                },
            };
        }
    }
    debug!("Terminated gracefully");
}

async fn poll_marker(
    pool: Pool,
    queue_name: QueueName,
    sender: mpsc::Sender<(String, DateTime<Utc>)>,
) {
    let marker_name = queue_name.marker();
    loop {
        if sender.is_closed() {
            // Receiver is dropped, terminate gracefully
            info!("Terminate gracefully");
            return;
        }
        let con = pool.get().await;
        if let Err(e) = con {
            warn!("Marker poll could not get connection from pool: {:?}", e);
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        let mut con = con.unwrap();
        let res: Result<Option<(String, String, i64)>, RedisError> =
            con.bzpopmin(&marker_name, 30.).await;
        if let Err(e) = res {
            warn!("Marker poll failed to get next timestamp: {:?}", e);
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        let res = res.unwrap();
        if res.is_none() {
            continue;
        }
        let (_key, job_id, timestamp) = res.unwrap();
        let ts: Option<DateTime<Utc>> = DateTime::from_timestamp_millis(timestamp);
        if ts.is_none() {
            warn!("Marker poll failed to parse next timestamp: {}", timestamp);
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        let ts = ts.unwrap();
        if let Err(_e) = sender.send((job_id, ts)).await {
            // Pull thread terminated and can't receive updates
            break;
        }
    }
    debug!("Terminate gracefully");
}
