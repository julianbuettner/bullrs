use bullrs::{JobOptions, Repeat, SchedulerId, SchedulerTemplate, SchedulerWindow, WorkerArgs};
use ntest::timeout;
use std::{str::FromStr, time::{Duration, Instant}};
use tokio::time::sleep;
mod setup;
use setup::*;

#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn redis_scheduler_every() {
    let tq = setup::TestQueue::new("scheduler-every");
    let q = &tq.queue;
    let mut w = q.worker(WorkerArgs::default());

    let repeat = Repeat::Every {
        interval: Duration::from_millis(500),
        offset: None,
    };
    let id = SchedulerId::try_new("tick").unwrap();

    q.upsert_job_scheduler(
        &id,
        &repeat,
        &SchedulerWindow::default(),
        SchedulerTemplate {
            name: "tick",
            data: &Input { input: 42 },
            opts: &JobOptions::default(),
        },
    )
    .await
    .unwrap();

    // BullMQ "every" without a start date fires the first job immediately and
    // produces subsequent jobs every `interval`. We measure the gap between
    // consecutive jobs.
    let j1 = w.next().await.expect("some").expect("ok");
    let t1 = Instant::now();
    assert_eq!(j1.name(), "tick");
    assert_eq!(j1.data(), &Input { input: 42 });
    j1.done(&Output { output: 1 }).await.unwrap();

    let j2 = w.next().await.expect("some").expect("ok");
    let gap = t1.elapsed();
    assert_eq!(j2.name(), "tick");
    assert!(
        gap >= Duration::from_millis(450),
        "Second job arrived too early: gap was {gap:?}"
    );
    j2.done(&Output { output: 2 }).await.unwrap();
}

#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn redis_scheduler_remove() {
    let tq = setup::TestQueue::new("scheduler-remove");
    let q = &tq.queue;
    let mut w = q.worker(WorkerArgs::default());

    let repeat = Repeat::Every {
        interval: Duration::from_millis(200),
        offset: None,
    };
    let id = SchedulerId::try_new("rapid").unwrap();

    q.upsert_job_scheduler(
        &id,
        &repeat,
        &SchedulerWindow::default(),
        SchedulerTemplate {
            name: "rapid",
            data: &Input { input: 1 },
            opts: &JobOptions::default(),
        },
    )
    .await
    .unwrap();

    let j1 = w.next().await.expect("some").expect("ok");
    j1.done(&Output { output: 1 }).await.unwrap();

    q.remove_job_scheduler(&id).await.unwrap();

    sleep(Duration::from_millis(400)).await;
    assert!(
        !w.has_next(),
        "Scheduler produced a job after it was removed"
    );
}

#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn redis_scheduler_limit() {
    let tq = setup::TestQueue::new("scheduler-limit");
    let q = &tq.queue;
    let mut w = q.worker(WorkerArgs::default());

    let repeat = Repeat::Every {
        interval: Duration::from_millis(200),
        offset: None,
    };
    let id = SchedulerId::try_new("limited").unwrap();
    let window = SchedulerWindow {
        limit: Some(3),
        ..Default::default()
    };

    q.upsert_job_scheduler(
        &id,
        &repeat,
        &window,
        SchedulerTemplate {
            name: "limited",
            data: &Input { input: 1 },
            opts: &JobOptions::default(),
        },
    )
    .await
    .unwrap();

    let mut count = 0;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        if w.has_next() {
            let job = w.next().await.expect("some").expect("ok");
            count += 1;
            job.done(&Output { output: count as i64 }).await.unwrap();
        } else {
            sleep(Duration::from_millis(50)).await;
        }
    }

    assert_eq!(
        count, 3,
        "Expected exactly 3 jobs from limited scheduler, got {count}"
    );
}

#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn redis_scheduler_cron_every_second() {
    let tq = setup::TestQueue::new("scheduler-cron");
    let q = &tq.queue;
    let mut w = q.worker(WorkerArgs::default());

    let repeat = Repeat::Cron {
        pattern: croner::Cron::from_str("*/1 * * * * *").unwrap(),
        tz: None,
    };
    let id = SchedulerId::try_new("cron-tick").unwrap();
    let window = SchedulerWindow {
        limit: Some(2),
        ..Default::default()
    };

    q.upsert_job_scheduler(
        &id,
        &repeat,
        &window,
        SchedulerTemplate {
            name: "cron-tick",
            data: &Input { input: 99 },
            opts: &JobOptions::default(),
        },
    )
    .await
    .unwrap();

    let j1 = w.next().await.expect("some").expect("ok");
    assert_eq!(j1.name(), "cron-tick");
    j1.done(&Output { output: 1 }).await.unwrap();

    let j2 = w.next().await.expect("some").expect("ok");
    assert_eq!(j2.name(), "cron-tick");
    j2.done(&Output { output: 2 }).await.unwrap();
}
