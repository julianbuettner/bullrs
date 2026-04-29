use bullrs::{JobOptions, RepeatOptions, WorkerArgs};
use ntest::timeout;
use std::time::{Duration, Instant};
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

    let start = Instant::now();
    let repeat = RepeatOptions {
        pattern: None,
        every: Some(500),
        tz: None,
        start_date: None,
        end_date: None,
        limit: None,
        offset: None,
        immediately: None,
    };

    q.upsert_job_scheduler("tick", &repeat, bullrs::JobSchedulerTemplate {
        name: "tick",
        data: &Input { input: 42 },
        opts: &JobOptions::default(),
    })
    .await
    .unwrap();

    // First job
    let j1 = w.next().await.expect("some").expect("ok");
    assert_eq!(j1.name(), "tick");
    assert_eq!(j1.data(), &Input { input: 42 });
    let elapsed1 = start.elapsed();
    assert!(
        elapsed1 >= Duration::from_millis(450),
        "First job arrived too early: {elapsed1:?}"
    );
    j1.done(&Output { output: 1 }).await.unwrap();

    // Second job
    let j2 = w.next().await.expect("some").expect("ok");
    assert_eq!(j2.name(), "tick");
    let elapsed2 = start.elapsed();
    assert!(
        elapsed2 >= Duration::from_millis(950),
        "Second job arrived too early: {elapsed2:?}"
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

    let repeat = RepeatOptions {
        pattern: None,
        every: Some(200),
        tz: None,
        start_date: None,
        end_date: None,
        limit: None,
        offset: None,
        immediately: None,
    };

    q.upsert_job_scheduler("rapid", &repeat, bullrs::JobSchedulerTemplate {
        name: "rapid",
        data: &Input { input: 1 },
        opts: &JobOptions::default(),
    })
    .await
    .unwrap();

    // Get first job
    let j1 = w.next().await.expect("some").expect("ok");
    j1.done(&Output { output: 1 }).await.unwrap();

    // Remove scheduler before next job arrives
    q.remove_job_scheduler("rapid").await.unwrap();

    // Wait a bit longer than the interval
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

    let repeat = RepeatOptions {
        pattern: None,
        every: Some(200),
        tz: None,
        start_date: None,
        end_date: None,
        limit: Some(3),
        offset: None,
        immediately: None,
    };

    q.upsert_job_scheduler("limited", &repeat, bullrs::JobSchedulerTemplate {
        name: "limited",
        data: &Input { input: 1 },
        opts: &JobOptions::default(),
    })
    .await
    .unwrap();

    let mut count = 0;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(Ok(job)) = w.next().await {
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

    let repeat = RepeatOptions {
        pattern: Some("*/1 * * * * *".into()),
        every: None,
        tz: None,
        start_date: None,
        end_date: None,
        limit: Some(2),
        offset: None,
        immediately: None,
    };

    q.upsert_job_scheduler("cron-tick", &repeat, bullrs::JobSchedulerTemplate {
        name: "cron-tick",
        data: &Input { input: 99 },
        opts: &JobOptions::default(),
    })
    .await
    .unwrap();

    // First job should appear within ~1s
    let j1 = w.next().await.expect("some").expect("ok");
    assert_eq!(j1.name(), "cron-tick");
    j1.done(&Output { output: 1 }).await.unwrap();

    // Second job within ~2s total
    let j2 = w.next().await.expect("some").expect("ok");
    assert_eq!(j2.name(), "cron-tick");
    j2.done(&Output { output: 2 }).await.unwrap();
}
