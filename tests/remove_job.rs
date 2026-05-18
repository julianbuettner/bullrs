use std::time::Duration;

use bullrs::{
    JobOptions, RemoveJobError, Repeat, Retain, SchedulerId, SchedulerTemplate, SchedulerWindow,
    WorkerArgs,
};
use ntest::timeout;

mod setup;
use setup::*;

/// A removed waiting job is skipped by the worker.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_waiting_job() {
    let tq = TestQueue::new("remove-waiting");
    let q = &tq.queue;

    let first = q.add("first", &Input { input: 1 }).await.unwrap();
    let first_id = first.id().to_owned();
    q.add("second", &Input { input: 2 }).await.unwrap();

    q.remove_job(&first_id).await.unwrap();

    let mut w = q.worker(WorkerArgs::default());
    let job = w.next().await.unwrap().unwrap();
    assert_eq!(job.name(), "second");
    assert_eq!(job.data(), &Input { input: 2 });
    job.done(&Output { output: 2 }).await.unwrap();
}

/// `JobJoinHandle::remove` removes a waiting job.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_via_handle() {
    let tq = TestQueue::new("remove-handle");
    let q = &tq.queue;

    let handle = q.add("to-remove", &Input { input: 1 }).await.unwrap();
    q.add("to-keep", &Input { input: 2 }).await.unwrap();

    handle.remove().await.unwrap();

    let mut w = q.worker(WorkerArgs::default());
    let job = w.next().await.unwrap().unwrap();
    assert_eq!(job.name(), "to-keep");
    job.done(&Output { output: 2 }).await.unwrap();
}

/// An active (locked) job cannot be removed.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_active_job_returns_locked() {
    let tq = TestQueue::new("remove-active");
    let q = &tq.queue;

    q.add("work", &Input { input: 1 }).await.unwrap();

    let mut w = q.worker(WorkerArgs::default());
    let job = w.next().await.unwrap().unwrap();
    let job_id = job.id().to_owned();

    // Job is now active and locked — removal must fail.
    let err = q.remove_job(&job_id).await.unwrap_err();
    assert!(
        matches!(err, RemoveJobError::JobLocked),
        "expected JobLocked, got: {err:?}"
    );

    job.done(&Output { output: 1 }).await.unwrap();
}

/// A completed job that was retained can be removed.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_completed_job() {
    let tq = TestQueue::new("remove-completed");
    let q = &tq.queue;

    let handle = q
        .add_with(
            "work",
            &Input { input: 1 },
            &JobOptions::builder()
                .remove_on_complete(Retain::Forever)
                .build(),
        )
        .await
        .unwrap();
    let job_id = handle.id().to_owned();

    let mut w = q.worker(WorkerArgs::default());
    let job = w.next().await.unwrap().unwrap();
    job.done(&Output { output: 1 }).await.unwrap();

    // Wait for the completed event so the job hash exists in Redis.
    handle.result().await.unwrap();

    // Completed job is retained and can now be removed.
    q.remove_job(&job_id).await.unwrap();
}

/// Removing a non-existent job ID is a no-op (idempotent).
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_nonexistent_job_is_noop() {
    let tq = TestQueue::new("remove-noop");
    let q = &tq.queue;

    // No jobs have been added — removal of a phantom ID must succeed.
    q.remove_job("9999").await.unwrap();
}

/// Removing a job produced by a scheduler returns `IsSchedulerJob`.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_scheduler_job_returns_is_scheduler_job() {
    let tq = TestQueue::new("remove-scheduler");
    let q = &tq.queue;

    let id = SchedulerId::try_new("test-sched").unwrap();
    let ok = q
        .upsert_job_scheduler(
            &id,
            &Repeat::Every {
                interval: Duration::from_secs(60),
                offset: None,
            },
            &SchedulerWindow::default(),
            SchedulerTemplate {
                name: "tick",
                data: &Input { input: 1 },
                opts: &JobOptions::default(),
            },
        )
        .await
        .unwrap();

    // The scheduler created a pending next job; removing it directly must fail.
    let err = q.remove_job(&ok.job_id).await.unwrap_err();
    assert!(
        matches!(err, RemoveJobError::IsSchedulerJob),
        "expected IsSchedulerJob, got: {err:?}"
    );
}

/// `remove_job_with_children` works for jobs that have no children.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn remove_job_with_children_childless() {
    let tq = TestQueue::new("remove-children-noop");
    let q = &tq.queue;

    let handle = q.add("work", &Input { input: 1 }).await.unwrap();
    let job_id = handle.id().to_owned();

    q.remove_job_with_children(&job_id).await.unwrap();

    // Job is gone — worker finds nothing.
    let mut w = q.worker(WorkerArgs::default());
    q.add("sentinel", &Input { input: 99 }).await.unwrap();
    let job = w.next().await.unwrap().unwrap();
    assert_eq!(job.name(), "sentinel");
    job.done(&Output { output: 99 }).await.unwrap();
}
