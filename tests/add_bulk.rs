use std::time::Duration;

use bullrs::{JobOptions, Retain, WorkerArgs};
use ntest::timeout;

mod setup;
use setup::*;

/// Bulk-added jobs arrive in the order they were submitted.
#[tokio::test]
#[test_log::test]
#[timeout(10_000)]
async fn add_bulk_preserves_order() {
    let tq = TestQueue::new("bulk-order");
    let q = &tq.queue;

    let opts = JobOptions::default();
    let jobs = vec![
        ("first", &Input { input: 1 }, &opts),
        ("second", &Input { input: 2 }, &opts),
        ("third", &Input { input: 3 }, &opts),
    ];

    let handles = q.add_bulk(&jobs).await.unwrap();
    assert_eq!(handles.len(), 3);

    let mut w = q.worker(WorkerArgs::default());
    for expected in [1i64, 2, 3] {
        let job = w.next().await.unwrap().unwrap();
        assert_eq!(job.data().input, expected);
        job.done(&Output { output: expected }).await.unwrap();
    }
}

/// An empty slice is a no-op and returns an empty vec.
#[tokio::test]
#[test_log::test]
#[timeout(5_000)]
async fn add_bulk_empty_is_noop() {
    let tq = TestQueue::new("bulk-empty");
    let q = &tq.queue;

    let handles: Vec<_> = q
        .add_bulk(&Vec::<(&str, &Input, &JobOptions)>::new())
        .await
        .unwrap();

    assert!(handles.is_empty());
}

/// Returned handles can be used to wait for job results.
#[tokio::test]
#[test_log::test]
#[timeout(10_000)]
async fn add_bulk_handles_usable() {
    let tq = TestQueue::new("bulk-handles");
    let q = &tq.queue;

    let retain = JobOptions::builder()
        .remove_on_complete(Retain::Forever)
        .build();
    let opts = JobOptions::default();

    let jobs = vec![
        ("a", &Input { input: 10 }, &retain),
        ("b", &Input { input: 20 }, &opts),
    ];
    let handles = q.add_bulk(&jobs).await.unwrap();
    let mut handles_iter = handles.into_iter();
    let handle_a = handles_iter.next().unwrap();
    let _handle_b = handles_iter.next().unwrap();

    let mut w = q.worker(WorkerArgs::default());

    // Process job "a"
    let job = w.next().await.unwrap().unwrap();
    assert_eq!(job.data().input, 10);
    job.done(&Output { output: 10 }).await.unwrap();

    // handle_a.result() resolves after the job completes
    let result = handle_a.result().await.unwrap();
    assert_eq!(result.output, 10);

    // Process job "b"
    let job = w.next().await.unwrap().unwrap();
    job.done(&Output { output: 20 }).await.unwrap();
}

/// Mix of standard and delayed jobs enqueued in one bulk call.
#[tokio::test]
#[test_log::test]
#[timeout(10_000)]
async fn add_bulk_mixed_options() {
    let tq = TestQueue::new("bulk-mixed");
    let q = &tq.queue;

    let standard = JobOptions::default();
    let delayed = JobOptions::builder()
        .delay(Duration::from_millis(100))
        .build();

    let jobs = vec![
        ("immediate", &Input { input: 1 }, &standard),
        ("later", &Input { input: 2 }, &delayed),
        ("also-immediate", &Input { input: 3 }, &standard),
    ];

    let handles = q.add_bulk(&jobs).await.unwrap();
    assert_eq!(handles.len(), 3);

    // The two immediate jobs should be processable right away.
    let mut w = q.worker(WorkerArgs::default());

    let job = w.next().await.unwrap().unwrap();
    assert_eq!(job.name(), "immediate");
    job.done(&Output { output: 1 }).await.unwrap();

    let job = w.next().await.unwrap().unwrap();
    assert_eq!(job.name(), "also-immediate");
    job.done(&Output { output: 3 }).await.unwrap();
}
