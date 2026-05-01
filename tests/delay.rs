use bullrs::{JobOptions, WorkerArgs};
use ntest::timeout;
use std::time::{Duration, Instant};
use tokio::time::sleep;
mod setup;
use setup::*;

#[tokio::test]
#[test_log::test]
#[timeout(3_000)]
async fn redis_time_delayed() {
    let tq = setup::TestQueue::new("delayed");
    let q = &tq.queue;
    let mut w = q.worker(WorkerArgs::default());
    let start = Instant::now();
    q.add_with(
        "A",
        &Input { input: 11 },
        &JobOptions::builder()
            .delay(Duration::from_millis(10))
            .build(),
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(9) - start.elapsed()).await;
    assert!(
        !w.has_next(),
        "Job dequeued from worker after 9ms but delay was 10ms"
    );

    let j = w.next().await.expect("some").expect("ok");
    let diff = start.elapsed();
    assert!(diff > Duration::from_millis(10), "Duration was {diff:?}");
    assert!(
        // Even if Redis freezes for a bit and we are on a slow PC,
        // the given time should be enough. I think. If this is flakey, I will
        // Increase it significantly
        diff < Duration::from_millis(50),
        "Delay was 10ms, but received after {diff:?}"
    );

    assert_eq!(j.name(), "A");
    assert_eq!(j.data(), &Input { input: 11 });
}
