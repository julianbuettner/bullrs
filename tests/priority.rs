mod setup;
use bullrs::{JobOptions, WorkerArgs};
use setup::*;

#[tokio::test]
#[test_log::test]
async fn redis_priority() {
    let tq = setup::TestQueue::new("priority");
    let q = &tq.queue;
    q.add_with(
        "A",
        &Input { input: 11 },
        &JobOptions::builder().priority(90).build(),
    )
    .await
    .unwrap();
    q.add_with(
        "B1",
        &Input { input: 20 },
        &JobOptions::builder().priority(10).build(),
    )
    .await
    .unwrap();

    q.add_with(
        "B2",
        &Input { input: 21 },
        &JobOptions::builder().priority(10).build(),
    )
    .await
    .unwrap();
    q.add("C", &Input { input: 33 }).await.unwrap();

    let mut w = q.worker(WorkerArgs::default());
    let job = w.next().await.unwrap();
    assert_eq!(job.name(), "C");
    assert_eq!(job.data().input, 33);

    // Maintain FIFO for same priority
    let job = w.next().await.unwrap();
    assert_eq!(job.name(), "B1");
    assert_eq!(job.data().input, 20);
    let job = w.next().await.unwrap();
    assert_eq!(job.name(), "B2");
    assert_eq!(job.data().input, 21);

    let job = w.next().await.unwrap();
    assert_eq!(job.name(), "A");
    assert_eq!(job.data().input, 11);
}
