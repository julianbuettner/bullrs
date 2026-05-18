// If a job is dropped without `done()` or `failed()` called, it should
// not wait for the lock to expire but being marked as stalled quickly.
#[allow(dead_code)]
pub async fn dropped_to_failed() {}
