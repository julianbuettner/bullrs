use deadpool_redis::PoolError;
use redis::RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("something happened on the Redis protocol side: {0}")]
    Redis(#[from] RedisError),
    #[error("worker terminated due to failure; check if error was previously emitted")]
    AlreadyTerminated,
    /// The redis connection pool was closed from the outside of the worker.
    /// This is bad, because jobs keep a reference to the worker as well
    /// and can't store their results like they could in graceful termination.
    #[error("redis connection pool was closed")]
    PoolClosed,
    #[error("Some other pool error happened: {0}")]
    PoolError(#[from] PoolError),
}
