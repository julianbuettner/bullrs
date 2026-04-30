use std::string::FromUtf8Error;

use croner::errors::CronError;
use deadpool_redis::PoolError;
use error_set::error_set;
use redis::{RedisError, Value};

use crate::{QueueName, redisext::RedisHashMapError};

error_set! {
    /// Unified error type for all queue operations. Allows using `?` across different calls.
    pub BullrsError := {
        /// Pause or resume failed.
        PauseResume(PauseResumeError),
        /// Adding a job failed.
        AddJob(AddJobErr),
        /// Adding a log entry failed.
        AddLog(AddLogError),
        /// Stalled-job recovery failed.
        MoveStalledToWait(MoveStalledToWaitError),
    }

    /// Error from obtaining job scheduler
    pub JobSchedulerError := {
        /// Failed to parse Cron from job stored in Redis
        #[display("cron parse error: {error} - \"{pattern}\"")]
        CronError {
            error: croner::errors::CronError,
            pattern: String,
        }
    } || BasicRedisError

    /// Error from pausing or resuming a queue.
    pub PauseResumeError := BasicRedisError

    /// Error from adding a job to a queue.
    pub AddJobErr := BasicAddError

    /// Error from adding a log line to a job.
    pub AddLogError := BasicJobNotFound || BasicRedisError

    /// Error from the stalled-job recovery process.
    pub MoveStalledToWaitError := BasicRedisError

    /// Error from moving a job to the active state.
    pub MoveToActiveErr := BasicRedisError || JobPayloadLoading

    /// Error from completing or failing a job.
    pub MoveToFinishedErr := {
        /// Missing key
        #[display("job has not been found")]
        MissingKey,
        /// Missing lock
        #[display("job lock doesn't exist (anymore?)")]
        MissingLock,
        /// Job not in active set
        #[display("job was not in the active set")]
        JobNotActive,
        /// Job has pending children
        #[display("job has pending children")]
        JobHasPendingChildren,
        /// Lock is not owned by this client
        #[display("job lock is owned by other worker")]
        LockNotOwned,
        /// Job has failed children
        #[display("job has failed children")]
        JobHasFailedChildren,
        /// Failed to serialize job result
        #[display("failed to serialize job result: {0:?}")]
        Serialize(serde_json::Error),
    } || BasicRedisError

    /// Error from obliterating a queue.
    pub ObliterateError := {
        /// Jobs are active, use force to overwrite anyways
        #[display("queue can only be obliterated when no job is active")]
        ActiveJobs,
        /// Queue is not paused
        #[display("queue can only be obliterated if paused")]
        NotPaused,
    } || BasicRedisError

    /// Error from updating job progress.
    pub UpdateProgressError := BasicJobNotFound || BasicRedisError

    /// Error from adding a job scheduler.
    pub AddJobSchedulerError := {
        /// A job with the computed ID already exists and could not be replaced.
        #[display("scheduler job ID collision")]
        JobIdCollision,
        /// Both the current and next time slot already have existing jobs.
        #[display("scheduler job slots busy")]
        JobSlotsBusy,
        /// Serialization of job data or options failed.
        #[display("failed to serialize: {0:?}")]
        SerializationFailed(serde_json::Error),
    } || BasicRedisError

    /// Error from removing a job scheduler.
    pub RemoveJobSchedulerError := {
        /// No scheduler with the given ID exists.
        #[display("job scheduler not found")]
        NotFound,
    } || BasicRedisError

    /// Error from checking whether a job has finished.
    pub IsFinishedError := BasicRedisError

    /// Error from awaiting a job's result via [`crate::JobJoinHandle::result`].
    pub JobAwaitError := {
        /// The worker called `failed()` on this job.
        #[display("job failed: {reason}")]
        #[allow(missing_docs)]
        JobFailed {
            reason: String,
        },
        /// The job key no longer exists in Redis. Indicates external removal
        /// (obliterated queue, manual deletion, or TTL expiry).
        #[display("job not found in Redis — it may have been removed externally")]
        JobNotFound,
        /// Return value could not be deserialized from JSON.
        #[display("failed to deserialize job return value: {0}")]
        Deserialize(serde_json::Error),
    } || BasicRedisError

    /// Low-level Redis or connection pool error.
    pub BasicRedisError := {
        /// Redis command error.
        #[display("redis error: {0}")]
        RedisError(RedisError),
        /// Connection pool error.
        #[display("redis pool error: {0}")]
        PoolError(PoolError),
    }

    /// Errors when serializing or enqueuing a job.
    BasicAddError := {
        /// Job payload could not be serialized to JSON.
        #[display("failed to serialize job payload to json: {0}")]
        SerializationFailed(serde_json::Error),
        /// Parent key was expected but missing.
        #[display("parent key is missing")]
        MissingParentKey,
        /// Scheduler job ID collision.
        #[display("scheduler job ID collision")]
        SchedulerJobIdCollision,
        /// Scheduler job slots busy.
        #[display("scheduler job slots busy")]
        SchedulerJobSlotsBusy,
    } || BasicRedisError

    /// The referenced job does not exist (anymore).
    BasicJobNotFound := {
        /// Job not found in the queue.
        #[display("job \"{job_id}\" in queue \"{}\" doesn't exist (anymore)", queue_name.as_str())]
        #[allow(missing_docs)]
        JobNotFound {
            job_id: String,
            queue_name: QueueName,
        },
    }

    /// Errors from loading job data out of Redis.
    JobPayloadLoading := {
        /// Hash map field extraction failed.
        #[display("failed to load job data (payload?): {0}")]
        RedisHashMapdisplay(RedisHashMapError),
        /// Value was not valid UTF-8.
        #[display("expected valid utf8-string from redis: {0:?}")]
        RedisStringInvalid(FromUtf8Error),
        /// Expected a hash map but got something else.
        #[display("lua job did not return hash map as expected: {value:?}")]
        #[allow(missing_docs)]
        UnexpectedRedisValue { value: Value },
        /// Lua script returned unexpected values.
        #[display("Unexpected lua script return values: {} {} {} - {:?}", v.1, v.2, v.3, v.0)]
        #[allow(missing_docs)]
        UnexpectedLuaOutput{ v: (Value, String, u64, i64) },
        /// Timestamp could not be interpreted.
        #[display("Bad timestamp: {ts}")]
        #[allow(missing_docs)]
        BadTimestamp{ ts: i64 },
    }
}
