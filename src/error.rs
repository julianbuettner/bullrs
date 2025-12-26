use std::string::FromUtf8Error;

use deadpool_redis::PoolError;
use error_set::error_set;
use redis::{RedisError, Value};

use crate::{QueueName, redisext::RedisHashMapError};

error_set! {
    // Unifying Error so user can use the ?-operator easily
    pub BullrsError := {
        PauseResume(PauseResumeError),
        AddJob(AddJobErr),
        AddLog(AddLogError),
        MoveStalledToWait(MoveStalledToWaitError),
    }

    // Granular Errors of individual tasks
    pub PauseResumeError := BasicRedisError

    pub AddJobErr := BasicAddError

    pub AddLogError := BasicJobNotFound || BasicRedisError

    pub MoveStalledToWaitError := BasicRedisError

    pub MoveToActiveErr := BasicRedisError || JobPayloadLoading

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

    pub ObliterateError := {
        /// Jobs are active, use force to overwrite anyways
        #[display("queue can only be obliterated when no job is active")]
        ActiveJobs,
        /// Queue is not paused
        #[display("queue can only be obliterated if paused")]
        NotPaused,
    } || BasicRedisError

    pub UpdateProgressError := BasicJobNotFound || BasicRedisError

    pub BasicRedisError := {
        #[display("redis error: {0}")]
        RedisError(RedisError),
        #[display("redis pool error: {0}")]
        PoolError(PoolError),
    }

    BasicAddError := {
        #[display("failed to serialize job payload to json: {0}")]
        SerializationFailed(serde_json::Error),
        #[display("parent key is missing")]
        MissingParentKey,
    } || BasicRedisError

    BasicJobNotFound := {
        #[display("job \"{job_id}\" in queue \"{}\" doesn't exist (anymore)", queue_name.as_str())]
        JobNotFound {
            job_id: String,
            queue_name: QueueName,
        },
    }

    JobPayloadLoading := {
        #[display("failed to load job data (payload?): {0}")]
        RedisHashMapdisplay(RedisHashMapError),
        #[display("expected valid utf8-string from redis: {0:?}")]
        RedisStringInvalid(FromUtf8Error),
        #[display("lua job did not return hash map as expected: {value:?}")]
        UnexpectedRedisValue { value: Value },
        #[display("Unexpected lua script return values: {} {} {} - {:?}", v.1, v.2, v.3, v.0)]
        UnexpectedLuaOutput{ v: (Value, String, u64, i64) },
        #[display("Bad timestamp: {ts}")]
        BadTimestamp{ ts: i64 },
    }
}
