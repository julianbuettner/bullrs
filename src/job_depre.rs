use std::{collections::HashMap, time::SystemTime};

use chrono::{DateTime, Duration, Utc};
use redis::{FromRedisValue, from_redis_value};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub struct JobId(pub String);

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct JobOptions {
    attempts: Option<usize>,
    // #[serde(with = "duration_millis_option")]
    // delay: Option<Duration>,
}

/// All information and parameters, that are required
/// to schedule a job.
#[derive(Debug)]
pub struct JobScheduling<P> {
    pub name: String,
    pub data: P,
    pub priority: Option<usize>,
}

// All "trivial" information a job can have.
// "Trivial" excludes bigger data like logs.
#[derive(Debug)]
pub struct JobState<D, R, P = String, F = String> {
    pub name: String,
    pub data: D,
    pub progress: Option<P>,
    pub result: Option<R>,
    pub priority: Option<usize>,
    pub timestamp: DateTime<Utc>,
    pub processed_on: Option<DateTime<Utc>>,
    pub delay: Option<Duration>,
    pub opts: Option<JobOptions>,
    pub failed_reason: Option<F>,
    // Moved from active to stalled to ready
    pub stc: Option<usize>,
}

impl<D, R, P> FromRedisValue for JobState<D, R, P>
where
    D: DeserializeOwned,
    P: DeserializeOwned,
    R: DeserializeOwned,
{
    fn from_redis_value(item: &redis::Value) -> redis::RedisResult<Self> {
        let hm: HashMap<String, String> = from_redis_value(item)?;
        let ts: Option<i64> = hm
            .get("timestamp")
            .map(|v| serde_json::from_str(v))
            .transpose()?;
        let po: Option<i64> = hm
            .get("processedOn")
            .map(|v| serde_json::from_str(v))
            .transpose()?;
        let delay: Option<i64> = hm
            .get("delay")
            .map(|v| serde_json::from_str(v))
            .transpose()?;
        let priority: Option<usize> = hm
            .get("priority")
            .map(|v| serde_json::from_str(v))
            .transpose()?;
        let opts = hm
            .get("opts")
            .map(|o| serde_json::from_str(&o))
            .transpose()?;
        let data = serde_json::from_str(hm.get("data").unwrap())?;
        Ok(Self {
            name: hm.get("name").unwrap().into(),
            data,
            result: hm
                .get("result")
                .map(|v| serde_json::from_str(v))
                .transpose()?,
            progress: hm
                .get("progress")
                .map(|v| serde_json::from_str(v))
                .transpose()?,
            failed_reason: hm
                .get("failedReason")
                .map(|v| serde_json::from_str(v))
                .transpose()?,
            opts,
            timestamp: ts.map(DateTime::from_timestamp_millis).flatten().unwrap(),
            processed_on: po.map(DateTime::from_timestamp_millis).flatten(),
            delay: delay.map(Duration::milliseconds),
            priority,
        })
    }
}

mod duration_millis_option {
    use chrono::Duration;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => serializer.serialize_some(&d.num_milliseconds()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<i64>::deserialize(deserializer)?;
        Ok(opt.map(Duration::milliseconds))
    }
}
