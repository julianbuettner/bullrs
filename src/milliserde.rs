use chrono::{DateTime, Utc};
use serde::{self, Deserialize, Deserializer, Serializer};
use std::time::Duration;

pub mod duration_millis_option {
    use std::time::Duration;

    use serde::{self, Deserialize, Deserializer, Serializer, ser};

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => {
                if (d.as_millis() > u64::MAX as u128) {
                    return Err(<S::Error as ser::Error>::custom(
                        "Duration was too long, causing u64 millisecond overflow",
                    ));
                }
                serializer.serialize_u64(d.as_millis() as u64)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<u64>::deserialize(deserializer)?;
        Ok(opt.map(Duration::from_millis))
    }
}

pub mod duration_millis {
    use super::*;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

pub mod timestamp_millis {
    use super::*;

    pub fn serialize<S>(timestamp: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(timestamp.timestamp_millis())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = i64::deserialize(deserializer)?;
        DateTime::from_timestamp_millis(millis)
            .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))
    }
}

pub mod timestamp_millis_option {
    use super::*;

    pub fn serialize<S>(timestamp: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match timestamp {
            Some(ts) => serializer.serialize_i64(ts.timestamp_millis()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<i64>::deserialize(deserializer)?;
        match opt {
            Some(millis) => {
                let timestamp = DateTime::from_timestamp_millis(millis)
                    .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))?;
                Ok(Some(timestamp))
            }
            None => Ok(None),
        }
    }
}
