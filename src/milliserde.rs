use chrono::{DateTime, Utc};
use serde::{self, Deserialize, Deserializer, Serializer};
use std::time::Duration;

/// De/serialize `Option<Duration>` as milliseconds:
/// ```
/// use serde::{Serialize, Deserialize};
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize)]
/// struct MyStruct {
///     #[serde(with = "bullrs::milliserde::duration_millis_option")]
///     dur: Option<Duration>,
/// }
/// ```
pub mod duration_millis_option {
    use std::time::Duration;

    use serde::{self, Deserialize, Deserializer, Serializer, ser};

    /// Serialize `Option<Duration>` as milliseconds via serde field attribute.
    /// `#[serde(deserialize_with = "bullrs::milliserde::duration_millis_option::serialize")]`
    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => {
                if d.as_millis() > u64::MAX as u128 {
                    return Err(<S::Error as ser::Error>::custom(
                        "Duration was too long, causing u64 millisecond overflow",
                    ));
                }
                serializer.serialize_u64(d.as_millis() as u64)
            }
            None => serializer.serialize_none(),
        }
    }

    /// Deserialize `Option<Duration>` as milliseconds via serde field attribute.
    /// `#[serde(deserialize_with = "bullrs::milliserde::duration_millis_option::deserialize")]`
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<u64>::deserialize(deserializer)?;
        Ok(opt.map(Duration::from_millis))
    }
}

/// De/serialize `Duration` as milliseconds:
/// ```
/// use serde::{Serialize, Deserialize};
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize)]
/// struct MyStruct {
///     #[serde(with = "bullrs::milliserde::duration_millis")]
///     dur: Duration,
/// }
/// ```
pub mod duration_millis {
    use super::*;

    /// Serialize `Duration` as milliseconds:
    /// `#[serde(with = "bullrs::milliserde::duration_millis::serialize")]`
    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    /// Deserialize `Duration` as milliseconds:
    /// `#[serde(with = "bullrs::milliserde::duration_millis::deserialize")]`
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

/// De/serialize `DateTime<Utc>` as milliseconds:
/// ```
/// use serde::{Serialize, Deserialize};
/// use chrono::{DateTime, Utc};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyStruct {
///     #[serde(with = "bullrs::milliserde::timestamp_millis")]
///     ts: DateTime<Utc>,
/// }
/// ```
pub mod timestamp_millis {
    use super::*;

    /// Serialize `DateTime<Utc>` as milliseconds:
    /// `#[serde(with = "bullrs::milliserde::timestamp_millis::serialize")]`
    pub fn serialize<S>(timestamp: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(timestamp.timestamp_millis())
    }

    /// Deserialize `DateTime<Utc>` as milliseconds:
    /// `#[serde(with = "bullrs::milliserde::timestamp_millis::deserialize")]`
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = i64::deserialize(deserializer)?;
        DateTime::from_timestamp_millis(millis)
            .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))
    }
}

/// De/serialize `Option<DateTime<Utc>>` as milliseconds:
/// ```
/// use serde::{Serialize, Deserialize};
/// use chrono::{DateTime, Utc};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyStruct {
///     #[serde(with = "bullrs::milliserde::timestamp_millis_option")]
///     ts: Option<DateTime<Utc>>,
/// }
/// ```
pub mod timestamp_millis_option {
    use super::*;

    /// Serialize `Option<DateTime<Utc>>` as milliseconds:
    /// `#[serde(with = "bullrs::milliserde::timestamp_millis_option::serialize")]`
    pub fn serialize<S>(timestamp: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match timestamp {
            Some(ts) => serializer.serialize_i64(ts.timestamp_millis()),
            None => serializer.serialize_none(),
        }
    }

    /// Deserialize `Option<DateTime<Utc>>` as milliseconds:
    /// `#[serde(with = "bullrs::milliserde::timestamp_millis_option::deserialize")]`
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
