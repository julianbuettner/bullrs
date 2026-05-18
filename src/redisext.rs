use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisHashMapError {
    #[error("Value missing in key value map from redis: {key}")]
    Missing { key: String },
    #[error("Could not deserialize json value: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("Timestamp {key} was out of range: {ts}")]
    TimeStampOutOfRange { key: String, ts: i64 },
}

#[allow(dead_code)]
pub trait RedisHashMapExt {
    fn get_v(&self, key: &str) -> Result<String, RedisHashMapError>;
    fn extract<T: DeserializeOwned>(&self, key: &str) -> Result<T, RedisHashMapError>;
    fn extract_opt<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, RedisHashMapError>;
    fn extract_timestamp_ms(&self, key: &str) -> Result<DateTime<Utc>, RedisHashMapError>;
    fn extract_duration_ms_opt(&self, key: &str) -> Result<Option<Duration>, RedisHashMapError>;
}

impl RedisHashMapExt for HashMap<String, String> {
    fn get_v(&self, key: &str) -> Result<String, RedisHashMapError> {
        match self.get(key) {
            None => Err(RedisHashMapError::Missing { key: key.into() }),
            Some(v) => Ok(v.clone()),
        }
    }
    fn extract<T: DeserializeOwned>(&self, key: &str) -> Result<T, RedisHashMapError> {
        match self.get(key) {
            None => Err(RedisHashMapError::Missing { key: key.into() }),
            Some(v) => Ok(serde_json::from_str(v)?),
        }
    }
    fn extract_opt<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, RedisHashMapError> {
        match self.get(key) {
            None => Ok(None),
            Some(v) => Ok(serde_json::from_str(v)?),
        }
    }
    fn extract_timestamp_ms(&self, key: &str) -> Result<DateTime<Utc>, RedisHashMapError> {
        let ts: i64 = self.extract(key)?;
        match DateTime::from_timestamp_millis(ts) {
            Some(dt) => Ok(dt),
            None => Err(RedisHashMapError::TimeStampOutOfRange {
                key: key.into(),
                ts,
            }),
        }
    }
    fn extract_duration_ms_opt(&self, key: &str) -> Result<Option<Duration>, RedisHashMapError> {
        let ms: Option<u64> = self.extract_opt(key)?;
        Ok(ms.map(Duration::from_millis))
    }
}
