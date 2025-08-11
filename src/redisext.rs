use serde::{Deserialize, de::DeserializeOwned};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisHashMapError {
    #[error("Value missing in key valu map from redis: {key}")]
    Missing { key: String },
    #[error("Could not deserialize json value: {0}")]
    Deserialize(#[from] serde_json::Error),
}

pub trait RedisHashMapExt {
    fn extract<T: DeserializeOwned>(&self, key: &str) -> Result<T, RedisHashMapError>;
    fn extract_opt<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, RedisHashMapError>;
}

impl RedisHashMapExt for HashMap<String, String> {
    fn extract<T: DeserializeOwned>(&self, key: &str) -> Result<T, RedisHashMapError> {
        match self.get(key) {
            None => Err(RedisHashMapError::Missing { key: key.into() }),
            Some(v) => Ok(serde_json::from_str(v)?),
        }
    }
    fn extract_opt<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, RedisHashMapError> {
        match self.get(key) {
            None => Ok(None),
            Some(v) => Ok(serde_json::from_str(v)?),
        }
    }
}
