use std::time::Duration;

use serde::Serialize;

use crate::bullmq::rate_limiter::WireRateLimiter;

/// Msgpack-serialized options blob for `moveToFinished-14.lua`.
#[derive(Debug, Serialize)]
pub(crate) struct WireMoveToFinishedOpts {
    pub token: String,
    #[serde(rename = "keepJobs")]
    pub keep_jobs: WireKeepJobsConfig,
    #[serde(with = "crate::milliserde::duration_millis", rename = "lockDuration")]
    pub lock_duration: Duration,
    pub attempts: usize,
    #[serde(rename = "maxMetricsSize")]
    pub max_metrics_size: usize,
    #[serde(rename = "fpof")]
    pub fail_parent_on_fail: Option<bool>,
    #[serde(rename = "cpof")]
    pub continue_parent_on_failure: Option<bool>,
    #[serde(rename = "idof")]
    pub ignore_dependency_on_fail: Option<bool>,
    #[serde(rename = "rdof")]
    pub remove_dependency_on_fail: Option<bool>,
    pub name: String,
    pub limiter: Option<WireRateLimiter>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WireKeepJobsConfig {
    pub count: i64,
    #[serde(with = "crate::milliserde::duration_millis_option")]
    pub age: Option<Duration>,
}
