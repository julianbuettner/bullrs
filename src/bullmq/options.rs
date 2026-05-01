use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::job::{Backoff, JobOptions, ParentRef, Retain};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct WireJobOptions {
    pub attempts: Option<usize>,
    pub backoff: Option<WireBackoff>,
    #[serde(with = "crate::milliserde::duration_millis_option", default)]
    pub delay: Option<Duration>,
    pub job_id: Option<String>,
    #[serde(rename = "kl")]
    pub limit_logs: Option<usize>,
    pub lifo: Option<bool>,
    pub parent: Option<WireParent>,
    pub priority: Option<usize>,
    pub remove_on_complete: Option<WireKeepJobs>,
    pub remove_on_fail: Option<WireKeepJobs>,
    pub size_limit: Option<usize>,
    pub stack_trace_limit: Option<usize>,
    #[serde(with = "crate::milliserde::timestamp_millis_option", default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(rename = "cpof")]
    pub continue_parent_on_failure: Option<bool>,
    #[serde(rename = "de")]
    pub deduplication: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WireBackoff {
    #[serde(with = "crate::milliserde::duration_millis")]
    Number(Duration),
    BackoffOptions(WireBackoffOptions),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireBackoffOptions {
    #[serde(with = "crate::milliserde::duration_millis_option")]
    pub delay: Option<Duration>,
    pub r#type: WireBackoffType,
    pub jitter: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WireBackoffType {
    Exponential,
    Fixed,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WireKeepJobs {
    Count(usize),
    Bool(bool),
    Config {
        #[serde(with = "crate::milliserde::duration_millis_option")]
        age: Option<Duration>,
        count: Option<usize>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireParent {
    pub id: String,
    pub queue: String,
}

// ---------- Domain → Wire conversions ----------

impl From<&JobOptions> for WireJobOptions {
    fn from(o: &JobOptions) -> Self {
        WireJobOptions {
            attempts: o.attempts,
            backoff: o.backoff.as_ref().map(WireBackoff::from),
            delay: o.delay,
            job_id: o.job_id.clone(),
            limit_logs: o.limit_logs,
            lifo: o.lifo,
            parent: o.parent.as_ref().map(WireParent::from),
            priority: o.priority,
            remove_on_complete: o.remove_on_complete.as_ref().map(WireKeepJobs::from),
            remove_on_fail: o.remove_on_fail.as_ref().map(WireKeepJobs::from),
            size_limit: o.size_limit,
            stack_trace_limit: o.stack_trace_limit,
            timestamp: o.timestamp,
            continue_parent_on_failure: o.continue_parent_on_failure,
            deduplication: o.deduplication.clone(),
        }
    }
}

impl From<&Backoff> for WireBackoff {
    fn from(b: &Backoff) -> Self {
        match b {
            Backoff::Fixed { delay } => WireBackoff::BackoffOptions(WireBackoffOptions {
                delay: Some(*delay),
                r#type: WireBackoffType::Fixed,
                jitter: None,
            }),
            Backoff::Exponential { base, jitter } => {
                WireBackoff::BackoffOptions(WireBackoffOptions {
                    delay: Some(*base),
                    r#type: WireBackoffType::Exponential,
                    jitter: *jitter,
                })
            }
        }
    }
}

impl From<&Retain> for WireKeepJobs {
    fn from(r: &Retain) -> Self {
        match r {
            Retain::Forever => WireKeepJobs::Bool(true),
            Retain::Drop => WireKeepJobs::Bool(false),
            Retain::LastN(n) => WireKeepJobs::Count(*n),
            Retain::OlderThan(age) => WireKeepJobs::Config {
                age: Some(*age),
                count: None,
            },
            Retain::Both { count, age } => WireKeepJobs::Config {
                age: Some(*age),
                count: Some(*count),
            },
        }
    }
}

impl From<&ParentRef> for WireParent {
    fn from(p: &ParentRef) -> Self {
        WireParent {
            id: p.id.clone(),
            queue: p.queue.clone(),
        }
    }
}

// ---------- Wire → Domain conversions ----------

impl From<WireJobOptions> for JobOptions {
    fn from(w: WireJobOptions) -> Self {
        JobOptions {
            attempts: w.attempts,
            backoff: w.backoff.map(Backoff::from),
            delay: w.delay,
            job_id: w.job_id,
            limit_logs: w.limit_logs,
            lifo: w.lifo,
            parent: w.parent.map(ParentRef::from),
            priority: w.priority,
            remove_on_complete: w.remove_on_complete.map(Retain::from),
            remove_on_fail: w.remove_on_fail.map(Retain::from),
            size_limit: w.size_limit,
            stack_trace_limit: w.stack_trace_limit,
            timestamp: w.timestamp,
            continue_parent_on_failure: w.continue_parent_on_failure,
            deduplication: w.deduplication,
        }
    }
}

impl From<WireBackoff> for Backoff {
    fn from(b: WireBackoff) -> Self {
        match b {
            WireBackoff::Number(d) => Backoff::Fixed { delay: d },
            WireBackoff::BackoffOptions(o) => match o.r#type {
                WireBackoffType::Fixed => Backoff::Fixed {
                    delay: o.delay.unwrap_or_default(),
                },
                WireBackoffType::Exponential => Backoff::Exponential {
                    base: o.delay.unwrap_or_default(),
                    jitter: o.jitter,
                },
            },
        }
    }
}

impl From<WireKeepJobs> for Retain {
    fn from(w: WireKeepJobs) -> Self {
        match w {
            WireKeepJobs::Bool(true) => Retain::Forever,
            WireKeepJobs::Bool(false) => Retain::Drop,
            WireKeepJobs::Count(n) => Retain::LastN(n),
            WireKeepJobs::Config { age, count } => match (age, count) {
                (Some(age), Some(count)) => Retain::Both { age, count },
                (Some(age), None) => Retain::OlderThan(age),
                (None, Some(count)) => Retain::LastN(count),
                (None, None) => Retain::Forever,
            },
        }
    }
}

impl From<WireParent> for ParentRef {
    fn from(w: WireParent) -> Self {
        ParentRef {
            id: w.id,
            queue: w.queue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_fixed_round_trips() {
        let b = Backoff::Fixed {
            delay: Duration::from_millis(750),
        };
        let wire = WireBackoff::from(&b);
        match Backoff::from(wire) {
            Backoff::Fixed { delay } => assert_eq!(delay, Duration::from_millis(750)),
            other => panic!("expected Fixed, got {other:?}"),
        }
    }

    #[test]
    fn backoff_exponential_round_trips_with_jitter() {
        let b = Backoff::Exponential {
            base: Duration::from_secs(2),
            jitter: Some(0.25),
        };
        let wire = WireBackoff::from(&b);
        match Backoff::from(wire) {
            Backoff::Exponential { base, jitter } => {
                assert_eq!(base, Duration::from_secs(2));
                assert_eq!(jitter, Some(0.25));
            }
            other => panic!("expected Exponential, got {other:?}"),
        }
    }

    #[test]
    fn backoff_exponential_round_trips_without_jitter() {
        let b = Backoff::Exponential {
            base: Duration::from_secs(1),
            jitter: None,
        };
        match Backoff::from(WireBackoff::from(&b)) {
            Backoff::Exponential { jitter, .. } => assert_eq!(jitter, None),
            _ => panic!(),
        }
    }

    #[test]
    fn backoff_number_wire_form_decodes_as_fixed() {
        // BullMQ accepts a bare number for backoff; ensure we map it to Fixed.
        let b = Backoff::from(WireBackoff::Number(Duration::from_millis(500)));
        match b {
            Backoff::Fixed { delay } => assert_eq!(delay, Duration::from_millis(500)),
            _ => panic!(),
        }
    }

    #[test]
    fn retain_forever_maps_to_bool_true() {
        match WireKeepJobs::from(&Retain::Forever) {
            WireKeepJobs::Bool(b) => assert!(b),
            _ => panic!(),
        }
    }

    #[test]
    fn retain_drop_maps_to_bool_false() {
        match WireKeepJobs::from(&Retain::Drop) {
            WireKeepJobs::Bool(b) => assert!(!b),
            _ => panic!(),
        }
    }

    #[test]
    fn retain_last_n_maps_to_count() {
        match WireKeepJobs::from(&Retain::LastN(42)) {
            WireKeepJobs::Count(n) => assert_eq!(n, 42),
            _ => panic!(),
        }
    }

    #[test]
    fn retain_older_than_maps_to_config_age_only() {
        match WireKeepJobs::from(&Retain::OlderThan(Duration::from_secs(3600))) {
            WireKeepJobs::Config { age, count } => {
                assert_eq!(age, Some(Duration::from_secs(3600)));
                assert_eq!(count, None);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn retain_both_maps_to_config_with_count_and_age() {
        match WireKeepJobs::from(&Retain::Both {
            count: 10,
            age: Duration::from_secs(60),
        }) {
            WireKeepJobs::Config { age, count } => {
                assert_eq!(age, Some(Duration::from_secs(60)));
                assert_eq!(count, Some(10));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn keep_jobs_config_with_neither_age_nor_count_falls_back_to_forever() {
        match Retain::from(WireKeepJobs::Config {
            age: None,
            count: None,
        }) {
            Retain::Forever => {}
            _ => panic!(),
        }
    }

    #[test]
    fn parent_ref_round_trips() {
        let p = ParentRef {
            id: "p1".into(),
            queue: "bull:q:wait".into(),
        };
        let wire = WireParent::from(&p);
        let back: ParentRef = wire.into();
        assert_eq!(back.id, "p1");
        assert_eq!(back.queue, "bull:q:wait");
    }

    #[test]
    fn job_options_round_trips_through_wire() {
        let opts = JobOptions {
            attempts: Some(3),
            delay: Some(Duration::from_millis(200)),
            job_id: Some("job-7".into()),
            limit_logs: Some(99),
            lifo: Some(true),
            priority: Some(5),
            size_limit: Some(1024),
            stack_trace_limit: Some(8),
            continue_parent_on_failure: Some(true),
            deduplication: Some("dedup-key".into()),
            ..Default::default()
        };
        let back: JobOptions = WireJobOptions::from(&opts).into();
        assert_eq!(back.attempts, Some(3));
        assert_eq!(back.delay, Some(Duration::from_millis(200)));
        assert_eq!(back.job_id.as_deref(), Some("job-7"));
        assert_eq!(back.limit_logs, Some(99));
        assert_eq!(back.lifo, Some(true));
        assert_eq!(back.priority, Some(5));
        assert_eq!(back.size_limit, Some(1024));
        assert_eq!(back.stack_trace_limit, Some(8));
        assert_eq!(back.continue_parent_on_failure, Some(true));
        assert_eq!(back.deduplication.as_deref(), Some("dedup-key"));
    }

    /// Mirrors a subset of BullMQ's on-the-wire field names. If our serde
    /// renames regress, these fields fail to deserialize.
    #[derive(Debug, Default, Deserialize)]
    struct WireProbe {
        kl: Option<usize>,
        cpof: Option<bool>,
        de: Option<String>,
        delay: Option<u64>,
        priority: Option<usize>,
    }

    #[test]
    fn job_options_msgpack_uses_bullmq_field_renames() {
        let opts = JobOptions {
            limit_logs: Some(7),
            continue_parent_on_failure: Some(false),
            deduplication: Some("k".into()),
            ..Default::default()
        };
        let bytes = rmp_serde::to_vec_named(&WireJobOptions::from(&opts)).unwrap();
        let probe: WireProbe = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(probe.kl, Some(7), "expected `kl` rename for limit_logs");
        assert_eq!(probe.cpof, Some(false), "expected `cpof` rename");
        assert_eq!(probe.de.as_deref(), Some("k"), "expected `de` rename");
    }

    #[test]
    fn job_options_msgpack_encodes_durations_as_milliseconds() {
        let opts = JobOptions {
            delay: Some(Duration::from_millis(1234)),
            priority: Some(7),
            ..Default::default()
        };
        let bytes = rmp_serde::to_vec_named(&WireJobOptions::from(&opts)).unwrap();
        let probe: WireProbe = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(probe.delay, Some(1234));
        assert_eq!(probe.priority, Some(7));
    }
}
