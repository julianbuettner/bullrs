use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use nutype::nutype;
use thiserror::Error;

use crate::JobOptions;

/// Unique identifier for a job scheduler.
#[nutype(
    validate(not_empty, predicate = |s: &str| !s.contains(":")),
    sanitize(trim),
    derive(AsRef, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Display, Serialize, Deserialize)
)]
pub struct SchedulerId(String);

/// Repetition rule for a job scheduler.
#[derive(Debug, Clone)]
pub enum Repeat {
    /// Fire at a fixed interval. Optional `offset` shifts the alignment.
    Every {
        /// Distance between consecutive firings.
        interval: Duration,
        /// Optional offset added to the alignment slot.
        offset: Option<Duration>,
    },
    /// Fire on a cron schedule (optional seconds field). When `tz` is
    /// `Some`, the cron expression is evaluated in that timezone.
    Cron {
        /// Cron pattern (Unix cron, optional seconds field).
        pattern: Cron,
        /// Timezone to evaluate the cron expression in (defaults to UTC).
        tz: Option<Tz>,
    },
}

/// Optional scheduler bounds: when it should start, end, and how many jobs
/// it may produce in total.
#[derive(Debug, Default, Clone)]
pub struct SchedulerWindow {
    /// Earliest fire time. Defaults to "now" when `None`.
    pub start: Option<DateTime<Utc>>,
    /// Latest fire time. After this point, no further jobs are produced.
    pub end: Option<DateTime<Utc>>,
    /// Maximum total number of jobs to produce.
    pub limit: Option<u64>,
    /// Produce the first job immediately, regardless of schedule alignment.
    pub immediately: Option<bool>,
}

/// Template for jobs produced by a scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerTemplate<'a, D> {
    /// Job name applied to every produced job.
    pub name: &'a str,
    /// Job data applied to every produced job.
    pub data: &'a D,
    /// Job options applied to every produced job.
    pub opts: &'a JobOptions,
}

/// A snapshot of an existing scheduler, returned by
/// [`Queue::get_job_schedulers`](crate::Queue::get_job_schedulers).
#[derive(Debug, Clone)]
pub struct SchedulerInfo {
    /// Scheduler ID.
    pub id: SchedulerId,
    /// Job-name template (the `name` of every produced job).
    pub name: Option<String>,
    /// Repetition rule.
    pub repeat: Option<Repeat>,
    /// Optional scheduler window.
    pub window: SchedulerWindow,
    /// Number of iterations produced so far.
    pub iteration_count: Option<u64>,
    /// Next scheduled fire time.
    pub next_fire: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub(crate) enum CronError {
    #[error("cron parse: {0}")]
    Parse(String),
    #[error("cron next-occurrence lookup: {0}")]
    Next(String),
}

/// Compute the next occurrence of a cron pattern after `now`.
pub(crate) fn compute_cron_next_millis(
    schedule: &Cron,
    tz: Option<Tz>,
    now: DateTime<Utc>,
) -> Result<i64, CronError> {
    use croner::parser::{CronParser, Seconds};

    let next = if let Some(tz) = tz {
        let local_now = now.with_timezone(&tz);
        schedule
            .find_next_occurrence(&local_now, false)
            .map_err(|e| CronError::Next(format!("{e:?}")))?
            .with_timezone(&Utc)
    } else {
        schedule
            .find_next_occurrence(&now, false)
            .map_err(|e| CronError::Next(format!("{e:?}")))?
    };

    Ok(next.timestamp_millis())
}

/// Compute `next_millis` for the given [`Repeat`] rule.
///
/// For `Every` schedules, the Lua script realigns the value to the proper slot,
/// so any monotonically-increasing value is acceptable.
pub(crate) fn compute_next_millis(repeat: &Repeat, now: DateTime<Utc>) -> Result<i64, CronError> {
    match repeat {
        Repeat::Every { interval, .. } => Ok(now.timestamp_millis() + interval.as_millis() as i64),
        Repeat::Cron { pattern, tz } => compute_cron_next_millis(pattern, *tz, now),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use chrono::TimeZone;
    use chrono_tz::Europe;

    #[test]
    fn scheduler_id_rejects_empty() {
        assert!(SchedulerId::try_new("").is_err());
        assert!(SchedulerId::try_new("   ").is_err());
    }

    #[test]
    fn scheduler_id_rejects_colon() {
        assert!(SchedulerId::try_new("foo:bar").is_err());
    }

    #[test]
    fn scheduler_id_trims_whitespace() {
        let id = SchedulerId::try_new("  daily  ").unwrap();
        assert_eq!(id.as_ref(), "daily");
    }

    #[test]
    fn scheduler_id_accepts_dashes_and_dots() {
        assert!(SchedulerId::try_new("daily-report.v2").is_ok());
    }

    #[test]
    fn compute_next_millis_every_adds_interval() {
        let now = Utc.timestamp_millis_opt(1_000_000).unwrap();
        let repeat = Repeat::Every {
            interval: Duration::from_millis(500),
            offset: None,
        };
        let next = compute_next_millis(&repeat, now).unwrap();
        assert_eq!(next, 1_000_500);
    }

    #[test]
    fn compute_next_millis_every_ignores_offset_field() {
        // Lua realigns; the Rust value just needs to be in the future.
        let now = Utc.timestamp_millis_opt(0).unwrap();
        let repeat = Repeat::Every {
            interval: Duration::from_secs(1),
            offset: Some(Duration::from_secs(123)),
        };
        let next = compute_next_millis(&repeat, now).unwrap();
        assert_eq!(next, 1_000);
    }

    #[test]
    fn compute_cron_next_millis_advances_to_next_minute() {
        // 12:34:00 UTC → next "* * * * *" minute boundary is 12:35:00.
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 34, 0).unwrap();
        let next_ms =
            compute_cron_next_millis(&Cron::from_str("* * * * *").unwrap(), None, now).unwrap();
        let next = Utc.timestamp_millis_opt(next_ms).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 12, 35, 0).unwrap());
    }

    #[test]
    fn compute_cron_next_millis_supports_optional_seconds_field() {
        // "*/2 * * * * *" = every 2 seconds.
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap();
        let next_ms =
            compute_cron_next_millis(&Cron::from_str("*/2 * * * * *").unwrap(), None, now).unwrap();
        let next = Utc.timestamp_millis_opt(next_ms).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 2).unwrap());
    }

    #[test]
    fn compute_cron_next_millis_evaluates_in_tz() {
        // "0 9 * * *" daily 09:00 in Berlin (CET, UTC+1 in January) = 08:00 UTC.
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next_ms = compute_cron_next_millis(
            &Cron::from_str("0 9 * * *").unwrap(),
            Some(Europe::Berlin),
            now,
        )
        .unwrap();
        let next = Utc.timestamp_millis_opt(next_ms).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap());
    }
}
