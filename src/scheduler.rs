use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Repeat options compatible with BullMQ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepeatOptions {
    /// Cron pattern, e.g. "0 15 3 * * *" (optional seconds).
    pub pattern: Option<String>,
    /// Fixed interval in milliseconds (alternative to `pattern`).
    pub every: Option<u64>,
    /// IANA timezone for cron evaluation (e.g. "Europe/Berlin").
    pub tz: Option<String>,
    /// Earliest timestamp when the scheduler may start producing jobs.
    #[serde(
        with = "crate::milliserde::timestamp_millis_option",
        rename = "startDate",
        default
    )]
    pub start_date: Option<DateTime<Utc>>,
    /// Latest timestamp when the scheduler may produce jobs.
    #[serde(
        with = "crate::milliserde::timestamp_millis_option",
        rename = "endDate",
        default
    )]
    pub end_date: Option<DateTime<Utc>>,
    /// Maximum number of jobs to produce.
    pub limit: Option<u64>,
    /// Offset in ms for `every` schedules.
    pub offset: Option<i64>,
    /// Produce first job immediately, regardless of schedule alignment.
    pub immediately: Option<bool>,
}

/// Template for jobs produced by a scheduler.
#[derive(Debug, Serialize)]
pub struct JobSchedulerTemplate<'a, D> {
    /// Job name used for every produced job.
    pub name: &'a str,
    /// Job data used for every produced job.
    pub data: &'a D,
    /// Job options used for every produced job.
    pub opts: &'a crate::JobOptions,
}

/// Information about an existing job scheduler, returned by `get_job_schedulers`.
#[derive(Debug, Deserialize)]
pub struct JobSchedulerInfo {
    /// Scheduler id (the key in the repeat zset).
    pub id: String,
    /// Name template.
    pub name: Option<String>,
    /// Timezone for cron evaluation.
    pub tz: Option<String>,
    /// Cron pattern.
    pub pattern: Option<String>,
    /// Fixed interval in milliseconds.
    pub every: Option<u64>,
    /// End date as millisecond timestamp.
    pub end_date: Option<i64>,
    /// Start date as millisecond timestamp.
    pub start_date: Option<i64>,
    /// Maximum number of jobs to produce.
    pub limit: Option<u64>,
    /// Offset in milliseconds.
    pub offset: Option<i64>,
    /// Number of iterations produced so far.
    pub iteration_count: Option<u64>,
    /// Next scheduled fire time (score in the repeat zset).
    pub next_millis: i64,
}

/// Compute the next occurrence of a cron pattern after `now`.
///
/// Uses the `croner` crate with optional seconds support, matching BullMQ's
/// cron-parser format ("unix cron w/ optional seconds").
pub fn compute_cron_next_millis(
    pattern: &str,
    tz_name: Option<&str>,
    now: DateTime<Utc>,
) -> Result<i64, String> {
    use chrono_tz::Tz;
    use croner::parser::{CronParser, Seconds};

    let parser = CronParser::builder().seconds(Seconds::Optional).build();
    let schedule = parser.parse(pattern).map_err(|e| format!("cron parse: {e:?}"))?;

    let next = if let Some(tz_name) = tz_name {
        let tz: Tz = tz_name.parse().map_err(|e| format!("tz parse: {e:?}"))?;
        let local_now = now.with_timezone(&tz);
        schedule
            .find_next_occurrence(&local_now, false)
            .map_err(|e| format!("cron find_next_occurrence: {e:?}"))?
            .with_timezone(&Utc)
    } else {
        schedule
            .find_next_occurrence(&now, false)
            .map_err(|e| format!("cron find_next_occurrence: {e:?}"))?
    };

    Ok(next.timestamp_millis())
}

/// Convenience helper to compute `next_millis` for a [`RepeatOptions`].
///
/// - For `every`: returns `now + every` as a starting point (the Lua script
///   realigns it to the proper slot).
/// - For `pattern`: uses `compute_cron_next_millis`.
pub fn compute_next_millis(
    repeat: &RepeatOptions,
    now: DateTime<Utc>,
) -> Result<i64, String> {
    if let Some(every) = repeat.every {
        Ok(now.timestamp_millis() + every as i64)
    } else if let Some(ref pattern) = repeat.pattern {
        compute_cron_next_millis(pattern, repeat.tz.as_deref(), now)
    } else {
        Err("repeat options must specify either `every` or `pattern`".into())
    }
}

