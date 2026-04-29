use serde::{Deserialize, Serialize};

use crate::scheduler::{Repeat, SchedulerWindow};

/// Wire shape for the scheduler-opts msgpack blob passed as ARGV[2] to
/// `addJobScheduler-11.lua`. Stored under the `bull:<q>:repeat:<id>` hash.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireSchedulerOpts {
    pub name: String,
    pub tz: Option<String>,
    pub pattern: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<i64>,
    pub every: Option<u64>,
    pub offset: Option<i64>,
    #[serde(rename = "startDate")]
    pub start_date: Option<i64>,
    pub limit: Option<u64>,
}

impl WireSchedulerOpts {
    pub fn from_domain(name: &str, repeat: &Repeat, window: &SchedulerWindow) -> Self {
        let (pattern, every, tz, offset) = match repeat {
            Repeat::Every { interval, offset } => (
                None,
                Some(interval.as_millis() as u64),
                None,
                offset.map(|d| d.as_millis() as i64),
            ),
            Repeat::Cron { pattern, tz } => (
                Some(pattern.clone()),
                None,
                tz.as_ref().map(|t| t.name().to_string()),
                None,
            ),
        };
        WireSchedulerOpts {
            name: name.to_string(),
            tz,
            pattern,
            end_date: window.end.map(|dt| dt.timestamp_millis()),
            every,
            offset,
            start_date: window.start.map(|dt| dt.timestamp_millis()),
            limit: window.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::Europe;
    use std::time::Duration;

    #[test]
    fn every_repeat_sets_only_every_and_offset() {
        let repeat = Repeat::Every {
            interval: Duration::from_millis(500),
            offset: Some(Duration::from_millis(50)),
        };
        let opts = WireSchedulerOpts::from_domain("ticker", &repeat, &SchedulerWindow::default());
        assert_eq!(opts.every, Some(500));
        assert_eq!(opts.offset, Some(50));
        assert_eq!(opts.pattern, None);
        assert_eq!(opts.tz, None);
    }

    #[test]
    fn cron_repeat_sets_pattern_and_tz_name() {
        let repeat = Repeat::Cron {
            pattern: "0 9 * * *".into(),
            tz: Some(Europe::Berlin),
        };
        let opts = WireSchedulerOpts::from_domain("daily", &repeat, &SchedulerWindow::default());
        assert_eq!(opts.pattern.as_deref(), Some("0 9 * * *"));
        assert_eq!(opts.tz.as_deref(), Some("Europe/Berlin"));
        assert_eq!(opts.every, None);
        assert_eq!(opts.offset, None);
    }

    #[test]
    fn name_is_taken_from_argument_not_repeat() {
        let repeat = Repeat::Every {
            interval: Duration::from_secs(1),
            offset: None,
        };
        let opts =
            WireSchedulerOpts::from_domain("custom-name", &repeat, &SchedulerWindow::default());
        assert_eq!(opts.name, "custom-name");
    }

    #[test]
    fn window_dates_serialize_as_millisecond_timestamps() {
        let start = chrono::Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
        let end = chrono::Utc.timestamp_millis_opt(1_700_001_000_000).unwrap();
        let window = SchedulerWindow {
            start: Some(start),
            end: Some(end),
            limit: Some(5),
            immediately: None,
        };
        let opts = WireSchedulerOpts::from_domain(
            "x",
            &Repeat::Every {
                interval: Duration::from_secs(1),
                offset: None,
            },
            &window,
        );
        assert_eq!(opts.start_date, Some(1_700_000_000_000));
        assert_eq!(opts.end_date, Some(1_700_001_000_000));
        assert_eq!(opts.limit, Some(5));
    }

    #[test]
    fn empty_window_yields_no_dates_or_limit() {
        let opts = WireSchedulerOpts::from_domain(
            "x",
            &Repeat::Every {
                interval: Duration::from_secs(1),
                offset: None,
            },
            &SchedulerWindow::default(),
        );
        assert_eq!(opts.start_date, None);
        assert_eq!(opts.end_date, None);
        assert_eq!(opts.limit, None);
    }

    #[test]
    fn wire_opts_msgpack_round_trip_preserves_camelcase_field_names() {
        let opts = WireSchedulerOpts::from_domain(
            "n",
            &Repeat::Every {
                interval: Duration::from_secs(1),
                offset: None,
            },
            &SchedulerWindow {
                start: Some(chrono::Utc.timestamp_millis_opt(1).unwrap()),
                end: Some(chrono::Utc.timestamp_millis_opt(2).unwrap()),
                limit: Some(1),
                immediately: None,
            },
        );
        let bytes = rmp_serde::to_vec_named(&opts).unwrap();

        // BullMQ Lua reads `startDate` / `endDate` (camelCase). If the rename
        // attrs regress, these fields don't deserialize.
        #[derive(Deserialize)]
        struct Probe {
            #[serde(rename = "startDate")]
            start_date: Option<i64>,
            #[serde(rename = "endDate")]
            end_date: Option<i64>,
        }
        let probe: Probe = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(probe.start_date, Some(1));
        assert_eq!(probe.end_date, Some(2));
    }
}
