use std::time::Duration;

use serde::Serialize;

use crate::RateLimit;

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct WireRateLimiter {
    pub max: usize,
    #[serde(with = "crate::milliserde::duration_millis")]
    pub duration: Duration,
}

impl From<&RateLimit> for WireRateLimiter {
    fn from(r: &RateLimit) -> Self {
        WireRateLimiter {
            max: r.max,
            duration: r.window,
        }
    }
}

impl From<RateLimit> for WireRateLimiter {
    fn from(r: RateLimit) -> Self {
        (&r).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_field_names_map_to_bullmq_wire_form() {
        let r = RateLimit {
            max: 100,
            window: Duration::from_secs(60),
        };
        let wire = WireRateLimiter::from(&r);
        assert_eq!(wire.max, 100);
        assert_eq!(wire.duration, Duration::from_secs(60));
    }

    #[test]
    fn rate_limit_owned_conversion_matches_borrowed() {
        let r = RateLimit {
            max: 5,
            window: Duration::from_millis(250),
        };
        let from_owned = WireRateLimiter::from(r);
        let from_ref = WireRateLimiter::from(&RateLimit {
            max: 5,
            window: Duration::from_millis(250),
        });
        assert_eq!(from_owned.max, from_ref.max);
        assert_eq!(from_owned.duration, from_ref.duration);
    }

    #[test]
    fn rate_limit_msgpack_encodes_duration_as_milliseconds() {
        let r = RateLimit {
            max: 2,
            window: Duration::from_millis(750),
        };
        let bytes = rmp_serde::to_vec_named(&WireRateLimiter::from(&r)).unwrap();
        #[derive(serde::Deserialize)]
        struct Probe {
            max: usize,
            duration: u64,
        }
        let probe: Probe = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(probe.max, 2);
        assert_eq!(probe.duration, 750);
    }
}
