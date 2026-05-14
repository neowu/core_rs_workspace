use std::time::Duration;

use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveTime;
use chrono::TimeDelta;
use chrono::Utc;

pub(super) enum Trigger {
    FixedRate(Duration),
    Daily { time_zone: FixedOffset, time: NaiveTime },
}

impl Trigger {
    pub(super) fn next(&self, previous: DateTime<Utc>, first: bool) -> DateTime<Utc> {
        match self {
            Self::FixedRate(interval) => {
                if first {
                    previous
                } else {
                    previous + chrono::Duration::from_std(*interval).expect("input cannot be out of range")
                }
            }
            Self::Daily { time_zone, time } => {
                let next_time = previous.with_timezone(time_zone).with_time(*time).unwrap();
                if next_time > previous {
                    next_time.to_utc()
                } else {
                    next_time.checked_add_signed(TimeDelta::days(1)).expect("result cannot be out of range").to_utc()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::FixedOffset;
    use chrono::NaiveTime;
    use chrono::TimeZone as _;
    use chrono::Utc;

    use super::Trigger;

    #[test]
    fn fixed_rate_first_returns_previous() {
        let trigger = Trigger::FixedRate(Duration::from_mins(1));
        let previous = Utc.with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, true), previous);
    }

    #[test]
    fn fixed_rate_subsequent_adds_interval() {
        let trigger = Trigger::FixedRate(Duration::from_mins(1));
        let previous = Utc.with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 5, 13, 10, 1, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_before_target_returns_same_day() {
        let trigger = Trigger::Daily {
            time_zone: FixedOffset::east_opt(0).unwrap(),
            time: NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
        };
        let previous = Utc.with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 5, 13, 15, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_after_target_returns_next_day() {
        let trigger = Trigger::Daily {
            time_zone: FixedOffset::east_opt(0).unwrap(),
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        };
        let previous = Utc.with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 5, 14, 9, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_respects_timezone() {
        // target 09:00 in +08:00 = 01:00 UTC; previous 00:00 UTC (= 08:00 local) is before target same day
        let trigger = Trigger::Daily {
            time_zone: FixedOffset::east_opt(8 * 3600).unwrap(),
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        };
        let previous = Utc.with_ymd_and_hms(2026, 5, 13, 0, 0, 0).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 5, 13, 1, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }
}
