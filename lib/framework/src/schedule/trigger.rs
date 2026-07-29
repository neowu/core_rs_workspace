use std::time::Duration;

use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveTime;
use chrono::TimeDelta;

pub(super) enum Trigger {
    FixedRate(Duration),
    Daily(NaiveTime),
}

const INITIAL_DELAY: chrono::Duration = chrono::Duration::seconds(3);

impl Trigger {
    pub(super) fn next(&self, previous: DateTime<FixedOffset>, first: bool) -> DateTime<FixedOffset> {
        match self {
            Self::FixedRate(interval) => {
                if first {
                    previous + INITIAL_DELAY // initial delay
                } else {
                    previous + chrono::Duration::from_std(*interval).expect("input cannot be out of range")
                }
            }
            Self::Daily(time) => {
                let next_time = previous.with_time(*time).single().expect("fixed offset cannot be ambiguous");
                if next_time > previous {
                    next_time
                } else {
                    next_time.checked_add_signed(TimeDelta::days(1)).expect("result cannot be out of range")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::FixedOffset;
    use chrono::NaiveDate;
    use chrono::NaiveTime;
    use chrono::TimeZone as _;
    use chrono::Utc;

    use super::INITIAL_DELAY;
    use super::Trigger;

    fn utc() -> FixedOffset {
        FixedOffset::east_opt(0).unwrap()
    }

    fn east_8() -> FixedOffset {
        FixedOffset::east_opt(8 * 3600).unwrap()
    }

    #[test]
    fn fixed_rate_first_returns_previous() {
        let trigger = Trigger::FixedRate(Duration::from_mins(1));
        let previous = utc().with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, true), previous + INITIAL_DELAY);
    }

    #[test]
    fn fixed_rate_subsequent_adds_interval() {
        let trigger = Trigger::FixedRate(Duration::from_mins(1));
        let previous = utc().with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let expected = utc().with_ymd_and_hms(2026, 5, 13, 10, 1, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn fixed_rate_keeps_timezone() {
        let trigger = Trigger::FixedRate(Duration::from_mins(1));
        let previous = east_8().with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let next = trigger.next(previous, false);
        assert_eq!(*next.offset(), east_8(), "offset must be preserved");
        assert_eq!(next, east_8().with_ymd_and_hms(2026, 5, 13, 10, 1, 0).unwrap());
    }

    #[test]
    fn daily_before_target_returns_same_day() {
        let trigger = Trigger::Daily(NaiveTime::from_hms_opt(15, 0, 0).unwrap());
        let previous = utc().with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let expected = utc().with_ymd_and_hms(2026, 5, 13, 15, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_after_target_returns_next_day() {
        let trigger = Trigger::Daily(NaiveTime::from_hms_opt(9, 0, 0).unwrap());
        let previous = utc().with_ymd_and_hms(2026, 5, 13, 10, 0, 0).unwrap();
        let expected = utc().with_ymd_and_hms(2026, 5, 14, 9, 0, 0).unwrap();
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_respects_timezone() {
        // target 09:00 in +08:00 = 01:00 UTC; previous 08:00 local (= 00:00 UTC) is before target same day
        let trigger = Trigger::Daily(NaiveTime::from_hms_opt(9, 0, 0).unwrap());
        let previous = east_8().with_ymd_and_hms(2026, 5, 13, 8, 0, 0).unwrap();
        let next = trigger.next(previous, false);
        assert_eq!(next.to_utc(), Utc.with_ymd_and_hms(2026, 5, 13, 1, 0, 0).unwrap());
    }

    #[test]
    fn daily_returns_local_date() {
        // 01:00 in +08:00 is 17:00 UTC the previous day, the local date is what a job derives from
        let trigger = Trigger::Daily(NaiveTime::from_hms_opt(1, 0, 0).unwrap());
        let previous = east_8().with_ymd_and_hms(2026, 5, 13, 20, 0, 0).unwrap();
        let next = trigger.next(previous, false);
        assert_eq!(next, east_8().with_ymd_and_hms(2026, 5, 14, 1, 0, 0).unwrap());
        assert_eq!(next.date_naive(), NaiveDate::from_ymd_opt(2026, 5, 14).unwrap());
        assert_eq!(next.to_utc().date_naive(), NaiveDate::from_ymd_opt(2026, 5, 13).unwrap(), "utc date is a day behind");
    }
}
