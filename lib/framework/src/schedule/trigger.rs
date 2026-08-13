use crate::date::DateTime;
use crate::date::SignedDuration;
use crate::date::Time;

pub(super) enum Trigger {
    FixedRate(SignedDuration),
    Daily(Time),
}

const INITIAL_DELAY: SignedDuration = SignedDuration::from_secs(3);
// the scheduler timezone is a fixed offset, so a day is always 24 hours
const ONE_DAY: SignedDuration = SignedDuration::from_hours(24);

impl Trigger {
    pub(super) fn next(&self, previous: DateTime, first: bool) -> DateTime {
        match self {
            Self::FixedRate(interval) => {
                if first {
                    previous.add_duration(INITIAL_DELAY).expect("duration must be in range") // initial delay
                } else {
                    previous.add_duration(*interval).expect("duration must be in range")
                }
            }
            Self::Daily(time) => {
                let next_time = previous.with_time(*time);
                if next_time > previous {
                    next_time
                } else {
                    next_time.add_duration(ONE_DAY).expect("duration must be in range")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::INITIAL_DELAY;
    use super::Trigger;
    use crate::date::Date;
    use crate::date::DateTime;
    use crate::date::Offset;
    use crate::date::SignedDuration;
    use crate::date::Time;

    const EAST_8: Offset = Offset::new(8, 0);

    // DateTime::new() is utc, noon keeps the local date on the same day for any offset within +/-12,
    // so replacing the time afterwards yields the wanted local date time
    fn date_time(offset: Offset, date: Date, time: Time) -> DateTime {
        DateTime::new(date, Time::new(12, 0, 0)).with_timezone(offset).with_time(time)
    }

    #[test]
    fn fixed_rate_first_returns_previous() {
        let trigger = Trigger::FixedRate(SignedDuration::from_mins(1));
        let previous = date_time(Offset::UTC, Date::new(2026, 5, 13), Time::new(10, 0, 0));
        assert_eq!(trigger.next(previous, true), previous.add_duration(INITIAL_DELAY).unwrap());
    }

    #[test]
    fn fixed_rate_subsequent_adds_interval() {
        let trigger = Trigger::FixedRate(SignedDuration::from_mins(1));
        let previous = date_time(Offset::UTC, Date::new(2026, 5, 13), Time::new(10, 0, 0));
        let expected = date_time(Offset::UTC, Date::new(2026, 5, 13), Time::new(10, 1, 0));
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn fixed_rate_keeps_timezone() {
        let trigger = Trigger::FixedRate(SignedDuration::from_mins(1));
        let previous = date_time(EAST_8, Date::new(2026, 5, 13), Time::new(10, 0, 0));
        let next = trigger.next(previous, false);
        assert_eq!(next.timezone(), EAST_8, "offset must be preserved");
        assert_eq!(next, date_time(EAST_8, Date::new(2026, 5, 13), Time::new(10, 1, 0)));
    }

    #[test]
    fn daily_before_target_returns_same_day() {
        let trigger = Trigger::Daily(Time::new(15, 0, 0));
        let previous = date_time(Offset::UTC, Date::new(2026, 5, 13), Time::new(10, 0, 0));
        let expected = date_time(Offset::UTC, Date::new(2026, 5, 13), Time::new(15, 0, 0));
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_after_target_returns_next_day() {
        let trigger = Trigger::Daily(Time::new(9, 0, 0));
        let previous = date_time(Offset::UTC, Date::new(2026, 5, 13), Time::new(10, 0, 0));
        let expected = date_time(Offset::UTC, Date::new(2026, 5, 14), Time::new(9, 0, 0));
        assert_eq!(trigger.next(previous, false), expected);
    }

    #[test]
    fn daily_respects_timezone() {
        // target 09:00 in +08:00 = 01:00 UTC; previous 08:00 local (= 00:00 UTC) is before target same day
        let trigger = Trigger::Daily(Time::new(9, 0, 0));
        let previous = date_time(EAST_8, Date::new(2026, 5, 13), Time::new(8, 0, 0));
        let next = trigger.next(previous, false);
        assert_eq!(next, DateTime::new(Date::new(2026, 5, 13), Time::new(1, 0, 0)));
    }

    #[test]
    fn daily_returns_local_date() {
        // 01:00 in +08:00 is 17:00 UTC the previous day, the local date is what a job derives from
        let trigger = Trigger::Daily(Time::new(1, 0, 0));
        let previous = date_time(EAST_8, Date::new(2026, 5, 13), Time::new(20, 0, 0));
        let next = trigger.next(previous, false);
        assert_eq!(next, date_time(EAST_8, Date::new(2026, 5, 14), Time::new(1, 0, 0)));
        assert_eq!(next.date(), Date::new(2026, 5, 14));
        assert_eq!(next.with_timezone(Offset::UTC).date(), Date::new(2026, 5, 13), "utc date is a day behind");
    }
}
