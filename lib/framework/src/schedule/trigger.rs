use std::time::Duration;

use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveTime;
use chrono::TimeDelta;
use chrono::Utc;

use super::Trigger;

pub(crate) struct FixedRateTrigger {
    pub(crate) interval: Duration,
}

impl Trigger for FixedRateTrigger {
    fn next(&self, previous: DateTime<Utc>) -> DateTime<Utc> {
        previous + chrono::Duration::from_std(self.interval).expect("input cannot be out of range")
    }
}

pub(crate) struct DailyTrigger {
    pub(crate) time_zone: FixedOffset,
    pub(crate) time: NaiveTime,
}

impl Trigger for DailyTrigger {
    fn next(&self, previous: DateTime<Utc>) -> DateTime<Utc> {
        let next_time = previous.with_timezone(&self.time_zone).with_time(self.time).unwrap();
        if next_time > previous {
            next_time.to_utc()
        } else {
            next_time.checked_add_signed(TimeDelta::days(1)).expect("result cannot be out of range").to_utc()
        }
    }
}
