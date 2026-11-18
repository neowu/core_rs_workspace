#![allow(clippy::tests_outside_test_module)]

use framework::json;
use framework::time::Date;
use framework::time::DateTime;
use framework::time::Offset;
use framework::time::SignedDuration;
use framework::time::Time;

const LOCAL: Offset = Offset::new(8, 0);
const OPENING: Time = Time::new(9, 0, 0);

// timestamps are stored and transferred in UTC, the offset is only applied to read or change wall clock fields
#[test]
fn schedule_in_local_time() {
    let ordered = DateTime::parse("2023-11-14T22:13:20Z").unwrap();

    let local = ordered.with_timezone(LOCAL);
    assert_eq!(local, ordered, "timezone changes the view, not the instant");
    assert_eq!(local.timezone(), LOCAL);
    assert_eq!(local.date(), Date::new(2023, 11, 15));
    assert_eq!(local.time(), Time::new(6, 13, 20));

    // next day at opening time, local
    let next_day = local.date().add_days(1).unwrap();
    let delivery = local.with_date(next_day).with_time(OPENING);
    assert_eq!(delivery.to_rfc3339(), "2023-11-16T09:00:00+08:00");
    assert_eq!(delivery.with_timezone(Offset::UTC).to_rfc3339(), "2023-11-16T01:00:00Z");
    assert_eq!(json::to_json(&delivery).unwrap(), r#""2023-11-16T01:00:00Z""#);

    assert_eq!(next_day - ordered.date(), 2, "local date is a day ahead of UTC date");
}

// as_* return whole units of the entire duration, not the remainder of the larger unit
#[test]
fn elapsed() {
    let start = DateTime::parse("2023-11-14T22:13:20Z").unwrap();
    let end = DateTime::parse("2023-11-16T09:00:00+08:00").unwrap();

    let elapsed = end - start;
    assert!(elapsed.is_positive());
    assert_eq!(elapsed.as_days(), 1);
    assert_eq!(elapsed.as_hours(), 26);
    assert_eq!(elapsed.as_mins(), 1606);
    assert_eq!(elapsed.as_secs(), 96_400);
    assert_eq!(format!("{elapsed:?}"), "P1DT2H46M40S");

    assert_eq!(start.add_duration(elapsed).unwrap(), end);

    let remaining = start - end;
    assert!(remaining.is_negative());
    assert_eq!(remaining.as_hours(), -26, "whole units truncate toward zero");
    assert!((start - start).is_zero());
}

#[test]
fn expiration() {
    const TTL: SignedDuration = SignedDuration::from_mins(30);

    let created = DateTime::now();
    let expires = created.add_duration(TTL).unwrap();
    assert!(created < expires);
    assert_eq!((expires - created).as_mins(), 30);
    assert_eq!(expires.unix_timestamp_millis() - created.unix_timestamp_millis(), 30 * 60 * 1000);
}

#[test]
fn parts() {
    let date = DateTime::parse("2023-11-14T22:13:20.123456789Z").unwrap();
    assert_eq!(date.date().to_ymd(), (2023, 11, 14));
    assert_eq!(date.time().to_hms_nanos(), (22, 13, 20, 123_456_789));
    assert_eq!(Time::MIDNIGHT.to_hms_nanos(), (0, 0, 0, 0));

    let start_of_day = DateTime::new(date.date(), Time::MIDNIGHT);
    assert_eq!(start_of_day.to_rfc3339(), "2023-11-14T00:00:00Z");
    assert_eq!((date - start_of_day).as_hours(), 22);
}
