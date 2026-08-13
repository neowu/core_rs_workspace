use std::borrow::Cow;
use std::ops::Sub;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de;
use serde::ser;
use time::OffsetDateTime;
use time::UtcOffset;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::exception::Exception;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(time::Date);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(time::Time);

/// Timestamp in UTC, the type every app should use to store and transfer a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime(time::OffsetDateTime);

/// Fixed offset timezone, this layer intentionally does not support named timezones with DST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Offset(time::UtcOffset);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignedDuration(time::SignedDuration);

impl Date {
    // new() is always called by constructing const or unit test, panic on invalid input
    // for dynamic input, use parse or deserialize
    pub const fn new(year: i32, month: u8, day: u8) -> Self {
        let month = match month {
            1 => time::Month::January,
            2 => time::Month::February,
            3 => time::Month::March,
            4 => time::Month::April,
            5 => time::Month::May,
            6 => time::Month::June,
            7 => time::Month::July,
            8 => time::Month::August,
            9 => time::Month::September,
            10 => time::Month::October,
            11 => time::Month::November,
            12 => time::Month::December,
            _ => panic!("input must be valid"),
        };
        match time::Date::from_calendar_date(year, month, day) {
            Ok(date) => Date(date),
            Err(_) => panic!("input must be valid"),
        }
    }

    pub fn parse(value: &str) -> Result<Self, Exception> {
        time::Date::parse(value, format_description!("[year]-[month]-[day]"))
            .map(Self)
            .map_err(|err| exception!(format!("failed to parse date, value={value}"), source = err))
    }

    pub fn to_rfc3339(self) -> String {
        self.0.format(format_description!("[year]-[month]-[day]")).expect("format cannot fail")
    }

    #[inline]
    pub const fn to_ymd(self) -> (i32, u8, u8) {
        let (year, month, day) = self.0.to_calendar_date();
        (year, month as u8, day)
    }

    pub fn add_days(self, days: i64) -> Result<Self, Exception> {
        self.0
            .checked_add(time::Duration::days(days))
            .map(Self)
            .ok_or_else(|| exception!(format!("date is out of range, days={days}")))
    }
}

impl Sub for Date {
    type Output = i64;

    #[inline]
    fn sub(self, other: Self) -> Self::Output {
        (self.0 - other.0).whole_days()
    }
}

impl Time {
    pub const fn new(hour: u8, minute: u8, second: u8) -> Self {
        match time::Time::from_hms(hour, minute, second) {
            Ok(time) => Time(time),
            Err(_) => panic!("input must be valid"),
        }
    }

    pub fn parse(value: &str) -> Result<Self, Exception> {
        time::Time::parse(value, format_description!("[hour]:[minute]:[second][optional [.[subsecond]]]"))
            .map(Self)
            .map_err(|err| exception!(format!("failed to parse time, value={value}"), source = err))
    }

    pub fn to_rfc3339(self) -> String {
        if self.0.nanosecond() == 0 {
            self.0.format(format_description!("[hour]:[minute]:[second]")).expect("format cannot fail")
        } else {
            self.0.format(format_description!("[hour]:[minute]:[second].[subsecond]")).expect("format cannot fail")
        }
    }

    #[inline]
    pub const fn to_hms_nanos(self) -> (u8, u8, u8, u32) {
        self.0.as_hms_nano()
    }
}

impl DateTime {
    #[inline]
    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc())
    }

    #[inline]
    pub const fn new(date: Date, time: Time) -> Self {
        Self(OffsetDateTime::new_utc(date.0, time.0))
    }

    pub fn parse(value: &str) -> Result<Self, Exception> {
        OffsetDateTime::parse(value, &Rfc3339)
            .map(Self)
            .map_err(|err| exception!(format!("failed to parse date, value={value}"), source = err))
    }

    pub fn from_unix_timestamp_millis(millis: i64) -> Result<Self, Exception> {
        let nanos = i128::from(millis) * 1_000_000;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map(Self)
            .map_err(|err| exception!(format!("timestamp is out of range, millis={millis}"), source = err))
    }

    #[inline]
    pub fn unix_timestamp_millis(&self) -> i64 {
        let millis = self.0.unix_timestamp_nanos() / 1_000_000;
        i64::try_from(millis).expect("value must be in range")
    }

    #[inline]
    pub const fn timezone(self) -> Offset {
        Offset(self.0.offset())
    }

    #[inline]
    pub const fn date(self) -> Date {
        Date(self.0.date())
    }

    #[inline]
    pub const fn time(self) -> Time {
        Time(self.0.time())
    }

    #[inline]
    #[must_use]
    pub const fn with_timezone(self, offset: Offset) -> Self {
        DateTime(self.0.to_offset(offset.0))
    }

    #[inline]
    #[must_use]
    pub const fn with_date(self, date: Date) -> Self {
        DateTime(self.0.replace_date(date.0))
    }

    #[inline]
    #[must_use]
    pub const fn with_time(self, time: Time) -> Self {
        DateTime(self.0.replace_time(time.0))
    }

    pub fn add_duration(self, duration: SignedDuration) -> Result<Self, Exception> {
        self.0.checked_add(duration.0).map(Self).ok_or_else(|| exception!("result is out of range"))
    }

    pub fn to_rfc3339_utc_secs(&self) -> String {
        self.0
            .to_utc()
            .format(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z"))
            .expect("format cannot fail")
    }

    pub fn to_rfc3339(&self) -> String {
        self.0.format(&Rfc3339).expect("format cannot fail")
    }
}

impl Sub for DateTime {
    type Output = SignedDuration;

    #[inline]
    fn sub(self, other: Self) -> Self::Output {
        SignedDuration(self.0 - other.0)
    }
}

impl Offset {
    pub const UTC: Self = Self(UtcOffset::UTC);

    /// East of UTC is positive, e.g. 8 for +08:00, -5 for -05:00,
    /// the sign of the hour component applies to the whole offset.
    pub const fn new(hours: i8, minutes: i8) -> Self {
        match UtcOffset::from_hms(hours, minutes, 0) {
            Ok(offset) => Offset(offset),
            Err(_) => panic!("input must be valid"),
        }
    }
}

impl SignedDuration {
    #[inline]
    pub const fn from_hours(hours: i64) -> Self {
        Self(time::SignedDuration::hours(hours))
    }

    #[inline]
    pub const fn from_mins(mins: i64) -> Self {
        Self(time::SignedDuration::minutes(mins))
    }

    #[inline]
    pub const fn from_secs(secs: i64) -> Self {
        Self(time::SignedDuration::seconds(secs))
    }

    #[inline]
    pub const fn from_nanos(nanos: i128) -> Self {
        Self(time::SignedDuration::nanoseconds_i128(nanos))
    }

    #[inline]
    pub const fn as_days(self) -> i64 {
        self.0.whole_days()
    }

    #[inline]
    pub const fn as_secs(self) -> i64 {
        self.0.whole_seconds()
    }

    #[inline]
    pub const fn as_nanos(self) -> i128 {
        self.0.whole_nanoseconds()
    }
}

impl Serialize for Date {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Cow::<str>::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

impl Serialize for Time {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Time {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Cow::<str>::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

impl Serialize for DateTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let string = self.0.to_utc().format(&Rfc3339).map_err(ser::Error::custom)?;
        serializer.serialize_str(&string)
    }
}

impl<'de> Deserialize<'de> for DateTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Cow::<str>::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::date::Date;
    use crate::date::DateTime;
    use crate::date::Offset;
    use crate::date::Time;
    use crate::json;

    const EPOCH_SECONDS: i64 = 1_700_000_000; // 2023-11-14T22:13:20Z

    #[test]
    fn date() {
        assert_eq!(Date::parse("2025-11-05").unwrap(), Date::new(2025, 11, 5));
        assert_eq!(Date::parse("2024-02-29").unwrap().to_rfc3339(), "2024-02-29"); // leap day

        assert!(Date::parse("2025-11-05").unwrap() < Date::parse("2025-11-06").unwrap());
    }

    #[test]
    fn date_json() {
        let date = Date::parse("2025-11-05").unwrap();
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2025-11-05""#);
        assert_eq!(json::from_json::<Date>(&json).unwrap(), date);
    }

    #[test]
    fn sub_date() {
        assert_eq!(Date::new(2023, 11, 14) - Date::new(2023, 11, 13), 1);
        assert_eq!(Date::new(2023, 11, 13) - Date::new(2023, 11, 13), 0);
        assert_eq!(Date::new(2023, 11, 13) - Date::new(2023, 11, 14), -1);
    }

    #[test]
    fn add_days() {
        assert_eq!(Date::new(2023, 11, 14).add_days(1).unwrap(), Date::new(2023, 11, 15));
        assert_eq!(Date::new(2023, 11, 14).add_days(0).unwrap(), Date::new(2023, 11, 14));
        assert_eq!(Date::new(2023, 11, 14).add_days(-1).unwrap(), Date::new(2023, 11, 13));
    }

    #[test]
    fn time() {
        assert_eq!(Time::parse("11:29:13").unwrap(), Time::new(11, 29, 13));
        assert_eq!(Time::parse("11:30:00").unwrap().to_rfc3339(), "11:30:00");
        assert_eq!(Time::parse("01:02:03.123456789").unwrap().to_rfc3339(), "01:02:03.123456789");

        assert!(Time::parse("11:29:00").unwrap() < Time::parse("11:30:00").unwrap());
    }

    #[test]
    fn time_json() {
        let time = Time::new(11, 29, 13);
        let json = json::to_json(&time).unwrap();
        assert_eq!(json, r#""11:29:13""#);
        assert_eq!(json::from_json::<Time>(&json).unwrap(), time);
    }

    #[test]
    fn from_unix_timestamp() {
        let date = DateTime::from_unix_timestamp_millis(EPOCH_SECONDS * 1000 + 123).unwrap();
        assert_eq!(date.to_rfc3339(), "2023-11-14T22:13:20.123Z");
        assert_eq!(date.unix_timestamp_millis(), 1_700_000_000_123);
    }

    #[test]
    fn datetime() {
        let date = DateTime::parse("2023-11-14T22:13:20.123456789Z").unwrap();
        assert_eq!(date.date(), Date::new(2023, 11, 14));
        assert_eq!(date.time(), Time::parse("22:13:20.123456789").unwrap());
        assert_eq!(date.to_rfc3339(), "2023-11-14T22:13:20.123456789Z");

        let offset = Offset::new(8, 0);
        let offset_date = date.with_timezone(offset);
        assert_eq!(offset_date, date);
        assert_eq!(offset_date.to_rfc3339(), "2023-11-15T06:13:20.123456789+08:00");
        assert_eq!(DateTime::parse("2023-11-15T06:13:20.123456789+08:00").unwrap(), date);
    }

    #[test]
    fn datetime_json() {
        let date = DateTime::parse("2023-11-14T22:13:20Z").unwrap();
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2023-11-14T22:13:20Z""#);
        assert_eq!(json::from_json::<DateTime>(&json).unwrap(), date);

        let offset = Offset::new(8, 0);
        let date = date.with_timezone(offset);
        assert_eq!(json::to_json(&date).unwrap(), r#""2023-11-14T22:13:20Z""#);
    }
}
