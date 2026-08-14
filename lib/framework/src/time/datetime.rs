use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Sub;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de;
use serde::ser;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::exception::Exception;
use crate::time::Date;
use crate::time::Offset;
use crate::time::SignedDuration;
use crate::time::Time;

/// Timestamp in UTC, the type every app should use to store and transfer a point in time.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime(time::OffsetDateTime);

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

    pub fn from_unix_timestamp_nanos(nanos: i128) -> Result<Self, Exception> {
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map(Self)
            .map_err(|err| exception!(format!("timestamp is out of range, nanos={nanos}"), source = err))
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

    pub fn to_rfc3339(&self) -> String {
        self.0.format(&Rfc3339).expect("format cannot fail")
    }
}

impl Debug for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_rfc3339())
    }
}

impl Sub for DateTime {
    type Output = SignedDuration;

    #[inline]
    fn sub(self, other: Self) -> Self::Output {
        SignedDuration(self.0 - other.0)
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
    use crate::json;
    use crate::time::Date;
    use crate::time::DateTime;
    use crate::time::Offset;
    use crate::time::Time;

    #[test]
    fn parse() {
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
    fn to_json() {
        let date = DateTime::parse("2023-11-14T22:13:20Z").unwrap();
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2023-11-14T22:13:20Z""#);
        assert_eq!(json::from_json::<DateTime>(&json).unwrap(), date);
        assert_eq!(json::from_json::<DateTime>(r#""2023-11-15T06:13:20+08:00""#).unwrap(), date);

        let date = date.with_timezone(Offset::new(8, 0));
        assert_eq!(json::to_json(&date).unwrap(), r#""2023-11-14T22:13:20Z""#, "to_json should always convert to UTC");
    }

    #[test]
    fn from_unix_timestamp() {
        const EPOCH_SECONDS: i128 = 1_700_000_000; // 2023-11-14T22:13:20Z

        let date = DateTime::from_unix_timestamp_nanos(EPOCH_SECONDS * 1_000_000_000 + 123_000_000).unwrap();
        assert_eq!(date.to_rfc3339(), "2023-11-14T22:13:20.123Z");
        assert_eq!(date.unix_timestamp_millis(), 1_700_000_000_123);

        assert_eq!(
            DateTime::from_unix_timestamp_nanos(EPOCH_SECONDS * 1_000_000_000 + 123_456_789).unwrap().to_rfc3339(),
            "2023-11-14T22:13:20.123456789Z"
        );

        assert_eq!(DateTime::from_unix_timestamp_nanos(0).unwrap(), DateTime::parse("1970-01-01T00:00:00Z").unwrap());
        assert_eq!(
            DateTime::from_unix_timestamp_nanos(-500_000_000).unwrap(),
            DateTime::parse("1969-12-31T23:59:59.5Z").unwrap()
        );

        let _err = DateTime::from_unix_timestamp_nanos(i128::MAX).unwrap_err();
    }

    #[test]
    fn sub() {
        let date1 = DateTime::parse("2023-11-14T22:13:20Z").unwrap();
        let date2 = DateTime::parse("2023-11-14T22:13:21.500Z").unwrap();
        assert_eq!((date2 - date1).as_millis(), 1500);
        assert_eq!((date1 - date2).as_millis(), -1500);
        assert_eq!((date2 - date1).as_secs(), 1);
    }
}
