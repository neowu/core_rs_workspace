use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;

use framework::time::DateTime;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de;

// maps to clickhouse DateTime64(3, 'UTC'): RowBinary carries i64 milliseconds since epoch;
// framework's DateTime serde impl emits an RFC3339 string, so this newtype switches on the
// format instead of requiring #[serde(with = ...)] at every callsite.
// Option<DateTime64> works as-is for Nullable(DateTime64), no ::option helper variant needed.
// as a query param the SQL serializer (is_human_readable) applies instead, and the RFC3339
// string is what the server parses; the millis form would compare as a plain number
// against DateTime64 and silently match nothing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime64(DateTime);

// QueryParam requires Debug, and it is what the query param log prints, so render the RFC3339
// string that gets bound rather than the nested debug of the inner types
impl Debug for DateTime64 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for DateTime64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            self.0.unix_timestamp_millis().serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for DateTime64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            DateTime::deserialize(deserializer).map(Self)
        } else {
            let millis: i128 = i64::deserialize(deserializer)? as i128;
            DateTime::from_unix_timestamp_nanos(millis * 1_000_000).map(Self).map_err(de::Error::custom)
        }
    }
}

impl From<DateTime> for DateTime64 {
    fn from(date_time: DateTime) -> Self {
        Self(date_time)
    }
}

impl From<DateTime64> for DateTime {
    fn from(date_time: DateTime64) -> Self {
        date_time.0
    }
}

impl Deref for DateTime64 {
    type Target = DateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use framework::json;
    use framework::time::DateTime;

    use super::DateTime64;

    // this is what ClickHouse logs as a query param
    #[test]
    fn debug_format() {
        let date_time = DateTime64::from(DateTime::parse("2026-07-15T12:30:45.123Z").unwrap());
        assert_eq!(format!("{date_time:?}"), "2026-07-15T12:30:45.123Z");
        assert_eq!(format!("{:?}", Some(date_time)), "Some(2026-07-15T12:30:45.123Z)");
    }

    // json is a human readable format, same branch the SQL param serializer takes;
    // the RowBinary branch is covered end to end by test/clickhouse_test
    #[test]
    fn serde() {
        let datetime = DateTime64::from(DateTime::parse("2026-07-15T12:30:45Z").unwrap());
        let json = json::to_json(&datetime).unwrap();
        assert_eq!(json, r#""2026-07-15T12:30:45Z""#);
        assert_eq!(json::from_json::<DateTime64>(&json).unwrap(), datetime);
    }

    #[test]
    fn from_datetime() {
        let now = DateTime::now();
        assert_eq!(DateTime::from(DateTime64::from(now)), now);
    }
}
