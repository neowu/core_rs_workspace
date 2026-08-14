use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;

use framework::time::Date;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de;
use serde::ser;

// maps to clickhouse Date: RowBinary carries u16 days since 1970-01-01, covering 1970-01-01
// to 2149-06-06;
// framework's Date serde impl emits a "YYYY-MM-DD" string, so this newtype switches on the
// format instead of requiring #[serde(with = ...)] at every callsite.
// Option<Date16> works as-is for Nullable(Date), no ::option helper variant needed.
// as a query param the SQL serializer (is_human_readable) applies instead, and the
// 'YYYY-MM-DD' string is what the server parses; the u16 form would be rejected outright,
// the server refuses to compare Date with a number.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date16(Date);

const BASE_DATE: Date = Date::new(1970, 1, 1);

impl Debug for Date16 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for Date16 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            // the server clamps anything outside the u16 days range silently, so serialize fails instead
            let days = u16::try_from(self.0 - BASE_DATE)
                .map_err(|_err| ser::Error::custom(format!("date is out of range, date={self:?}")))?;
            days.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Date16 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            Date::deserialize(deserializer).map(Self)
        } else {
            let days = u16::deserialize(deserializer)?;
            BASE_DATE.add_days(days as i64).map(Self).map_err(de::Error::custom)
        }
    }
}

impl From<Date> for Date16 {
    fn from(date: Date) -> Self {
        Self(date)
    }
}

impl From<Date16> for Date {
    fn from(date: Date16) -> Self {
        date.0
    }
}

impl Deref for Date16 {
    type Target = Date;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use framework::json;
    use framework::time::Date;
    use framework::time::DateTime;

    use super::Date16;

    // this is what ClickHouse logs as a query param
    #[test]
    fn debug_format() {
        assert_eq!(format!("{:?}", Date16::from(Date::new(2026, 7, 15))), "2026-07-15");
    }

    // json is a human readable format, same branch the SQL param serializer takes;
    // the RowBinary branch is covered end to end by test/clickhouse_test
    #[test]
    fn serde() {
        let date = Date16::from(Date::new(2026, 7, 15));
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2026-07-15""#);
        assert_eq!(json::from_json::<Date16>(&json).unwrap(), date);
    }

    #[test]
    fn from_date() {
        let today = DateTime::now().date();
        assert_eq!(Date::from(Date16::from(today)), today);
    }
}
