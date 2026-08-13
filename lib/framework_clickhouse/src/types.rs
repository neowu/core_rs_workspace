use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;

use framework::date::Date;
use framework::date::DateTime;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de;
use serde::ser;

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
        write!(f, "DateTime64({})", self.0.to_rfc3339())
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
            let millis = i64::deserialize(deserializer)?;
            DateTime::from_unix_timestamp_millis(millis).map(Self).map_err(de::Error::custom)
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
        write!(f, "Date16({})", self.0.to_rfc3339())
    }
}

impl Serialize for Date16 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            // the server clamps anything outside the u16 days range silently, so serialize fails instead
            let days = u16::try_from(self.0 - BASE_DATE).map_err(|_err| {
                ser::Error::custom(format!("date is out of Date16 range, date={}", self.0.to_rfc3339()))
            })?;
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

// maps to clickhouse Decimal64(S): RowBinary carries the raw Int64 value scaled by 10^S,
// which is exactly what serde(transparent) over i64 serializes/deserializes.
// apps pin their scale once via alias, e.g. `type Amount = framework_clickhouse::Decimal<6>;`
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Decimal64<const S: u8>(i64);

impl<const S: u8> Decimal64<S> {
    // Decimal64 precision is 18, so S > 18 fails here at compile time via const eval overflow
    const SCALE_UNIT: i64 = 10_i64.pow(S as u32);
    const SCALE: f64 = Self::SCALE_UNIT as f64;

    // f64 keeps 15-16 significant digits, exact for amounts up to ~10^9 with 6 decimal places
    pub fn from_f64(amount: f64) -> Self {
        Self((amount * Self::SCALE).round() as i64)
    }

    pub fn to_f64(self) -> f64 {
        self.0 as f64 / Self::SCALE
    }
}

// the raw scaled integer is unreadable in a query param log, so place the decimal point;
// integer math on purpose, it stays exact for values past the 15-16 digits to_f64 keeps
impl<const S: u8> Debug for Decimal64<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let value = self.0.unsigned_abs();
        let scale = Self::SCALE_UNIT.unsigned_abs();
        if S == 0 {
            write!(f, "Decimal64({sign}{value})")
        } else {
            write!(f, "Decimal64({sign}{}.{:0width$})", value / scale, value % scale, width = S as usize)
        }
    }
}

impl<const S: u8> From<f64> for Decimal64<S> {
    fn from(amount: f64) -> Self {
        Self::from_f64(amount)
    }
}

impl<const S: u8> From<Decimal64<S>> for f64 {
    fn from(decimal: Decimal64<S>) -> Self {
        decimal.to_f64()
    }
}

#[cfg(test)]
mod tests {
    use framework::date::Date;
    use framework::date::DateTime;
    use framework::json;
    use framework_macro::Enum8;

    use super::Date16;
    use super::DateTime64;
    use super::Decimal64;

    // Enum8('OK' = 1, 'ERROR' = -2)
    #[derive(Enum8, Debug, PartialEq)]
    enum TestResult {
        Ok = 1,
        Error = -2,
    }

    #[test]
    fn enum8_serde_i8() {
        assert_eq!(json::to_json(&TestResult::Ok).unwrap(), "1");
        assert_eq!(json::to_json(&TestResult::Error).unwrap(), "-2");
        assert_eq!(json::from_json::<TestResult>("1").unwrap(), TestResult::Ok);
        assert_eq!(json::from_json::<TestResult>("-2").unwrap(), TestResult::Error);
        let error = json::from_json::<TestResult>("3").unwrap_err();
        assert!(error.to_string().starts_with("failed to deserialize, json=3"));
    }

    // this is what ClickHouse logs as a query param
    #[test]
    fn debug_format() {
        let date_time = DateTime64::from(DateTime::parse("2026-07-15T12:30:45.123Z").unwrap());
        assert_eq!(format!("{date_time:?}"), "DateTime64(2026-07-15T12:30:45.123Z)");
        assert_eq!(format!("{:?}", Some(date_time)), "Some(DateTime64(2026-07-15T12:30:45.123Z))");
        assert_eq!(format!("{:?}", Date16::from(Date::new(2026, 7, 15))), "Date16(2026-07-15)");

        assert_eq!(format!("{:?}", Decimal64::<6>::from_f64(12.345_678)), "Decimal64(12.345678)");
        assert_eq!(format!("{:?}", Decimal64::<6>::from_f64(-0.000_001)), "Decimal64(-0.000001)");
        assert_eq!(format!("{:?}", Decimal64::<2>::from_f64(-12.3)), "Decimal64(-12.30)");
        assert_eq!(format!("{:?}", Decimal64::<0>::from_f64(42.0)), "Decimal64(42)");
        // past the 15-16 significant digits of f64, so this is what integer math buys
        assert_eq!(format!("{:?}", Decimal64::<6>(i64::MIN)), "Decimal64(-9223372036854.775808)");
    }

    // json is a human readable format, same branch the SQL param serializer takes;
    // the RowBinary branch is covered end to end by test/clickhouse_test
    #[test]
    fn date_time_serde_rfc3339() {
        let date_time = DateTime64::from(DateTime::parse("2026-07-15T12:30:45Z").unwrap());
        let json = json::to_json(&date_time).unwrap();
        assert_eq!(json, r#""2026-07-15T12:30:45Z""#);
        assert_eq!(json::from_json::<DateTime64>(&json).unwrap(), date_time);
    }

    #[test]
    fn date_time_from_framework_date() {
        let now = DateTime::now();
        assert_eq!(DateTime::from(DateTime64::from(now)), now);
    }

    #[test]
    fn date_serde_string() {
        let date = Date16::from(Date::new(2026, 7, 15));
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2026-07-15""#);
        assert_eq!(json::from_json::<Date16>(&json).unwrap(), date);
    }

    #[test]
    fn date_from_framework_date() {
        let today = DateTime::now().date();
        assert_eq!(Date::from(Date16::from(today)), today);
    }

    #[test]
    fn decimal_from_f64() {
        assert_eq!(Decimal64::<6>::from_f64(12.345_678), Decimal64(12_345_678));
        assert_eq!(Decimal64::<6>::from_f64(-0.000_001), Decimal64(-1));
        assert_eq!(Decimal64::<2>::from_f64(12.345), Decimal64(1_235));
        assert_eq!(Decimal64::<0>::from_f64(42.4), Decimal64(42));
        // 0.1 + 0.2 = 0.30000000000000004, round() absorbs the f64 representation error
        assert_eq!(Decimal64::<6>::from_f64(0.1 + 0.2), Decimal64(300_000));
    }

    // exact comparisons on purpose: these values fit in f64's 15-16 significant digits
    #[test]
    #[allow(clippy::float_cmp)]
    fn decimal_to_f64() {
        assert_eq!(Decimal64::<6>(12_345_678).to_f64(), 12.345_678);
        assert_eq!(Decimal64::<6>(-1).to_f64(), -0.000_001);
        assert_eq!(Decimal64::<0>(42).to_f64(), 42.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn decimal_f64_round_trip() {
        let amount = 1_234_567_890.123_456;
        assert_eq!(f64::from(Decimal64::<6>::from(amount)), amount);
    }

    #[test]
    fn decimal_serde_transparent() {
        let decimal: Decimal64<6> = json::from_json("12345678").unwrap();
        assert_eq!(decimal, Decimal64(12_345_678));
        assert_eq!(json::to_json(&decimal).unwrap(), "12345678");
    }
}
