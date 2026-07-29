use std::ops::Deref;

use chrono::NaiveDate;
use chrono::Utc;
use clickhouse::serde::chrono::date;
use clickhouse::serde::chrono::datetime64;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

// maps to clickhouse DateTime64(3, 'UTC'): RowBinary carries i64 milliseconds since epoch;
// chrono's own serde impl emits an RFC3339 string, so this newtype delegates to the
// clickhouse chrono helper instead of requiring #[serde(with = ...)] at every callsite.
// Option<DateTime> works as-is for Nullable(DateTime64), no ::option helper variant needed.
// as a query param the SQL serializer (is_human_readable) applies instead, and chrono's
// RFC3339 string is what the server parses; the millis form would compare as a plain number
// against DateTime64 and silently match nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime64(chrono::DateTime<Utc>);

impl Serialize for DateTime64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            datetime64::millis::serialize(&self.0, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for DateTime64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            chrono::DateTime::<Utc>::deserialize(deserializer).map(Self)
        } else {
            datetime64::millis::deserialize(deserializer).map(Self)
        }
    }
}

impl From<chrono::DateTime<Utc>> for DateTime64 {
    fn from(date_time: chrono::DateTime<Utc>) -> Self {
        Self(date_time)
    }
}

impl From<DateTime64> for chrono::DateTime<Utc> {
    fn from(date_time: DateTime64) -> Self {
        date_time.0
    }
}

impl Deref for DateTime64 {
    type Target = chrono::DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// maps to clickhouse Date: RowBinary carries u16 days since 1970-01-01 (up to 2149-06-06);
// chrono's own serde impl emits a "YYYY-MM-DD" string, so this newtype delegates to the
// clickhouse chrono helper instead of requiring #[serde(with = ...)] at every callsite.
// Option<Date> works as-is for Nullable(Date), no ::option helper variant needed.
// as a query param the SQL serializer (is_human_readable) applies instead, and chrono's
// 'YYYY-MM-DD' string is what the server parses; the u16 form would be rejected outright,
// the server refuses to compare Date with a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(NaiveDate);

impl Serialize for Date {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() { self.0.serialize(serializer) } else { date::serialize(&self.0, serializer) }
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            NaiveDate::deserialize(deserializer).map(Self)
        } else {
            date::deserialize(deserializer).map(Self)
        }
    }
}

impl From<NaiveDate> for Date {
    fn from(date: NaiveDate) -> Self {
        Self(date)
    }
}

impl From<Date> for NaiveDate {
    fn from(date: Date) -> Self {
        date.0
    }
}

impl Deref for Date {
    type Target = NaiveDate;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// maps to clickhouse Decimal64(S): RowBinary carries the raw Int64 value scaled by 10^S,
// which is exactly what serde(transparent) over i64 serializes/deserializes.
// apps pin their scale once via alias, e.g. `type Amount = framework_clickhouse::Decimal<6>;`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Decimal64<const S: u8>(i64);

impl<const S: u8> Decimal64<S> {
    // Decimal64 precision is 18, so S > 18 fails here at compile time via const eval overflow
    const SCALE: f64 = 10_i64.pow(S as u32) as f64;

    // f64 keeps 15-16 significant digits, exact for amounts up to ~10^9 with 6 decimal places
    pub fn from_f64(amount: f64) -> Self {
        Self((amount * Self::SCALE).round() as i64)
    }

    pub fn to_f64(self) -> f64 {
        self.0 as f64 / Self::SCALE
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
    use chrono::NaiveDate;
    use chrono::TimeZone as _;
    use chrono::Utc;
    use framework::json;
    use framework_macro::Enum8;

    use super::Date;
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

    // json is a human readable format, same branch the SQL param serializer takes;
    // the RowBinary branch is covered end to end by test/clickhouse_test
    #[test]
    fn date_time_serde_rfc3339() {
        let date_time = DateTime64::from(Utc.with_ymd_and_hms(2026, 7, 15, 12, 30, 45).unwrap());
        let json = json::to_json(&date_time).unwrap();
        assert_eq!(json, r#""2026-07-15T12:30:45Z""#);
        assert_eq!(json::from_json::<DateTime64>(&json).unwrap(), date_time);
    }

    #[test]
    fn date_time_from_chrono() {
        let now = Utc::now();
        assert_eq!(chrono::DateTime::<Utc>::from(DateTime64::from(now)), now);
    }

    #[test]
    fn date_serde_string() {
        let date = Date::from(NaiveDate::from_ymd_opt(2026, 7, 15).unwrap());
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2026-07-15""#);
        assert_eq!(json::from_json::<Date>(&json).unwrap(), date);
    }

    #[test]
    fn date_from_chrono() {
        let today = Utc::now().date_naive();
        assert_eq!(NaiveDate::from(Date::from(today)), today);
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
