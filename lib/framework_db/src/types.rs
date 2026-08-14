use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;

use bytes::BytesMut;
use framework::time;
use framework::time::DateTime;
use framework::time::SignedDuration;
use framework::time::Time;
use postgres_protocol::types;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::IsNull;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::types::accepts;
use tokio_postgres::types::to_sql_checked;

// maps to postgres timestamptz: the binary format carries i64 microseconds since 2000-01-01 UTC
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(DateTime);

const BASE_TIMESTAMP: DateTime = DateTime::new(BASE_DATE, Time::MIDNIGHT);

// ToSql requires Debug, and it is what the query param log prints, so render the wire value
// rather than the nested debug of the inner types
impl Debug for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ToSql for Timestamp {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // SignedDuration carries nanoseconds, sub microsecond precision is truncated on the wire
        let micros = i64::try_from((self.0 - BASE_TIMESTAMP).as_nanos() / 1_000)
            .map_err(|_err| format!("timestamp is out of range, timestamp={}", self.0.to_rfc3339()))?;
        types::timestamp_to_sql(micros, out);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

// infinity / -infinity are sent as i64::MAX / i64::MIN, add_duration fails with out of range on
// both rather than decoding to a bogus point in time
impl<'a> FromSql<'a> for Timestamp {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let micros = types::timestamp_from_sql(raw)?;
        let date_time = BASE_TIMESTAMP
            .add_duration(SignedDuration::from_nanos(i128::from(micros) * 1_000))
            .map_err(|_err| format!("timestamp is out of range, micros={micros}"))?;
        Ok(Self(date_time))
    }

    accepts!(TIMESTAMPTZ);
}

impl From<DateTime> for Timestamp {
    fn from(date_time: DateTime) -> Self {
        Self(date_time)
    }
}

impl From<Timestamp> for DateTime {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.0
    }
}

impl Deref for Timestamp {
    type Target = DateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// maps to postgres date: the binary format carries i32 days since 2000-01-01;
// framework's Date carries no postgres conversion itself, so this newtype provides it instead
// of enabling tokio_postgres's own date integrations.
// Option<Date> works as-is for a nullable column, tokio_postgres wraps every impl.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(time::Date);

// both postgres date and timestamptz count from this epoch
const BASE_DATE: time::Date = time::Date::new(2000, 1, 1);

impl Debug for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ToSql for Date {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let days = i32::try_from(self.0 - BASE_DATE)
            .map_err(|_err| format!("date is out of range, date={}", self.0.to_rfc3339()))?;
        types::date_to_sql(days, out);
        Ok(IsNull::No)
    }

    accepts!(DATE);
    to_sql_checked!();
}

// infinity / -infinity are sent as i32::MAX / i32::MIN, add_days fails with out of range on both
impl<'a> FromSql<'a> for Date {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let days = types::date_from_sql(raw)?;
        let date = BASE_DATE.add_days(i64::from(days)).map_err(|err| err.to_string())?;
        Ok(Self(date))
    }

    accepts!(DATE);
}

impl From<time::Date> for Date {
    fn from(date: time::Date) -> Self {
        Self(date)
    }
}

impl From<Date> for time::Date {
    fn from(date: Date) -> Self {
        date.0
    }
}

impl Deref for Date {
    type Target = time::Date;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
// the postgres binary format is big endian, to_be_bytes is how these tests spell the wire bytes
#[allow(clippy::big_endian_bytes)]
mod tests {
    use bytes::BytesMut;
    use framework::time;
    use framework::time::DateTime;
    use tokio_postgres::types::FromSql as _;
    use tokio_postgres::types::ToSql as _;
    use tokio_postgres::types::Type;

    use super::Date;
    use super::Timestamp;

    #[test]
    fn timestamp_to_sql() {
        let timestamp = Timestamp::from(DateTime::parse("2026-07-15T12:30:45.123456Z").unwrap());
        let mut buffer = BytesMut::new();
        timestamp.to_sql(&Type::TIMESTAMPTZ, &mut buffer).unwrap();
        // 2026-07-15T12:30:45.123456Z is 837_433_845.123456 seconds after 2000-01-01
        assert_eq!(buffer.as_ref(), 837_433_845_123_456_i64.to_be_bytes());
    }

    #[test]
    fn timestamp_from_sql() {
        let raw = 837_433_845_123_456_i64.to_be_bytes();
        let timestamp = Timestamp::from_sql(&Type::TIMESTAMPTZ, &raw).unwrap();
        assert_eq!(DateTime::from(timestamp), DateTime::parse("2026-07-15T12:30:45.123456Z").unwrap());
    }

    #[test]
    fn timestamp_round_trip_truncates_sub_micros() {
        let date_time = DateTime::parse("1970-01-01T00:00:00.123456789Z").unwrap(); // before the base, micros go negative
        let mut buffer = BytesMut::new();
        Timestamp::from(date_time).to_sql(&Type::TIMESTAMPTZ, &mut buffer).unwrap();
        assert_eq!(buffer.as_ref(), (-946_684_799_876_543_i64).to_be_bytes());
        let timestamp = Timestamp::from_sql(&Type::TIMESTAMPTZ, buffer.as_ref()).unwrap();
        // timestamptz keeps microseconds, the nanoseconds of SignedDuration do not survive;
        // the truncation is toward the base, so it rounds up on this side of it
        assert_eq!(timestamp.to_rfc3339(), "1970-01-01T00:00:00.123457Z");
    }

    #[test]
    fn timestamp_from_sql_rejects_infinity() {
        let raw = i64::MAX.to_be_bytes();
        let error = Timestamp::from_sql(&Type::TIMESTAMPTZ, &raw).unwrap_err();
        assert!(error.to_string().starts_with("timestamp is out of range"));
    }

    #[test]
    fn timestamp_rejects_wrong_type() {
        let timestamp = Timestamp::from(DateTime::now());
        let mut buffer = BytesMut::new();
        // to_sql_checked is what tokio_postgres calls, it validates against the column type
        assert!(timestamp.to_sql_checked(&Type::TIMESTAMP, &mut buffer).is_err());
    }

    #[test]
    fn date_to_sql() {
        let date = Date::from(time::Date::new(2026, 7, 15));
        let mut buffer = BytesMut::new();
        date.to_sql(&Type::DATE, &mut buffer).unwrap();
        assert_eq!(buffer.as_ref(), 9_692_i32.to_be_bytes()); // 9692 days after 2000-01-01
    }

    #[test]
    fn date_from_sql() {
        let raw = 9_692_i32.to_be_bytes();
        let date = Date::from_sql(&Type::DATE, &raw).unwrap();
        assert_eq!(time::Date::from(date), time::Date::new(2026, 7, 15));
    }

    #[test]
    fn date_round_trip() {
        // before the 2000-01-01 base, days_since is negative there
        let value = time::Date::new(1970, 1, 1);
        let mut buffer = BytesMut::new();
        Date::from(value).to_sql(&Type::DATE, &mut buffer).unwrap();
        assert_eq!(buffer.as_ref(), (-10_957_i32).to_be_bytes());
        assert_eq!(time::Date::from(Date::from_sql(&Type::DATE, buffer.as_ref()).unwrap()), value);
    }

    #[test]
    fn date_from_sql_rejects_infinity() {
        let raw = i32::MAX.to_be_bytes();
        let error = Date::from_sql(&Type::DATE, &raw).unwrap_err();
        assert!(error.to_string().starts_with("date is out of range"));
    }

    // this is what repository logs as a query param
    #[test]
    fn debug_format() {
        let timestamp = Timestamp::from(DateTime::parse("2026-07-16T08:30:45.123456Z").unwrap());
        assert_eq!(format!("{timestamp:?}"), "Timestamp(2026-07-16T08:30:45.123456Z)");
        assert_eq!(format!("{:?}", Some(timestamp)), "Some(Timestamp(2026-07-16T08:30:45.123456Z))");
        assert_eq!(format!("{:?}", Date::from(time::Date::new(2026, 7, 16))), "Date(2026-07-16)");
    }
}
