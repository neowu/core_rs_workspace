use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;

use bytes::BytesMut;
use framework::time;
use postgres_protocol::types;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::IsNull;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::types::accepts;
use tokio_postgres::types::to_sql_checked;

// maps to postgres date: the binary format carries i32 days since 2000-01-01;
// framework's Date carries no postgres conversion itself, so this newtype provides it instead
// of enabling tokio_postgres's own date integrations.
// Option<Date> works as-is for a nullable column, tokio_postgres wraps every impl.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(time::Date);

// both postgres date and timestamptz count from this epoch
pub(super) const BASE_DATE: time::Date = time::Date::new(2000, 1, 1);

// ToSql requires Debug, and it is what the query param log prints, so render the wire value
// rather than the nested debug of the inner types
impl Debug for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ToSql for Date {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let days = i32::try_from(self.0 - BASE_DATE).map_err(|_err| format!("date is out of range, date={self:?}"))?;
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
mod tests {
    use bytes::BytesMut;
    use framework::time;
    use tokio_postgres::types::FromSql as _;
    use tokio_postgres::types::ToSql as _;
    use tokio_postgres::types::Type;

    use super::Date;

    #[test]
    fn to_sql() {
        let date = Date::from(time::Date::new(2026, 7, 15));
        let mut buffer = BytesMut::new();
        date.to_sql(&Type::DATE, &mut buffer).unwrap();
        assert_eq!(buffer.as_ref(), 9_692_i32.to_be_bytes()); // 9692 days after 2000-01-01
    }

    #[test]
    fn from_sql() {
        let raw = 9_692_i32.to_be_bytes();
        let date = Date::from_sql(&Type::DATE, &raw).unwrap();
        assert_eq!(time::Date::from(date), time::Date::new(2026, 7, 15));
    }

    #[test]
    fn round_trip() {
        // before the 2000-01-01 base, days_since is negative there
        let value = time::Date::new(1970, 1, 1);
        let mut buffer = BytesMut::new();
        Date::from(value).to_sql(&Type::DATE, &mut buffer).unwrap();
        assert_eq!(buffer.as_ref(), (-10_957_i32).to_be_bytes());
        assert_eq!(time::Date::from(Date::from_sql(&Type::DATE, buffer.as_ref()).unwrap()), value);
    }

    #[test]
    fn from_sql_rejects_infinity() {
        let raw = i32::MAX.to_be_bytes();
        let error = Date::from_sql(&Type::DATE, &raw).unwrap_err();
        assert!(error.to_string().starts_with("result is out of range"));
    }

    // this is what repository logs as a query param
    #[test]
    fn debug_format() {
        assert_eq!(format!("{:?}", Date::from(time::Date::new(2026, 7, 16))), "2026-07-16");
    }
}
