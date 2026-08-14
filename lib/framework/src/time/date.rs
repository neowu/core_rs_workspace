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
use time::macros::format_description;

use crate::exception::Exception;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(pub(crate) time::Date);

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
            .ok_or_else(|| exception!(format!("result is out of range, days={days}")))
    }
}

impl Debug for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_rfc3339())
    }
}

impl Sub for Date {
    type Output = i64;

    #[inline]
    fn sub(self, other: Self) -> Self::Output {
        (self.0 - other.0).whole_days()
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

#[cfg(test)]
mod tests {
    use super::Date;
    use crate::json;

    #[test]
    fn parse() {
        assert_eq!(Date::parse("2025-11-05").unwrap(), Date::new(2025, 11, 5));
        assert_eq!(Date::parse("2024-02-29").unwrap().to_rfc3339(), "2024-02-29"); // leap day

        assert!(Date::parse("2025-11-05").unwrap() < Date::parse("2025-11-06").unwrap());
    }

    #[test]
    fn to_json() {
        let date = Date::parse("2025-11-05").unwrap();
        let json = json::to_json(&date).unwrap();
        assert_eq!(json, r#""2025-11-05""#);
        assert_eq!(json::from_json::<Date>(&json).unwrap(), date);
    }

    #[test]
    fn sub() {
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
}
