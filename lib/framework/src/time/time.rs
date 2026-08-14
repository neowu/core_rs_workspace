use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de;
use time::macros::format_description;

use crate::exception::Exception;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub(crate) time::Time);

impl Time {
    pub const MIDNIGHT: Time = Time(time::Time::MIDNIGHT);

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

impl Debug for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_rfc3339())
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

#[cfg(test)]
mod tests {
    use crate::json;
    use crate::time::Time;

    #[test]
    fn parse() {
        assert_eq!(Time::parse("11:29:13").unwrap(), Time::new(11, 29, 13));
        assert_eq!(Time::parse("11:30:00").unwrap().to_rfc3339(), "11:30:00");
        assert_eq!(Time::parse("01:02:03.123456789").unwrap().to_rfc3339(), "01:02:03.123456789");
        assert!(Time::parse("11:29:00").unwrap() < Time::parse("11:30:00").unwrap());
    }

    #[test]
    fn to_json() {
        let time = Time::new(11, 29, 13);
        let json = json::to_json(&time).unwrap();
        assert_eq!(json, r#""11:29:13""#);
        assert_eq!(json::from_json::<Time>(&json).unwrap(), time);
    }
}
