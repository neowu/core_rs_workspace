use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use time::UtcOffset;

/// Fixed offset timezone, this layer intentionally does not support named timezones with DST.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Offset(pub(crate) UtcOffset);

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

// renders the rfc3339 time-offset form, e.g. Z, +08:00, -05:00, seconds are not part of the grammar
// and new() always builds with zero seconds, so they are never printed
impl Debug for Offset {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.0.is_utc() {
            return f.write_str("Z");
        }
        // each component carries the sign, take the magnitude and print one sign for the whole offset
        let sign = if self.0.is_negative() { '-' } else { '+' };
        let (hours, minutes) = (self.0.whole_hours().unsigned_abs(), self.0.minutes_past_hour().unsigned_abs());
        write!(f, "{sign}{hours:02}:{minutes:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::Offset;

    #[test]
    fn debug() {
        assert_eq!(format!("{:?}", Offset::UTC), "Z");
        assert_eq!(format!("{:?}", Offset::new(0, 0)), "Z");
        assert_eq!(format!("{:?}", Offset::new(8, 0)), "+08:00");
        assert_eq!(format!("{:?}", Offset::new(5, 30)), "+05:30");
        assert_eq!(format!("{:?}", Offset::new(-5, 0)), "-05:00");
        assert_eq!(format!("{:?}", Offset::new(-3, -30)), "-03:30");
        assert_eq!(format!("{:?}", Offset::new(0, 30)), "+00:30");
        assert_eq!(format!("{:?}", Offset::new(0, -30)), "-00:30");
    }
}
