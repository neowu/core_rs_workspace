use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignedDuration(pub(crate) time::SignedDuration);

impl SignedDuration {
    #[inline]
    pub const fn from_days(days: i64) -> Self {
        Self(time::SignedDuration::days(days))
    }

    #[inline]
    pub const fn from_hours(hours: i64) -> Self {
        Self(time::SignedDuration::hours(hours))
    }

    #[inline]
    pub const fn from_mins(mins: i64) -> Self {
        Self(time::SignedDuration::minutes(mins))
    }

    #[inline]
    pub const fn from_secs(secs: i64) -> Self {
        Self(time::SignedDuration::seconds(secs))
    }

    #[inline]
    pub const fn from_nanos(nanos: i128) -> Self {
        Self(time::SignedDuration::nanoseconds_i128(nanos))
    }

    #[inline]
    pub const fn as_days(self) -> i64 {
        self.0.whole_days()
    }

    #[inline]
    pub const fn as_hours(self) -> i64 {
        self.0.whole_hours()
    }

    #[inline]
    pub const fn as_mins(self) -> i64 {
        self.0.whole_minutes()
    }

    #[inline]
    pub const fn as_secs(self) -> i64 {
        self.0.whole_seconds()
    }

    #[inline]
    pub const fn as_millis(self) -> i128 {
        self.0.whole_milliseconds()
    }

    #[inline]
    pub const fn as_nanos(self) -> i128 {
        self.0.whole_nanoseconds()
    }

    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    #[inline]
    pub const fn is_negative(self) -> bool {
        self.0.is_negative()
    }

    #[inline]
    pub const fn is_positive(self) -> bool {
        self.0.is_positive()
    }
}

// renders the rfc3339 duration form, e.g. P1DT2H3M4.5S, months and years are never used since they
// carry no fixed length, a duration only knows days and below
impl Debug for SignedDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // unsigned_abs() rather than abs(), the magnitude of MIN does not fit back into a signed duration
        let duration = self.0.unsigned_abs();
        let seconds = duration.as_secs();
        let nanos = duration.subsec_nanos();

        let (days, hours, mins, secs) = (seconds / 86400, seconds % 86400 / 3600, seconds % 3600 / 60, seconds % 60);

        if self.0.is_negative() {
            f.write_str("-")?;
        }
        f.write_str("P")?;
        if days > 0 {
            write!(f, "{days}D")?;
        }
        // zero renders as PT0S, the time part is only omitted when days carry the entire duration
        if days == 0 || hours > 0 || mins > 0 || secs > 0 || nanos > 0 {
            f.write_str("T")?;
            if hours > 0 {
                write!(f, "{hours}H")?;
            }
            if mins > 0 {
                write!(f, "{mins}M")?;
            }
            if secs > 0 || nanos > 0 || (days == 0 && hours == 0 && mins == 0) {
                if nanos == 0 {
                    write!(f, "{secs}S")?;
                } else {
                    // trailing zeros of the 9 digit fraction are trimmed, .5 rather than .500000000
                    let (mut fraction, mut width) = (nanos, 9);
                    while fraction % 10 == 0 {
                        fraction /= 10;
                        width -= 1;
                    }
                    write!(f, "{secs}.{fraction:0width$}S")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SignedDuration;

    #[test]
    fn debug() {
        assert_eq!(format!("{:?}", SignedDuration::from_secs(0)), "PT0S");
        assert_eq!(format!("{:?}", SignedDuration::from_secs(4)), "PT4S");
        assert_eq!(format!("{:?}", SignedDuration::from_mins(3)), "PT3M");
        assert_eq!(format!("{:?}", SignedDuration::from_hours(2)), "PT2H");
        assert_eq!(format!("{:?}", SignedDuration::from_days(1)), "P1D");
        assert_eq!(format!("{:?}", SignedDuration::from_secs(93_784)), "P1DT2H3M4S");
        assert_eq!(format!("{:?}", SignedDuration::from_secs(86_401)), "P1DT1S");
    }

    #[test]
    fn debug_negative() {
        assert_eq!(format!("{:?}", SignedDuration::from_secs(-4)), "-PT4S");
        assert_eq!(format!("{:?}", SignedDuration::from_secs(-93_784)), "-P1DT2H3M4S");
        assert_eq!(format!("{:?}", SignedDuration::from_nanos(-1_500_000_000)), "-PT1.5S");
    }

    #[test]
    fn debug_fraction() {
        assert_eq!(format!("{:?}", SignedDuration::from_nanos(1_500_000_000)), "PT1.5S");
        assert_eq!(format!("{:?}", SignedDuration::from_nanos(123_456_789)), "PT0.123456789S");
        assert_eq!(format!("{:?}", SignedDuration::from_nanos(1)), "PT0.000000001S");
        assert_eq!(format!("{:?}", SignedDuration::from_nanos(86_400_000_000_001)), "P1DT0.000000001S");
    }
}
