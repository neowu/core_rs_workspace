use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

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
            write!(f, "{sign}{value}")
        } else {
            write!(f, "{sign}{}.{:0width$}", value / scale, value % scale, width = S as usize)
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
    use framework::json;

    use super::Decimal64;

    // this is what ClickHouse logs as a query param
    #[test]
    fn debug_format() {
        assert_eq!(format!("{:?}", Decimal64::<6>::from_f64(12.345_678)), "12.345678");
        assert_eq!(format!("{:?}", Decimal64::<6>::from_f64(-0.000_001)), "-0.000001");
        assert_eq!(format!("{:?}", Decimal64::<2>::from_f64(-12.3)), "-12.30");
        assert_eq!(format!("{:?}", Decimal64::<0>::from_f64(42.0)), "42");
        // past the 15-16 significant digits of f64, so this is what integer math buys
        assert_eq!(format!("{:?}", Decimal64::<6>(i64::MIN)), "-9223372036854.775808");
    }

    #[test]
    fn from_f64() {
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
    fn to_f64() {
        assert_eq!(Decimal64::<6>(12_345_678).to_f64(), 12.345_678);
        assert_eq!(Decimal64::<6>(-1).to_f64(), -0.000_001);
        assert_eq!(Decimal64::<0>(42).to_f64(), 42.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn f64_round_trip() {
        let amount = 1_234_567_890.123_456;
        assert_eq!(f64::from(Decimal64::<6>::from(amount)), amount);
    }

    #[test]
    fn serde() {
        let decimal: Decimal64<6> = json::from_json("12345678").unwrap();
        assert_eq!(decimal, Decimal64(12_345_678));
        assert_eq!(json::to_json(&decimal).unwrap(), "12345678");
    }
}
