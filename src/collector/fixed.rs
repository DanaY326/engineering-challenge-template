//! Exact decimal-string to fixed-point conversion.
//!
//! Exchanges send prices and sizes as decimal strings ("0.9600", "-54.00",
//! "219.217767"). Going through `f64` would introduce rounding errors, so we
//! parse the digits directly into the output schema's fixed point, where
//! [`SCALE`] = 1 USDC (price) or 1 contract/share (quantity).

use anyhow::{Context, bail, ensure};

/// 1.0 in the output schema's fixed point.
pub const SCALE: i64 = 1_000_000;
const DECIMALS: usize = 6;

/// Parse a decimal string like `"-54.00"` into fixed point with 6 decimals.
/// Errors (rather than rounds) if the value has more than 6 significant
/// decimals or overflows.
pub fn parse_fixed(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    let (negative, digits) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    ensure!(
        !(int.is_empty() && frac.is_empty()),
        "empty decimal string {s:?}"
    );
    ensure!(
        int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()),
        "invalid decimal string {s:?}"
    );

    let frac = frac.trim_end_matches('0');
    if frac.len() > DECIMALS {
        bail!("{s:?} has more than {DECIMALS} decimals");
    }
    let int_value: i64 = if int.is_empty() { 0 } else { int.parse()? };
    let mut frac_value: i64 = 0;
    for b in frac.bytes() {
        frac_value = frac_value * 10 + i64::from(b - b'0');
    }
    frac_value *= 10_i64.pow((DECIMALS - frac.len()) as u32);

    let value = int_value
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac_value))
        .with_context(|| format!("{s:?} overflows fixed point"))?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exchange_formats() {
        assert_eq!(parse_fixed("0.9600").unwrap(), 960_000);
        assert_eq!(parse_fixed("-54.00").unwrap(), -54_000_000);
        assert_eq!(parse_fixed("219.217767").unwrap(), 219_217_767);
        assert_eq!(parse_fixed("1").unwrap(), 1_000_000);
        assert_eq!(parse_fixed(".5").unwrap(), 500_000);
        assert_eq!(parse_fixed("0.001").unwrap(), 1_000);
        assert_eq!(parse_fixed("0.12345600").unwrap(), 123_456);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(parse_fixed("").is_err());
        assert!(parse_fixed("-").is_err());
        assert!(parse_fixed("1e5").is_err());
        assert!(parse_fixed("0.1234567").is_err());
        assert!(parse_fixed("99999999999999999").is_err());
    }
}
