//! Helper functions.

use crate::{Error, ErrorKind, ReasonCode, Result};

// TODO: validate whether this is what the specification means.
pub fn is_valid_utf8_code_point(ch: char) -> bool {
    let c = ch as u32;
    let invalid = (c >= 0xD800 && c <= 0xDFFF) || c <= 0x1F || (c >= 0x7F && c <= 0x9F);
    !invalid
}

pub fn advance(stream: &[u8], n: usize) -> Result<&[u8]> {
    if n <= stream.len() {
        Ok(&stream[n..])
    } else {
        err!(InsufficientBytes, code: MalformedPacket, "insufficient bytes in packet")
    }
}

#[inline]
pub fn is_power_of_2<T>(n: T) -> bool
where
    T: TryInto<u32>,
    u32: From<T>,
{
    let n = u32::try_from(n).unwrap();
    n != 0 && (n & (n - 1)) == 0
}

#[inline]
pub fn ceil_power_of_2(n: u32) -> u64 {
    match (0..32).skip_while(|i| (1 << i) < n).next() {
        Some(i) => 1 << i,
        None => 1 << 32,
    }
}

#[cfg(test)]
#[path = "util_test.rs"]
mod util_test;
