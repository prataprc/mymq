use crate::{Error, ErrorKind, ReasonCode, Result};

pub fn is_invalid_utf8_code_point(ch: char) -> bool {
    let c = ch as u32;
    (c >= 0xD800 && c <= 0xDFFF) || c <= 0x1F || (c >= 0x7F && c <= 0x9F)
}

pub fn u8_to_bool(val: u8, what: &str) -> Result<bool> {
    match val {
        0 => Ok(false),
        1 => Ok(true),
        v => err!(ProtocolError, code: ProtocolError, "invalid bool {:?}, {:?}", what, v),
    }
}

pub fn bool_to_u8(val: bool) -> u8 {
    match val {
        true => 1,
        false => 0,
    }
}

pub fn advance(stream: &[u8], n: usize) -> Result<&[u8]> {
    if n < stream.len() {
        Ok(&stream[n..])
    } else {
        err!(InsufficientBytes, code: MalformedPacket, "insufficient bytes in packet")
    }
}

#[inline]
pub fn is_power_of_2(n: usize) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

pub fn ceil_power_of_2(n: usize) -> usize {
    let mut p = 1_usize;
    while p < n {
        p = p << 1;
    }
    p
}
