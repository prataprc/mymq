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

pub fn num_cores_ceiled() -> u32 {
    u32::try_from(ceil_power_of_2(u32::try_from(num_cpus::get()).unwrap())).unwrap()
}

/// Trait to pretty print a collection in row/column format.
#[cfg(feature = "prettytable-rs")]
pub trait PrettyRow {
    fn to_format() -> prettytable::format::TableFormat;

    fn to_head() -> prettytable::Row;

    fn to_row(&self) -> prettytable::Row;
}

/// Convert a collection of rows into table.
#[cfg(feature = "prettytable-rs")]
pub fn make_table<R>(rows: &[R]) -> prettytable::Table
where
    R: PrettyRow,
{
    let mut table = prettytable::Table::new();

    match rows.len() {
        0 => table,
        _ => {
            table.set_titles(R::to_head());
            rows.iter().for_each(|r| {
                table.add_row(r.to_row());
            });
            table.set_format(R::to_format());
            table
        }
    }
}

#[cfg(test)]
#[path = "util_test.rs"]
mod util_test;
