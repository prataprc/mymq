use bytes::Bytes;
use rand::{prelude::random, rngs::StdRng, SeedableRng};

use super::*;

#[test]
fn test_read_u16() {
    let seed = random();
    println!("test_read_u16 seed:{}", seed);
    let mut _rng = StdRng::seed_from_u64(seed);

    let mut bytes = Bytes::from([0xff, 0x78].as_slice());
    assert_eq!(read_u16(&mut bytes).unwrap(), 0xff78);

    let err = read_u16(&mut bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::MalformedPacket);
    assert_eq!(err.code(), Some(ReasonCode::MalformedPacket));
}

#[test]
fn test_read_u32() {
    let seed = random();
    println!("test_read_u32 seed:{}", seed);
    let mut _rng = StdRng::seed_from_u64(seed);

    let mut bytes = Bytes::from([0xff, 0x12, 0xaa, 0x45].as_slice());
    assert_eq!(read_u32(&mut bytes).unwrap(), 0xff12aa45);

    let err = read_u32(&mut bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::MalformedPacket);
    assert_eq!(err.code(), Some(ReasonCode::MalformedPacket));
}
