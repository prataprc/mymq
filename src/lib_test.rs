use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_packetize_u8() {
    let seed = random();
    println!("test_packetize_u8 seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100 {
        let val: u8 = rng.gen();

        let stream: Blob = val.encode().unwrap();
        assert_eq!((val, 1), u8::decode(stream.as_bytes()).unwrap());
        assert_eq!((val, 1), u8::decode_unchecked(stream.as_bytes()));

        let stream: Blob = val.into_blob().unwrap();
        assert_eq!((val, 1), u8::decode(stream.as_bytes()).unwrap());
        assert_eq!((val, 1), u8::decode_unchecked(stream.as_bytes()));
    }
}

#[test]
fn test_packetize_u16() {
    let seed = random();
    println!("test_packetize_u16 seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..1000 {
        let val: u16 = rng.gen();

        let stream: Blob = val.encode().unwrap();
        assert_eq!((val, 2), u16::decode(stream.as_bytes()).unwrap());
        assert_eq!((val, 2), u16::decode_unchecked(stream.as_bytes()));

        let stream: Blob = val.into_blob().unwrap();
        assert_eq!((val, 2), u16::decode(stream.as_bytes()).unwrap());
        assert_eq!((val, 2), u16::decode_unchecked(stream.as_bytes()));
    }
}

#[test]
fn test_packetize_u32() {
    let seed = random();
    println!("test_packetize_u32 seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..10000 {
        let val: u32 = rng.gen();

        let stream: Blob = val.encode().unwrap();
        assert_eq!((val, 4), u32::decode(stream.as_bytes()).unwrap());
        assert_eq!((val, 4), u32::decode_unchecked(stream.as_bytes()));

        let stream: Blob = val.into_blob().unwrap();
        assert_eq!((val, 4), u32::decode(stream.as_bytes()).unwrap());
        assert_eq!((val, 4), u32::decode_unchecked(stream.as_bytes()));
    }
}

#[test]
fn test_packetize_string() {
    use crate::fuzzy;

    let seed: u64 = [11477946006982904015, random()][random::<usize>() % 2];
    println!("test_packetize_string seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut ctx = fuzzy::Context::default();
    for _i in 0..10_000 {
        fuzzy::test::<String>(&mut rng, &mut ctx);
    }
    ctx.print_stats();
}

#[test]
fn test_packetize_bytes() {
    use crate::fuzzy;

    let seed: u64 = random();
    println!("test_packetize_vector seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut ctx = fuzzy::Context::default();
    for _i in 0..10_000 {
        fuzzy::test::<Vec<u8>>(&mut rng, &mut ctx);
    }
    ctx.print_stats();
}
