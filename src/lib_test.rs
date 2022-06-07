use arbitrary::Unstructured;
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
    let seed: u64 = random();
    println!("test_packetize_string seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..1000 {
        let val: String = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };
        let len = val.len();

        let stream: Blob = val.encode().unwrap();
        assert_eq!((val.clone(), 2 + len), String::decode(stream.as_bytes()).unwrap());
        assert_eq!((val.clone(), 2 + len), String::decode_unchecked(stream.as_bytes()));

        let stream: Blob = val.clone().into_blob().unwrap();
        assert_eq!((val.clone(), 2 + len), String::decode(stream.as_bytes()).unwrap());
        assert_eq!((val.clone(), 2 + len), String::decode_unchecked(stream.as_bytes()));
    }
}

#[test]
fn test_packetize_vector() {
    let seed: u64 = random();
    println!("test_packetize_vector seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..1000 {
        let val: Vec<u8> = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };
        let len = val.len();

        let stream: Blob = val.encode().unwrap();
        assert_eq!((val.clone(), 2 + len), Vec::<u8>::decode(stream.as_bytes()).unwrap());
        assert_eq!(
            (val.clone(), 2 + len),
            Vec::<u8>::decode_unchecked(stream.as_bytes())
        );

        let stream: Blob = val.clone().into_blob().unwrap();
        assert_eq!((val.clone(), 2 + len), Vec::<u8>::decode(stream.as_bytes()).unwrap());
        assert_eq!(
            (val.clone(), 2 + len),
            Vec::<u8>::decode_unchecked(stream.as_bytes())
        );
    }
}
