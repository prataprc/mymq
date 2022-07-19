use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use crate::{ErrorKind, ReasonCode};

use super::*;

#[test]
fn test_advance() {
    let stream = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    for i in 0..stream.len() {
        let out = advance(&stream, i).unwrap();
        assert_eq!(out, &stream[i..]);
    }

    assert_eq!(advance(&vec![], 0).unwrap().len(), 0);
    assert_eq!(advance(&vec![], 1).unwrap_err().kind(), ErrorKind::InsufficientBytes);
    assert_eq!(advance(&vec![], 1).unwrap_err().code(), ReasonCode::MalformedPacket);
}

#[test]
fn test_is_power_of_2() {
    let seed = random();
    println!("test_is_power_of_2 seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let power_of_2: Vec<u32> = (0..32).map(|i| 1 << i).collect();

    for _i in 0..1_000_000 {
        let num = rng.gen::<u32>();
        if power_of_2.contains(&num) {
            assert_eq!(is_power_of_2(num), true);
        } else {
            assert_eq!(is_power_of_2(num), false);
        }
    }
}

#[test]
fn test_ceil_power_of_2() {
    let seed = random();
    println!("test_ceil_power_of_2 seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let power_of_2: Vec<u64> = (0..33).map(|i| 1 << i).collect();

    let num = 1_u32;
    let out = ceil_power_of_2(num);
    assert_eq!(out, 1);

    for _i in 0..1_000_000 {
        let num = rng.gen::<u32>();
        let out = ceil_power_of_2(num);
        match power_of_2.binary_search(&out) {
            Ok(off) => {
                assert_eq!(out, power_of_2[off]);
                assert!(u64::from(num) > power_of_2[off - 1]);
            }
            Err(_) => panic!("ceil_power_of_2({}) gives {} not a power of 2", num, out),
        }
    }
}
