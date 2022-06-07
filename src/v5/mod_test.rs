use arbitrary::Unstructured;
use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_varu32() {
    let seed: u64 = random();
    println!("test_varu32 seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100_000 {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let ref_val: VarU32 = uns.arbitrary().unwrap();
        if ref_val <= VarU32::MAX {
            let blob = ref_val.encode().unwrap();
            assert_eq!(blob, ref_val.into_blob().unwrap());

            let (val, n) = VarU32::decode(blob.as_bytes()).unwrap();
            assert_eq!(val, ref_val);
            assert_eq!(n, blob.as_ref().len());
        } else {
            assert_eq!(ref_val.encode().is_err(), true);
        }
    }
}

#[test]
fn test_fixed_header() {
    let seed: u64 = random();
    println!("test_fixed_header seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100_000 {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let remaining_len = VarU32(rng.gen::<u32>() % *VarU32::MAX);
        let ref_fh = match rng.gen::<bool>() {
            true => FixedHeader::new(uns.arbitrary().unwrap(), remaining_len).unwrap(),
            false => {
                let qos = uns.arbitrary().unwrap();
                FixedHeader::new_pubish(rng.gen(), qos, rng.gen(), remaining_len).unwrap()
            }
        };

        let blob = ref_fh.encode().unwrap();
        assert_eq!(blob, ref_fh.into_blob().unwrap());

        let (fh, n) = FixedHeader::decode(blob.as_bytes()).unwrap();
        assert_eq!(fh, ref_fh);
        assert_eq!(n, blob.as_ref().len());
    }
}

#[test]
fn test_user_pair() {
    use crate::fuzzy;

    let seed: u64 = random();
    println!("test_user_pair seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100_000 {
        let bytes: Vec<u8> = fuzzy::dna_string(&mut rng, 256);
        let mut uns = Unstructured::new(&bytes);

        let key: String = uns.arbitrary().unwrap();
        let val: String = uns.arbitrary().unwrap();
        let ref_pair = (key, val);

        let blob = ref_pair.encode().unwrap();
        assert_eq!(blob, ref_pair.clone().into_blob().unwrap());

        let (pair, n) = UserPair::decode(blob.as_bytes()).unwrap();
        assert_eq!(pair, ref_pair);
        assert_eq!(n, blob.as_ref().len());
    }
}
