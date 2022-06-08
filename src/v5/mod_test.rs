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
    use crate::fuzzy;

    let seed: u64 = random();
    println!("test_fixed_header seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut ctx = fuzzy::Context::default();
    for _i in 0..10_000 {
        fuzzy::test::<FixedHeader>(&mut rng, &mut ctx);
    }
    ctx.print_stats();
}
