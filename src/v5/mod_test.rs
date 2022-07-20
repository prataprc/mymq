use arbitrary::Unstructured;
use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;

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
