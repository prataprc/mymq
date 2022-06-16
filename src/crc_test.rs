use rand::{prelude::random, rngs::StdRng, SeedableRng};
use uuid::Uuid;

use super::*;

#[test]
fn test_client_to_shard() {
    let seed = random();
    println!("test_client_to_shard_1 seed:{}", seed);
    let _rng = StdRng::seed_from_u64(seed);

    let keys = 1_000_000;
    let for_shards = [1, 2, 4, 8, 16, 32, 64, 1024];
    for shards in for_shards.into_iter() {
        let ideal = (keys as f32) / (shards as f32);
        let mut shards = vec![0_u32; shards];
        for _i in 0..keys {
            let uuid = Uuid::new_v4();
            let off = client_to_shard(&uuid, shards.len()).unwrap();
            shards[off as usize] += 1;
        }
        let devt_min =
            (((ideal as u32) - shards.iter().min().unwrap()) as f32 / ideal) * 100.0;
        let devt_max =
            ((shards.iter().max().unwrap() - (ideal as u32)) as f32 / ideal) * 100.0;
        println!("min_deviation:{}% max:{}%", devt_min, devt_max);
        assert!(devt_min < 15.0, "{}%", devt_min);
        assert!(devt_max < 15.0, "{}%", devt_min);
    }
}
