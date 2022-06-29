use super::*;

#[test]
fn test_session_to_shard() {
    let num_shards = 4096_u32;
    let num_sessions = 1024 * 1024 * 128;

    let mut shards = vec![0; num_shards as usize];
    let r = Rebalancer { num_shards };
    for _ in 0..num_sessions {
        shards[r.session_to_shard(&uuid::Uuid::new_v4()) as usize] += 1;
    }

    let mean = (num_sessions / num_shards) as i32;
    let total: i32 = shards
        .iter()
        .map(|n| {
            let a = (*n as i32) - mean;
            a * a
        })
        .sum();
    let sd = ((total / (num_shards as i32)) as f32).sqrt();
    println!(
        "mean:{} standard-deviation:{:.2} {:.2}%",
        mean,
        sd,
        (sd / (mean as f32)) * 100.0
    );
}
