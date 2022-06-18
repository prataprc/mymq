use rand::{prelude::random, rngs::StdRng, SeedableRng};

use super::*;
use crate::Shardable;

struct Nd(Uuid, u16);

impl Hostable for Nd {
    fn uuid(&self) -> Uuid {
        self.0
    }

    fn weight(&self) -> u16 {
        self.1
    }
}

struct Sd(Uuid);

impl Shardable for Sd {
    fn uuid(&self) -> Uuid {
        self.0
    }
}

#[test]
fn test_consistent_hash() {
    use std::collections::BTreeMap;

    let seed: u64 = random();
    println!("test_client_to_shard_1 seed:{}", seed);
    let _rng = StdRng::seed_from_u64(seed);

    let n_shards = 32;
    let n_nodes = 2;
    let weight = 128;

    let shards: Vec<Sd> = (0..n_shards).map(|_| Sd(Uuid::new_v4())).collect();
    let nodes: Vec<Nd> = (0..n_nodes).map(|_| Nd(Uuid::new_v4(), weight)).collect();

    let ch = ConsistentHash::from_nodes(&nodes).unwrap();
    println!("{:?}", ch);

    let mut shards_on_node = BTreeMap::new();
    for s in shards.iter() {
        let node_uuid = ch.shard_to_node(s.uuid());
        match shards_on_node.get_mut(&node_uuid) {
            Some(value) => *value += 1,
            None => {
                shards_on_node.insert(node_uuid, 1);
            }
        }
    }

    for (node, n) in shards_on_node.iter() {
        println!("{} {}", node, n);
    }
}
