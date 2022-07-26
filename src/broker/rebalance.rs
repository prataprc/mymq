//! The premises for rebalancer is that:
//!
//! * A Cluster shall be made up of nodes, capable of hosting one or more shards.
//! * Number of shards are fixed when creating a cluster, and cannot change there after.
//! * Number of shards must be power of 2.
//! * MQTT is a state-ful protocol, hence on the broker side, there shall be one `Session`
//!   for each client, identified by ClientID.
//! * Given the ClientID and number of shards, mapping of `Session` to `Shard` is computed
//!   with ZERO knowledge.
//! * There should atleast be as many shards as the CPU cores in a node.
//! * There shall be `one-master` and `zero-or-more-replicas` for each shard.
//! * Shard to node mapping is open-ended, Rebalancer can experiment with several
//!   algorithms, but the end result shall be that:
//!   * Shards, both master and its replicas, shall be distributed evenly across nodes.
//!   * All shards hosted by the same node, shall have same master/replica topology.
//!
//! The scope of Rebalancer is:
//!
//! * Assign session to shard, computed only using ClientID and num_shards.
//! * Assign a node for master shard.
//! * Assign ZERO or more node for replica shards.
//!
//! **Topology**
//!
//! Topology is distribution of shard's master and replicas across the cluster. For each
//! shard, there shall be a [Topology] discription. After every rebalance, whether
//! gracefull or fail-over, a new Topology shall be created that involves minimum
//! shard migration.
//!
//! **Shard-Migration**
//!
//! * Migration of master shard from one node to another.
//! * Migration of replica shard from one node not another.
//! * Demotion of master shard as replica-shard.
//! * Promotion of replica-shard as master-shard.

use crate::broker::Node;
use crate::Config;

#[derive(Clone, Eq, PartialEq)]
pub struct Topology {
    pub shard: u32,
    pub master: Node,
    pub replicas: Vec<Node>,
}

/// Implement rebalancing-algorithm, refer to module documentation.
pub struct Rebalancer {
    pub config: Config,
    pub algo: Algorithm,
}

impl Rebalancer {
    /// Clients are mapped to shards using ClientID, so that a client will always
    /// map to the same shard no matter where or when it is computed.
    pub fn session_partition<U: AsRef<[u8]>>(id: &U, num_shards: u32) -> u32 {
        let id: &[u8] = id.as_ref();
        let hash = cityhash_rs::cityhash_110_128(id);
        let hash = (hash & 0xFFFFFFFFFFFFFFFF) ^ ((hash >> 64) & 0xFFFFFFFFFFFFFFFF);
        let hash = ((hash & 0xFFFFFFFF) ^ ((hash >> 32) & 0xFFFFFFFF)) as u32;
        hash & (num_shards - 1)
    }

    /// Rebalance topology for supplied set of nodes. Subsequently use
    /// [diff_topology] passing in the old and new topology to identify the migrating
    /// shards.
    pub fn rebalance(&self, nodes: &[Node], old: Vec<Topology>) -> Vec<Topology> {
        self.algo.rebalance(&self.config, nodes, old)
    }
}

pub enum Algorithm {
    SingleNode,
}

impl Algorithm {
    fn rebalance(&self, c: &Config, nodes: &[Node], _: Vec<Topology>) -> Vec<Topology> {
        match self {
            Algorithm::SingleNode => {
                let node = &nodes[0];
                (0..c.num_shards())
                    .map(|shard| Topology {
                        shard,
                        master: node.clone(),
                        replicas: Vec::new(), // TODO: with_capacity ?
                    })
                    .collect()
            }
        }
    }
}

/// Compare the old and new topology to identify the migrating shards. For each
/// migrating shards, there shall be an entry in the returned list.
#[allow(dead_code)]
pub fn diff_topology(olds: &[Topology], news: &[Topology]) -> Vec<(Topology, Topology)> {
    let mut olds = olds.to_vec();
    olds.sort_by_key(|x| x.shard);
    let mut news = news.to_vec();
    news.sort_by_key(|x| x.shard);

    assert_eq!(olds.len(), news.len());

    let mut diffs = Vec::new(); // TODO: with_capacity
    for (old, new) in olds.into_iter().zip(news.into_iter()) {
        assert_eq!(old.shard, new.shard);
        if old == new {
            continue;
        } else {
            diffs.push((old, new))
        }
    }

    diffs
}

#[cfg(test)]
#[path = "rebalance_test.rs"]
mod rebalance_test;
