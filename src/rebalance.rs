//! The premises for rebalancer is that:
//!
//! * A Cluster shall be made up of nodes, to host one or more shards.
//! * The complete set of all sessions shall be divided into shards.
//! * Number of shards are fixed when creating a cluster, and cannot change there after.
//! * Number of shards must be power of 2.
//! * There should atleast be as many shards as the CPU cores in a node.
//! * There shall be `one-master` and `zero-or-more-replicas` for each shard.
//! * Shard mapping to node is open-ended, Rebalancer can experiment with several
//!   algorithms, but the end result shall be that, shards, both master and its replicas,
//!   shall be distributed across nodes.
//!
//! The scope of Rebalancer is:
//!
//! * Assign session to shard.
//! * Assign a node for master shard.
//! * Assign ZERO or more node for replica shards.
//!
//! **Topology**
//!
//! Topology is distribution of shard's master and replicas across the nodes. For each
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

use crate::Node;

#[derive(Clone, Eq, PartialEq)]
pub struct Topology {
    pub shard: u32,
    pub master: Node,
    pub replicas: Vec<Node>,
}

/// Implement rebalancing-algorithm, refer to module documentation.
pub struct Rebalancer {
    pub num_shards: u32,
    pub algo: Algorithm,
}

impl Rebalancer {
    /// Clients are mapped to shards using ClientID, so that a client will always
    /// map to the same shard no matter where or when it is computed.
    pub fn session_parition<U: AsRef<[u8]>>(&self, id: &U) -> u32 {
        let id: &[u8] = id.as_ref();
        let hash = cityhash_rs::cityhash_110_128(id);
        let hash = (hash & 0xFFFFFFFFFFFFFFFF) ^ ((hash >> 64) & 0xFFFFFFFFFFFFFFFF);
        let hash = ((hash & 0xFFFFFFFF) ^ ((hash >> 32) & 0xFFFFFFFF)) as u32;
        hash & (self.num_shards - 1)
    }

    /// Add new set of nodes into the cluster. Return a new topology. Subsequently use
    /// [diff_topology] passing in the old and new topology to identify the migrating
    /// shards.
    pub fn add_nodes(&self, ns: &[Node], topl: Vec<Topology>) -> Vec<Topology> {
        self.algo.add_nodes(ns, topl)
    }

    /// Remove an existing set of nodes from the cluster. Return a new topology.
    /// Subsequently use [diff_topology] passing in the old and new topology to
    /// identify the migrating shards.
    pub fn del_nodes(&self, ns: &[Node], topl: Vec<Topology>) -> Vec<Topology> {
        self.algo.del_nodes(ns, topl)
    }
}

pub enum Algorithm {
    SingleNode,
}

impl Algorithm {
    fn add_nodes(&self, _nodes: &[Node], topl: Vec<Topology>) -> Vec<Topology> {
        match self {
            Algorithm::SingleNode => topl,
        }
    }

    fn del_nodes(&self, _nodes: &[Node], topl: Vec<Topology>) -> Vec<Topology> {
        match self {
            Algorithm::SingleNode => topl,
        }
    }
}

/// Compare the old and new topology to identify the migrating shards. For each
/// migrating shards, there shall be an entry in the returned list.
pub fn diff_topology(olds: &[Topology], news: &[Topology]) -> Vec<(Topology, Topology)> {
    let mut olds = olds.to_vec();
    olds.sort_by_key(|x| x.shard);
    let mut news = news.to_vec();
    news.sort_by_key(|x| x.shard);

    assert_eq!(olds.len(), news.len());

    let mut diffs = vec![];
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
