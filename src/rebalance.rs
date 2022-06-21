// TODO: This module is not used at present.

/// The premises for rebalancer is that:
///
/// * A Cluster shall be made up of nodes, to host one or more shards.
/// * The complete set of all sessions shall be divided into shards.
/// * Number of shards are fixed when creating a cluster, and cannot change there after.
/// * Number of shards must be power of 2.
/// * Clients are mapped to shards using ClientID, so that a client will always map to
///   the same shard no matter when it is computed.
///  * One or more shards shall be mapped to a node.
///  * There should atleast be as many shards as the CPU cores in a node.
///  * Shard mapping to node is open-ended, Rebalancer can experiment with several
///    algorithsm
///  * Rebalancer shall account for replicas, for fault-tolerance and use consensus
///    protocol between master and replicas for "lossless" implementation of MQTT.
///  * Every master in the cluster shall be wired-up together via consensus protocol
///    for exchanging configuration information.
pub enum Rebalancer {
    CHash(ConsistentHash),
}

impl From<ConsistentHash> for Rebalancer {
    fn from(val: ConsistentHash) -> Rebalancer {
        Rebalancer::CHash(val)
    }
}

impl Rebalancer {
    pub fn add_nodes(&mut self, nodes: &[Node]) -> Result<()> {
        match self {
            Rebalancer::CHash(val) => val.add_nodes(nodes)?,
        };

        Ok(())
    }

    pub fn remove_nodes(&mut self, uuids: &[Uuid]) -> Result<()> {
        let nodes: Vec<Node> =
            uuids.iter().map(|uuid| Node { uuid: *uuid, ..Node::default() }).collect();
        match self {
            Rebalancer::CHash(val) => val.remove_nodes(&nodes)?,
        };

        Ok(())
    }

    pub fn shard_to_node(&self, uuid: Uuid) -> Uuid {
        match self {
            Rebalancer::CHash(val) => val.shard_to_node(uuid),
        }
    }
}

