use crate::broker::Config;

#[derive(Clone, Eq, PartialEq)]
pub struct Topology<N>
where
    N: Clone,
{
    pub shard: u32,
    pub master: N,
    pub replicas: Vec<N>,
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
    pub fn rebalance<N>(&self, nodes: &[N], old: Vec<Topology<N>>) -> Vec<Topology<N>>
    where
        N: Clone,
    {
        self.algo.rebalance(&self.config, nodes, old)
    }
}

pub enum Algorithm {
    SingleNode,
}

impl Algorithm {
    fn rebalance<N>(&self, c: &Config, ns: &[N], _: Vec<Topology<N>>) -> Vec<Topology<N>>
    where
        N: Clone,
    {
        match self {
            Algorithm::SingleNode => {
                let node = &ns[0];
                (0..c.num_shards)
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
pub fn diff_topology<N>(
    olds: &[Topology<N>],
    news: &[Topology<N>],
) -> Vec<(Topology<N>, Topology<N>)>
where
    N: PartialEq + Clone,
{
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
