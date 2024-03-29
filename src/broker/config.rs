use std::{fmt, net, path, result};

use crate::{Error, ErrorKind, Result};

/// Broker configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct Config {
    /// Human readable name of the broker-cluster.
    ///
    /// * **Default**: None, must be supplied
    /// * **Mutable**: No
    pub name: String,

    /// Maximum nodes that can exist in this cluster. When `max_nodes` > 1 broker is
    /// created in distributed mode using consensus algorithm (TODO).
    ///
    /// * **Default**: [Config::DEF_MAX_NODES].
    /// * **Mutable**: No
    pub max_nodes: u32,

    /// Fixed number of shards, of session/connections, that can exist in this cluster.
    /// Shards are assigned to nodes.
    ///
    /// * **Default**: <number of cores in the node>
    /// * **Mutable**: No
    pub num_shards: u32,

    /// Initial set of nodes that are going be part of this. If not provided, will start
    /// a single node cluster.
    ///
    /// * **Default**: [],
    /// * **Mutable**: No
    pub nodes: Vec<ConfigNode>,

    /// Flush timeout on message-queue socket, in seconds. If broker decides to shutdown
    /// a connection, because it is broken/half-broken or Malformed packets or due to
    /// ProtocolError, a flush thread will take over the connection and flush
    /// pending packets upstream and downstream. This timeout shall kick in once the
    /// flush thread receives flush-request. All flush activities are expected to be
    /// completed before the timeout expires.
    ///
    /// * **Default**: [Config::DEF_FLUSH_TIMEOUT]
    /// * **Mutable**: No
    pub flush_timeout: u32,

    /// Packets are read and written to sockets in batches. This parameter defines the
    /// batch size.
    ///
    /// * **Default**: [Config::DEF_PKT_BATCH_SIZE]
    /// * **Mutable**: No
    pub pkt_batch_size: u32,

    /// Interval between publish retry for QoS-1/2 messages, in seconds.
    ///
    /// * **Default**: [Config::DEF_PUBLISH_RETRY_INTERVAL]
    /// * **Mutable**: No
    pub publish_retry_interval: u32,

    /// Client ID generator.
    ///
    /// * **Default**: [Config::DEF_CLIENT_ID_GENERATOR]
    /// * **Mutable**: No
    pub client_id_generator: String,
}

impl Default for Config {
    fn default() -> Config {
        use crate::util::num_cores_ceiled;

        let node = ConfigNode::default();
        Config {
            name: "mymqd".to_string(),
            max_nodes: Self::DEF_MAX_NODES,
            num_shards: num_cores_ceiled(),
            nodes: vec![node],
            flush_timeout: Self::DEF_FLUSH_TIMEOUT,
            pkt_batch_size: Self::DEF_PKT_BATCH_SIZE,
            publish_retry_interval: Self::DEF_PUBLISH_RETRY_INTERVAL,
            client_id_generator: Self::DEF_CLIENT_ID_GENERATOR.to_string(),
        }
    }
}

impl TryFrom<toml::Value> for Config {
    type Error = Error;

    fn try_from(val: toml::Value) -> Result<Config> {
        use crate::util::ceil_power_of_2;

        let mut def = Config::default();
        match val.as_table() {
            Some(t) => {
                config_field!(t, name, def, as_str());
                config_field!(t, max_nodes, def, as_integer().map(|n| n.to_string()));
                config_field!(t, num_shards, def, as_integer().map(|n| n.to_string()));
                config_field!(t, flush_timeout, def, as_integer().map(|n| n.to_string()));
                config_field!(
                    t,
                    pkt_batch_size,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    publish_retry_interval,
                    def,
                    as_bool().map(|b| b.to_string())
                );
                config_field!(t, client_id_generator, def, as_str());

                if let Some(val) = t.get("node").map(|v| v.as_array()).flatten() {
                    def.nodes = vec![];
                    for val in val.clone().into_iter() {
                        def.nodes.push(ConfigNode::try_from(val)?);
                    }
                }
            }
            None => (),
        };

        def.num_shards = u32::try_from(ceil_power_of_2(def.num_shards)).unwrap();

        Ok(def)
    }
}

impl Config {
    /// Refer to [Config::max_nodes]
    pub const DEF_MAX_NODES: u32 = 1;
    /// Refer to [Config::flush_timeout]
    pub const DEF_FLUSH_TIMEOUT: u32 = 10; // in seconds.
    /// Refer to [Config::pkt_batch_size]
    pub const DEF_PKT_BATCH_SIZE: u32 = 1024; // default is 1MB.
    /// Refer to [Config::publish_retry_interval]
    pub const DEF_PUBLISH_RETRY_INTERVAL: u32 = 5; // in seconds
    /// Refer to [Config::client_id_generator]
    pub const DEF_CLIENT_ID_GENERATOR: &'static str = "uuid_v4";

    pub fn validate(&self) -> Result<()> {
        use crate::util::ceil_power_of_2;

        if self.max_nodes > 1 {
            err!(InvalidConfig, desc: "max_nodes:{} > 1", self.max_nodes)?
        }

        if u64::from(self.num_shards) != ceil_power_of_2(self.num_shards) {
            err!(InvalidConfig, desc: "num_shards:{} not power of 2", self.num_shards)?
        }

        match self.client_id_generator.as_str() {
            "uuid_v4" => (),
            val => err!(InvalidConfig, desc: "client_id_generator:{} invalid", val)?,
        };

        Ok(())
    }
}

/// Node configuration
#[derive(Clone, Eq, PartialEq)]
pub struct ConfigNode {
    /// Unique identifier for this node within this cluster. There may be other
    /// requirement on the unique-id, like randomness, cyptographic security, public-key.
    /// Refer to package documentation for more detail.
    /// * **Default**: <Shall be generated by the cluster>
    /// * **Mutable**: No
    pub uuid: String,
    /// MQTT address on which nodes listen, and clients can connect.
    pub mqtt_address: net::SocketAddr,
    /// Hierarchical path to nodes. Will be useful in selecting replica, and selecting
    /// `bridge-nodes` across the cluster.
    pub path: path::PathBuf,
    /// Weight to be given for each nodes, typically based on the number of cores,
    /// RAM-capacity, network-bandwidth and disk-size.
    /// * **Default**: <number of cores in the node>
    /// * **Mutable**: No
    pub weight: Option<u16>,
}

impl Default for ConfigNode {
    fn default() -> ConfigNode {
        use uuid::Uuid;

        ConfigNode {
            uuid: Uuid::new_v4().to_string(),
            mqtt_address: "0.0.0.0:1883".parse().unwrap(),
            path: "/".into(), // TODO: a meaningful path.
            weight: Some(u16::try_from(num_cpus::get()).unwrap()),
        }
    }
}

impl fmt::Debug for ConfigNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.uuid)
    }
}

impl TryFrom<toml::Value> for ConfigNode {
    type Error = Error;

    fn try_from(val: toml::Value) -> Result<ConfigNode> {
        let mut def = ConfigNode::default();

        match val.as_table() {
            Some(t) => {
                config_field!(t, uuid, def, as_str().map(|s| s.to_string()));
                config_field!(t, path, def, as_str());
                config_field!(opt: t, weight, def, as_integer().map(|s| s.to_string()));
                config_field!(t, mqtt_address, def, as_str());
            }
            None => (),
        }

        Ok(def)
    }
}
