use serde::de::DeserializeOwned;
use serde::Deserialize;

use std::{fs, net, path};

use crate::MQTT_PORT;
use crate::{Error, ErrorKind, Result};

/// Cluster configuration.
#[derive(Clone, Deserialize)]
pub struct Config {
    /// Human readable name of the cluster.
    /// * **Default**: None, must be supplied
    /// * **Mutable**: No
    pub name: String,

    /// Maximum nodes that can exist in this cluster, this is the limitation on
    /// federated nodes.
    /// * **Default**: [Config::DEF_MAX_NODES].
    /// * **Mutable**: No
    pub max_nodes: Option<usize>,

    /// Fixed number of shards, of session/connections, that can exist in this cluster.
    /// Shards are assigned to nodes.
    /// * **Default**: <number of cores in the node>
    /// * **Mutable**: No
    pub num_shards: Option<usize>,

    /// Network listening port for each node in this cluster. Once the cluster is
    /// spawned it will listen on all the available interfaces using this port.
    /// * **Default**: "0.0.0.0:1883", Refer to [MQTT_PORT]
    /// * **Mutable**: No
    pub port: Option<u16>,

    /// Initial set of nodes that are going control this cluster. If not provided,
    /// will start a single node cluster.
    /// * **Default**: [],
    /// * **Mutable**: No
    pub gods: Vec<ConfigNode>,

    /// Connect handshake timeout on MQTT socket, in seconds. For every new connection,
    /// this timer will kick in, and within the timeout period if connect/connack
    /// handshake is not complete, connection will be closed.
    /// * **Default**: [Config::DEF_CONNECT_TIMEOUT]
    /// * **Mutable**: No
    pub connect_timeout: Option<u32>,

    /// Read timeout on MQTT socket, in seconds. For every new packet this timeout
    /// will kick in, and within the timeout period if a new packet is not completely
    /// read, connection will be closed.
    /// * **Default**: [Config::DEF_MQTT_READ_TIMEOUT]
    /// * **Mutable**: No
    pub mqtt_read_timeout: Option<u32>,

    /// Write timeout on MQTT socket, in seconds. For every new packet this timeout
    /// will kick in, and within the timeout period if a new packet is not completely
    /// written, connection will be closed.
    /// * **Default**: [Config::DEF_MQTT_WRITE_TIMEOUT]
    /// * **Mutable**: No
    pub mqtt_write_timeout: Option<u32>,

    /// Flush timeout on MQTT socket, in seconds. If broker decides to shutdown a
    /// connection, because it is broken/half-broken or Malformed packets or due to
    /// ProtocolError, a flush thread will take over the connection and flush
    /// pending packets upstream and downstream. This timeout shall kick in once the
    /// flush thread is spawned and all flush activities are expected to be completed
    /// before the timeout expires.
    /// * **Default**: [Config::DEF_MQTT_FLUSH_TIMEOUT]
    /// * **Mutable**: No
    pub mqtt_flush_timeout: Option<u32>,

    /// Subscribe ack timeout, in secs, after receiving a subscribe/un-subscribe
    /// message, broker immediately sends corresponding ACK. If outbound queue to the
    /// client is full, tthis timeout will kick in. If broker cannot send an ACK
    /// within that timeout, session/connection shall be closed.
    pub subscribe_ack_timeout: Option<u32>,

    /// Publish ack timeout, in secs, after receiving a publish message, with QoS-0 or
    /// QoS-1, broker immediately sends corresponding ACK. If outbound queue to the
    /// client is full, tthis timeout will kick in. If broker cannot send an ACK
    /// within that timeout, session/connection shall be closed.
    pub publish_ack_timeout: Option<u32>,
}

impl Default for Config {
    fn default() -> Config {
        use crate::util::ceil_power_of_2;

        let num_cores = ceil_power_of_2(usize::try_from(num_cpus::get()).unwrap());
        Config {
            name: "poc".to_string(),
            max_nodes: Some(Config::DEF_MAX_NODES),
            num_shards: Some(num_cores),
            port: Some(MQTT_PORT),
            gods: Vec::default(),
            connect_timeout: Some(Self::DEF_CONNECT_TIMEOUT),
            mqtt_read_timeout: Some(Self::DEF_MQTT_READ_TIMEOUT),
            mqtt_write_timeout: Some(Self::DEF_MQTT_WRITE_TIMEOUT),
            mqtt_flush_timeout: Some(Self::DEF_MQTT_FLUSH_TIMEOUT),
            subscribe_ack_timeout: None,
            publish_ack_timeout: None,
        }
    }
}

impl Config {
    /// Refer to [Config::max_nodes]
    const DEF_MAX_NODES: usize = 1;
    /// Refer to [Config::connect_timeout]
    const DEF_CONNECT_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::mqtt_read_timeout]
    const DEF_MQTT_READ_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::mqtt_write_timeout]
    const DEF_MQTT_WRITE_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::mqtt_flush_timeout]
    const DEF_MQTT_FLUSH_TIMEOUT: u32 = 10; // in seconds.

    /// Construct a new configuration from a file located by `loc`.
    pub fn from_file<P>(loc: P) -> Result<Config>
    where
        P: AsRef<path::Path>,
    {
        load_toml(loc)
    }

    pub fn max_nodes(&self) -> usize {
        self.max_nodes.unwrap_or(Self::DEF_MAX_NODES)
    }

    pub fn num_shards(&self) -> usize {
        use crate::util::ceil_power_of_2;

        let num_cores = ceil_power_of_2(usize::try_from(num_cpus::get()).unwrap());
        self.num_shards.unwrap_or(num_cores)
    }

    pub fn connect_timeout(&self) -> u32 {
        self.connect_timeout.unwrap_or(Self::DEF_CONNECT_TIMEOUT)
    }

    pub fn mqtt_read_timeout(&self) -> u32 {
        self.mqtt_read_timeout.unwrap_or(Self::DEF_MQTT_READ_TIMEOUT)
    }

    pub fn mqtt_write_timeout(&self) -> u32 {
        self.mqtt_write_timeout.unwrap_or(Self::DEF_MQTT_WRITE_TIMEOUT)
    }

    pub fn mqtt_flush_timeout(&self) -> u32 {
        self.mqtt_flush_timeout.unwrap_or(Self::DEF_MQTT_FLUSH_TIMEOUT)
    }
}

/// Node configuration
#[derive(Clone, Deserialize)]
pub struct ConfigNode {
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
    /// Unique identifier for this node within this cluster. There may be other
    /// requirement on the unique-id, like randomness, cyptographic security, public-key.
    /// Refer to package documentation for more detail.
    /// * **Default**: <Shall be generated by the cluster>
    /// * **Mutable**: No
    pub uuid: Option<String>,
}

impl Default for ConfigNode {
    fn default() -> ConfigNode {
        use uuid::Uuid;

        ConfigNode {
            mqtt_address: "127.0.0.1:1883".parse().unwrap(),
            path: "/".into(),
            weight: Some(u16::try_from(num_cpus::get()).unwrap()),
            uuid: Some(Uuid::new_v4().to_string()),
        }
    }
}

fn load_toml<P, T>(loc: P) -> Result<T>
where
    P: AsRef<path::Path>,
    T: DeserializeOwned,
{
    use std::str::from_utf8;

    let ploc: &path::Path = loc.as_ref();
    let data = err!(IOError, try: fs::read(ploc), "reading config from {:?}", ploc)?;
    let s = err!(FailConvert, try: from_utf8(&data), "config not utf8 {:?}", ploc)?;
    err!(FailConvert, try: toml::from_str(s), "config not toml {:?}", ploc)
}