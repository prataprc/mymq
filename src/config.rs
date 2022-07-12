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
    pub max_nodes: Option<u32>,

    /// Fixed number of shards, of session/connections, that can exist in this cluster.
    /// Shards are assigned to nodes.
    /// * **Default**: <number of cores in the node>
    /// * **Mutable**: No
    pub num_shards: Option<u32>,

    /// Network listening port for each node in this cluster. Once the cluster is
    /// spawned it will listen on all the available interfaces using this port.
    /// * **Default**: "0.0.0.0:1883", Refer to [MQTT_PORT]
    /// * **Mutable**: No
    pub port: Option<u16>,

    /// Initial set of nodes that are going be part of this. If not provided, will start
    /// a single node cluster.
    /// * **Default**: [],
    /// * **Mutable**: No
    pub nodes: Vec<ConfigNode>,

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

    /// Maximum packet size allowed by the broker, this shall be communicated with
    /// remote client during handshake.
    /// * **Default**: [Config::DEF_MQTT_MAX_PACKET_SIZE]
    /// * **Mutable**: No
    pub mqtt_max_packet_size: Option<u32>,

    /// MQTT packets are drainded from queues and connections in batches, so that
    /// all queues will get evenly processed. This parameter defines the batch size
    /// while draining the message queues.
    /// * **Default**: [Config::DEF_MQTT_PKT_BATCH_SIZE]
    /// * **Mutable**: No
    pub mqtt_pkt_batch_size: Option<u32>,

    /// MQTT Keep Alive, in secs, that server can suggest to the client. If configured
    /// with non-zero value, clients should use this keep-alive instead of the client
    /// configured keep-alive-timeout.
    /// while draining the message queues.
    /// * **Default**: None,
    /// * **Mutable**: No
    pub mqtt_keep_alive: Option<u32>,

    /// MQTT Keep Alive factor, the final value of `mqtt_keep_alive` is computed by
    /// multiplying the `mqtt_keep_alive` with this factor.
    /// * **Default**: [Config::DEF_MQTT_KEEP_ALIVE_FACTOR]
    /// * **Mutable**: No
    pub mqtt_keep_alive_factor: Option<f32>,

    /// MQTT Receive-maximum, control the number of unacknowledged PUBLISH packets
    /// server can receive and process concurrently for the client.
    /// * **Default**: [Config::DEF_MQTT_RECEIVE_MAXIMUM]
    /// * **Mutable**: No
    pub mqtt_receive_maximum: Option<u16>,

    /// MQTT `session_expiry_interval` on the broker side. If `session_expiry_interval`
    /// is ZERO or None, then `session_expiry_interval` from CONNECT packet is used.
    /// CONNECT has no `session_expiry_interval` interval or it is ZERO, then session
    /// ends immediately at connection close.
    /// * **Default**: None,
    /// * **Mutable**: No
    pub mqtt_session_expiry_interval: Option<u32>,

    /// MQTT `maximum_qos` on the broker side. This is the advertised maximum supported
    /// QoS level by the broker.
    /// * **Default**: [Config::DEF_MQTT_MAX_QOS]
    /// * **Mutable**: No
    pub mqtt_maximum_qos: Option<u8>,

    /// MQTT retain available and supported by broker. Disabling this would disable
    /// retain-messaes on the borker side.
    /// * **Default**: [Config::DEF_MQTT_RETAIN_AVAILABLE]
    /// * **Mutable**: No
    pub mqtt_retain_available: Option<bool>,

    /// MQTT Maximum value for topic_alias allowed. Specifying a value of N would mean
    /// broker can handle N-1 aliases for topic-name. Setting this value to ZERO is
    /// same as specifying None, that is, broker won't accept any topic-aliases.
    /// * **Default**: [Config::DEF_MQTT_TOPIC_ALIAS_MAX]
    /// * **Mutable**: No
    pub mqtt_topic_alias_max: Option<u16>,
}

impl Default for Config {
    fn default() -> Config {
        use crate::util::ceil_power_of_2;

        let num_cores = ceil_power_of_2(u32::try_from(num_cpus::get()).unwrap());
        Config {
            name: "poc".to_string(),
            max_nodes: Some(Config::DEF_MAX_NODES),
            num_shards: Some(num_cores),
            port: Some(MQTT_PORT),
            nodes: Vec::default(),
            connect_timeout: Some(Self::DEF_CONNECT_TIMEOUT),
            mqtt_read_timeout: Some(Self::DEF_MQTT_READ_TIMEOUT),
            mqtt_write_timeout: Some(Self::DEF_MQTT_WRITE_TIMEOUT),
            mqtt_flush_timeout: Some(Self::DEF_MQTT_FLUSH_TIMEOUT),
            mqtt_max_packet_size: Some(Self::DEF_MQTT_MAX_PACKET_SIZE),
            mqtt_pkt_batch_size: Some(Self::DEF_MQTT_PKT_BATCH_SIZE),
            mqtt_keep_alive: None,
            mqtt_keep_alive_factor: Some(Self::DEF_MQTT_KEEP_ALIVE_FACTOR),
            mqtt_receive_maximum: Some(Self::DEF_MQTT_RECEIVE_MAXIMUM),
            mqtt_session_expiry_interval: None,
            mqtt_maximum_qos: Some(Self::DEF_MQTT_MAX_QOS),
            mqtt_retain_available: Some(Self::DEF_MQTT_RETAIN_AVAILABLE),
            mqtt_topic_alias_max: Some(Self::DEF_MQTT_TOPIC_ALIAS_MAX),
        }
    }
}

impl Config {
    /// Refer to [Config::max_nodes]
    pub const DEF_MAX_NODES: u32 = 1;
    /// Refer to [Config::connect_timeout]
    pub const DEF_CONNECT_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::mqtt_read_timeout]
    pub const DEF_MQTT_READ_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::mqtt_write_timeout]
    pub const DEF_MQTT_WRITE_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::mqtt_flush_timeout]
    pub const DEF_MQTT_FLUSH_TIMEOUT: u32 = 10; // in seconds.
    /// Refer to [Config::mqtt_max_packet_size]
    pub const DEF_MQTT_MAX_PACKET_SIZE: u32 = 1024 * 1024; // default is 1MB.
    /// Refer to [Config::mqtt_pkt_batch_size]
    pub const DEF_MQTT_PKT_BATCH_SIZE: u32 = 1024; // default is 1MB.
    /// Refer to [Config::mqtt_keep_alive_factor]
    pub const DEF_MQTT_KEEP_ALIVE_FACTOR: f32 = 1.5; // suggested by the spec.
    /// Refer to [Config::mqtt_receive_maximum]
    pub const DEF_MQTT_RECEIVE_MAXIMUM: u16 = 256;
    /// Refer to [Config::mqtt_maximum_qos]
    pub const DEF_MQTT_MAX_QOS: u8 = 1;
    /// Refer to [Config::mqtt_retain_available]
    pub const DEF_MQTT_RETAIN_AVAILABLE: bool = true;
    /// Refer to [Config::mqtt_topic_alias_max]
    pub const DEF_MQTT_TOPIC_ALIAS_MAX: u16 = 65535;

    /// Construct a new configuration from a file located by `loc`.
    pub fn from_file<P>(loc: P) -> Result<Config>
    where
        P: AsRef<path::Path>,
    {
        load_toml(loc)
    }

    pub fn max_nodes(&self) -> u32 {
        self.max_nodes.unwrap_or(Self::DEF_MAX_NODES)
    }

    pub fn num_shards(&self) -> u32 {
        use crate::util::ceil_power_of_2;

        let num_cores = ceil_power_of_2(u32::try_from(num_cpus::get()).unwrap());
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

    pub fn mqtt_max_packet_size(&self) -> u32 {
        match self.mqtt_max_packet_size {
            Some(val) if val > 268435456 => panic!("mqtt_max_packet_size is {}", val),
            Some(val) => val,
            None => Self::DEF_MQTT_FLUSH_TIMEOUT,
        }
    }

    pub fn mqtt_pkt_batch_size(&self) -> u32 {
        match self.mqtt_pkt_batch_size {
            Some(val) if val > 268435456 => panic!("mqtt_pkt_batch_size is {}", val),
            Some(val) => val,
            None => Self::DEF_MQTT_PKT_BATCH_SIZE,
        }
    }

    pub fn mqtt_keep_alive(&self) -> Option<u32> {
        match self.mqtt_keep_alive {
            Some(0) | None => None,
            Some(val) => Some(val),
        }
    }

    pub fn mqtt_keep_alive_factor(&self) -> f32 {
        self.mqtt_keep_alive_factor.unwrap_or(Self::DEF_MQTT_KEEP_ALIVE_FACTOR)
    }

    pub fn mqtt_receive_maximum(&self) -> u16 {
        self.mqtt_receive_maximum.unwrap_or(Self::DEF_MQTT_RECEIVE_MAXIMUM)
    }

    pub fn mqtt_session_expiry_interval(&self, connect: Option<u32>) -> Option<u32> {
        match connect {
            Some(session_expiry_interval) => Some(session_expiry_interval),
            None => self.mqtt_session_expiry_interval,
        }
    }

    pub fn mqtt_maximum_qos(&self) -> u8 {
        self.mqtt_maximum_qos.unwrap_or(Self::DEF_MQTT_MAX_QOS)
    }

    pub fn mqtt_retain_available(&self) -> bool {
        self.mqtt_retain_available.unwrap_or(Self::DEF_MQTT_RETAIN_AVAILABLE)
    }

    pub fn mqtt_topic_alias_max(&self) -> Option<u16> {
        match &self.mqtt_topic_alias_max {
            Some(0) => None,
            Some(val) => Some(*val),
            None => self.mqtt_topic_alias_max,
        }
    }
}

/// Node configuration
#[derive(Clone, Deserialize)]
pub struct ConfigNode {
    /// Unique identifier for this node within this cluster. There may be other
    /// requirement on the unique-id, like randomness, cyptographic security, public-key.
    /// Refer to package documentation for more detail.
    /// * **Default**: <Shall be generated by the cluster>
    /// * **Mutable**: No
    pub uuid: Option<String>,
    /// Hierarchical path to nodes. Will be useful in selecting replica, and selecting
    /// `bridge-nodes` across the cluster.
    pub path: path::PathBuf,
    /// Weight to be given for each nodes, typically based on the number of cores,
    /// RAM-capacity, network-bandwidth and disk-size.
    /// * **Default**: <number of cores in the node>
    /// * **Mutable**: No
    pub weight: Option<u16>,
    /// MQTT address on which nodes listen, and clients can connect.
    pub mqtt_address: net::SocketAddr,
}

impl Default for ConfigNode {
    fn default() -> ConfigNode {
        use uuid::Uuid;

        ConfigNode {
            mqtt_address: "0.0.0.0:1883".parse().unwrap(),
            path: "/".into(), // TODO: a meaningful path.
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
    let s = err!(InvalidInput, try: from_utf8(&data), "config not utf8 {:?}", ploc)?;
    err!(InvalidInput, try: toml::from_str(s), "config not toml {:?}", ploc)
}
