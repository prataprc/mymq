use std::{fs, net, path};

use crate::util;
use crate::{Error, ErrorKind, Result};

macro_rules! config_field {
    ($table:ident, $field:ident, $config:ident, $($args:tt)+) => {{
        let field = stringify!($field);
        if let Some(val) = $table.get(field) {
            $config.$field = match val.$($args)+ {
                Some(val) => val.parse()?,
                None => err!(
                    InvalidInput,
                    desc: "invalid config field {}, {}", field, val.to_string()
                )?,
            }
        }
    }};
    (opt: $table:ident, $field:ident, $config:ident, $($args:tt)+) => {{
        let field = stringify!($field);
        if let Some(val) = $table.get(field) {
            $config.$field = match val.$($args)+ {
                Some(val) => Some(val.parse()?),
                None => err!(
                    InvalidInput,
                    desc: "invalid config field {}, {}", field, val.to_string()
                )?,
            }
        }
    }};
}

/// Cluster configuration.
#[derive(Clone)]
pub struct Config {
    /// Human readable name of the cluster.
    /// * **Default**: None, must be supplied
    /// * **Mutable**: No
    pub name: String,

    /// Maximum nodes that can exist in this cluster. When `max_nodes` > 1 broker is
    /// created in distributed mode using consensus algorithm (TODO).
    /// * **Default**: [Config::DEF_MAX_NODES].
    /// * **Mutable**: No
    pub max_nodes: u32,

    /// Fixed number of shards, of session/connections, that can exist in this cluster.
    /// Shards are assigned to nodes.
    /// * **Default**: <number of cores in the node>
    /// * **Mutable**: No
    pub num_shards: u32,

    /// Network listening port for each node in this cluster. Once the cluster is
    /// spawned it will listen on all the available interfaces using this port.
    /// * **Default**: "0.0.0.0:1883", Refer to [Config::DEF_MQTT_PORT]
    /// * **Mutable**: No
    pub port: u16,

    /// Initial set of nodes that are going be part of this. If not provided, will start
    /// a single node cluster.
    /// * **Default**: [],
    /// * **Mutable**: No
    pub nodes: Vec<ConfigNode>,

    /// Connect handshake timeout on MQTT socket, in seconds. For every new connection,
    /// this timer will kick in, and within the timeout period if connect/connack
    /// handshake is not complete, connection will be closed.
    /// * **Default**: [Config::DEF_SOCK_MQTT_CONNECT_TIMEOUT]
    /// * **Mutable**: No
    pub sock_mqtt_connect_timeout: u32,

    /// Read timeout on MQTT socket, in seconds. For every new packet this timeout
    /// will kick in, and within the timeout period if a new packet is not completely
    /// read, connection will be closed.
    /// * **Default**: None,
    /// * **Mutable**: No
    pub sock_mqtt_read_timeout: Option<u32>,

    /// Write timeout on MQTT socket, in seconds. For every new packet this timeout
    /// will kick in, and within the timeout period if a new packet is not completely
    /// written, connection will be closed.
    /// * **Default**: None,
    /// * **Mutable**: No
    pub sock_mqtt_write_timeout: Option<u32>,

    /// Flush timeout on MQTT socket, in seconds. If broker decides to shutdown a
    /// connection, because it is broken/half-broken or Malformed packets or due to
    /// ProtocolError, a flush thread will take over the connection and flush
    /// pending packets upstream and downstream. This timeout shall kick in once the
    /// flush thread is spawned and all flush activities are expected to be completed
    /// before the timeout expires.
    /// * **Default**: [Config::DEF_SOCK_MQTT_FLUSH_TIMEOUT]
    /// * **Mutable**: No
    pub sock_mqtt_flush_timeout: u32,

    /// Maximum packet size allowed by the broker, this shall be communicated with
    /// remote client during handshake.
    /// * **Default**: [Config::DEF_MQTT_MAX_PACKET_SIZE]
    /// * **Mutable**: No
    pub mqtt_max_packet_size: u32,

    /// MQTT packets are drainded from queues and connections in batches, so that
    /// all queues will get evenly processed. This parameter defines the batch size
    /// while draining the message queues.
    /// * **Default**: [Config::DEF_MQTT_PKT_BATCH_SIZE]
    /// * **Mutable**: No
    pub mqtt_pkt_batch_size: u32,

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
    pub mqtt_keep_alive_factor: f32,

    /// MQTT Receive-maximum, control the number of unacknowledged PUBLISH packets
    /// server can receive and process concurrently for the client.
    /// * **Default**: [Config::DEF_MQTT_RECEIVE_MAXIMUM]
    /// * **Mutable**: No
    pub mqtt_receive_maximum: u16,

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
    pub mqtt_maximum_qos: u8,

    /// MQTT retain available and supported by broker. Disabling this would disable
    /// retain-messages on the borker side.
    /// * **Default**: [Config::DEF_MQTT_RETAIN_AVAILABLE]
    /// * **Mutable**: No
    pub mqtt_retain_available: bool,

    /// MQTT Maximum value for topic_alias allowed. Specifying a value of N would mean
    /// broker can handle N-1 aliases for topic-name. Setting this value to ZERO is
    /// same as specifying None, that is, broker won't accept any topic-aliases.
    /// * **Default**: [Config::DEF_MQTT_TOPIC_ALIAS_MAX]
    /// * **Mutable**: No
    pub mqtt_topic_alias_max: Option<u16>,

    /// MQTT Ignore duplicate. If the DUP flag is set to 1, it indicates that this
    /// might be re-delivery of an earlier attempt to send the packet.
    /// * **Default**: [Config::DEF_MQTT_IGNORE_DUPLICATE]
    /// * **Mutable**: No
    pub mqtt_ignore_duplicate: bool,

    /// MQTT interval between publish retry for QoS-1/2 messages, in seconds.
    pub mqtt_publish_retry_interval: u32,
}

impl Default for Config {
    fn default() -> Config {
        let node = ConfigNode::default();
        Config {
            name: "mymqd".to_string(),
            max_nodes: Self::DEF_MAX_NODES,
            num_shards: util::num_cores_ceiled(),
            port: Self::DEF_MQTT_PORT,
            nodes: vec![node],
            sock_mqtt_connect_timeout: Self::DEF_SOCK_MQTT_CONNECT_TIMEOUT,
            sock_mqtt_read_timeout: None,
            sock_mqtt_write_timeout: None,
            sock_mqtt_flush_timeout: Self::DEF_SOCK_MQTT_FLUSH_TIMEOUT,
            mqtt_max_packet_size: Self::DEF_MQTT_MAX_PACKET_SIZE,
            mqtt_pkt_batch_size: Self::DEF_MQTT_PKT_BATCH_SIZE,
            mqtt_keep_alive: None,
            mqtt_keep_alive_factor: Self::DEF_MQTT_KEEP_ALIVE_FACTOR,
            mqtt_receive_maximum: Self::DEF_MQTT_RECEIVE_MAXIMUM,
            mqtt_session_expiry_interval: None,
            mqtt_maximum_qos: Self::DEF_MQTT_MAX_QOS,
            mqtt_retain_available: Self::DEF_MQTT_RETAIN_AVAILABLE,
            mqtt_topic_alias_max: Some(Self::DEF_MQTT_TOPIC_ALIAS_MAX),
            mqtt_ignore_duplicate: Self::DEF_MQTT_IGNORE_DUPLICATE,
            mqtt_publish_retry_interval: Self::DEF_MQTT_PUBLISH_RETRY_INTERVAL,
        }
    }
}

impl TryFrom<toml::Value> for Config {
    type Error = Error;

    fn try_from(val: toml::Value) -> Result<Config> {
        let mut def = Config::default();
        match val.as_table() {
            Some(t) => {
                config_field!(t, name, def, as_str());
                config_field!(t, max_nodes, def, as_integer().map(|n| n.to_string()));
                config_field!(t, num_shards, def, as_integer().map(|n| n.to_string()));
                config_field!(t, port, def, as_integer().map(|n| n.to_string()));
                config_field!(
                    t,
                    sock_mqtt_connect_timeout,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    opt: t,
                    sock_mqtt_read_timeout,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    opt: t,
                    sock_mqtt_write_timeout,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    sock_mqtt_flush_timeout,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_max_packet_size,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_pkt_batch_size,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    opt: t,
                    mqtt_keep_alive,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_keep_alive_factor,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_receive_maximum,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    opt: t,
                    mqtt_session_expiry_interval,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_maximum_qos,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_retain_available,
                    def,
                    as_bool().map(|b| b.to_string())
                );
                config_field!(
                    opt: t,
                    mqtt_topic_alias_max,
                    def,
                    as_bool().map(|b| b.to_string())
                );
                config_field!(
                    t,
                    mqtt_ignore_duplicate,
                    def,
                    as_integer().map(|b| b.to_string())
                );
                config_field!(
                    t,
                    mqtt_publish_retry_interval,
                    def,
                    as_bool().map(|b| b.to_string())
                );

                if let Some(val) = t.get("node").map(|v| v.as_array()).flatten() {
                    def.nodes = vec![];
                    for val in val.clone().into_iter() {
                        def.nodes.push(ConfigNode::try_from(val)?);
                    }
                }
            }
            None => (),
        };

        def.num_shards = u32::try_from(util::ceil_power_of_2(def.num_shards)).unwrap();

        Ok(def)
    }
}

impl Config {
    /// Refer to [Config::port]
    pub const DEF_MQTT_PORT: u16 = 1883;
    /// Refer to [Config::max_nodes]
    pub const DEF_MAX_NODES: u32 = 1;
    /// Refer to [Config::sock_mqtt_connect_timeout]
    pub const DEF_SOCK_MQTT_CONNECT_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::sock_mqtt_flush_timeout]
    pub const DEF_SOCK_MQTT_FLUSH_TIMEOUT: u32 = 10; // in seconds.
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
    /// Refer to [Config::mqtt_ignore_duplicate]
    pub const DEF_MQTT_IGNORE_DUPLICATE: bool = true;
    /// Refer to [Config::mqtt_publish_retry_interval]
    pub const DEF_MQTT_PUBLISH_RETRY_INTERVAL: u32 = 5; // in seconds

    /// Construct a new configuration from a file located by `loc`.
    pub fn from_file<P>(loc: P) -> Result<Config>
    where
        P: AsRef<path::Path>,
    {
        use std::str::from_utf8;

        let ploc: &path::Path = loc.as_ref();

        let data = err!(IOError, try: fs::read(ploc), "reading config from {:?}", ploc)?;
        let s = err!(InvalidInput, try: from_utf8(&data), "config not utf8 {:?}", ploc)?;

        let val: toml::Value =
            err!(InvalidInput, try: toml::from_str(s), "config not toml {:?}", ploc)?;

        Config::try_from(val)
    }

    pub fn validate(&self) -> Result<()> {
        let val = self.mqtt_max_packet_size;
        if val > 268435456 {
            err!(InvalidInput, desc: "mqtt_max_packet_size is {}", val)
        } else {
            Ok(())
        }
    }

    pub fn mqtt_keep_alive(&self) -> Option<u32> {
        match self.mqtt_keep_alive {
            Some(0) | None => None,
            Some(val) => Some(val),
        }
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
#[derive(Clone)]
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
