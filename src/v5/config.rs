use crate::F32;
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Eq, PartialEq)]
pub struct Config {
    /// Spawn MQTT listener, listening on [Config::mqtt_port].
    pub mqtt_listener: bool,

    /// MQTT network port for each node in this cluster. Once the cluster is
    /// spawned it will listen on all the available interfaces using this port.
    /// * **Default**: "0.0.0.0:1883", Refer to [Config::DEF_MQTT_PORT]
    /// * **Mutable**: No
    pub mqtt_port: u16,

    /// Maximum packet size allowed by the broker, this shall be communicated with
    /// remote client during handshake.
    ///
    /// * **Default**: [Config::DEF_MQTT_MAX_PACKET_SIZE]
    /// * **Mutable**: No
    pub mqtt_max_packet_size: u32,

    /// Read timeout on MQTT socket, in seconds. For every new packet this timeout
    /// will kick in, and within the timeout period if a new packet is not completely
    /// read, connection will be closed.
    ///
    /// * **Default**: None,
    /// * **Mutable**: No
    pub mqtt_sock_read_timeout: Option<u32>,

    /// Write timeout on MQTT socket, in seconds. For every new packet this timeout
    /// will kick in, and within the timeout period if a new packet is not completely
    /// written, connection will be closed.
    ///
    /// * **Default**: None,
    /// * **Mutable**: No
    pub mqtt_sock_write_timeout: Option<u32>,

    /// Connect handshake timeout on MQTT socket, in seconds. For every new
    /// connection, this timer will kick in, and within the timeout period if
    /// connect/ack handshake is not complete, connection will be closed.
    ///
    /// * **Default**: [Config::DEF_CONNECT_TIMEOUT]
    /// * **Mutable**: No
    pub mqtt_connect_timeout: u32,

    /// Keep Alive in secs, that broker can suggest to the client. If configured
    /// with non-zero value, clients should use this keep-alive instead of the client
    /// configured keep-alive-timeout.
    ///
    /// * **Default**: None,
    /// * **Mutable**: No
    pub mqtt_keep_alive: Option<u16>,

    /// Keep Alive factor, the final value of `mqtt_keep_alive` is computed by
    /// multiplying the `mqtt_keep_alive` value with this factor.
    ///
    /// * **Default**: [Config::DEF_KEEP_ALIVE_FACTOR]
    /// * **Mutable**: No
    pub mqtt_keep_alive_factor: F32,

    /// MQTT `session_expiry_interval` on the broker side. If `session_expiry_interval`
    /// is ZERO or None, then `session_expiry_interval` from CONNECT packet is used.
    /// CONNECT has no `session_expiry_interval` interval or it is ZERO, then session
    /// ends immediately at connection close.
    ///
    /// * **Default**: None,
    /// * **Mutable**: No
    pub mqtt_session_expiry_interval: Option<u32>,

    /// MQTT `mqtt_maximum_qos` on the broker side. This is the advertised maximum
    /// supported QoS level by the broker.
    ///
    /// * **Default**: [Config::DEF_MAX_QOS]
    /// * **Mutable**: No
    pub mqtt_maximum_qos: u8,

    /// MQTT Receive-maximum, control the number of unacknowledged PUBLISH packets
    /// server can receive and process concurrently for the client.
    ///
    /// * **Default**: [Config::DEF_MQTT_RECEIVE_MAXIMUM]
    /// * **Mutable**: No
    pub mqtt_receive_maximum: u16,

    /// MQTT retain available and supported by broker. Disabling this would disable
    /// retain-messages on the borker side.
    ///
    /// * **Default**: [Config::DEF_MQTT_RETAIN_AVAILABLE]
    /// * **Mutable**: No
    pub mqtt_retain_available: bool,

    /// MQTT Maximum value for topic_alias allowed. Specifying a value of N would mean
    /// broker can handle N-1 aliases for topic-name. Setting this value to ZERO is
    /// same as specifying None, that is, broker won't accept any topic-aliases.
    ///
    /// * **Default**: [Config::DEF_MQTT_TOPIC_ALIAS_MAX]
    /// * **Mutable**: No
    pub mqtt_topic_alias_max: Option<u16>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            mqtt_listener: Self::DEF_MQTT_LISTENER,
            mqtt_port: Self::DEF_MQTT_PORT,
            mqtt_max_packet_size: Self::DEF_MQTT_MAX_PACKET_SIZE,
            mqtt_sock_read_timeout: None,
            mqtt_sock_write_timeout: None,
            mqtt_connect_timeout: Self::DEF_MQTT_CONNECT_TIMEOUT,
            mqtt_keep_alive: None,
            mqtt_keep_alive_factor: F32::from(Self::DEF_KEEP_ALIVE_FACTOR),
            mqtt_session_expiry_interval: None,
            mqtt_maximum_qos: Self::DEF_MQTT_MAX_QOS,
            mqtt_receive_maximum: Self::DEF_MQTT_RECEIVE_MAXIMUM,
            mqtt_retain_available: Self::DEF_MQTT_RETAIN_AVAILABLE,
            mqtt_topic_alias_max: Some(Self::DEF_MQTT_TOPIC_ALIAS_MAX),
        }
    }
}

impl TryFrom<toml::Value> for Config {
    type Error = Error;

    fn try_from(val: toml::Value) -> Result<Config> {
        let mut def = Config::default();
        match val.as_table() {
            Some(t) => {
                config_field!(t, mqtt_listener, def, as_bool().map(|b| b.to_string()));
                config_field!(t, mqtt_port, def, as_integer().map(|n| n.to_string()));
                config_field!(
                    t,
                    mqtt_max_packet_size,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    opt: t,
                    mqtt_sock_read_timeout,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    opt: t,
                    mqtt_sock_write_timeout,
                    def,
                    as_integer().map(|n| n.to_string())
                );
                config_field!(
                    t,
                    mqtt_connect_timeout,
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
                    mqtt_receive_maximum,
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
            }
            None => (),
        }

        Ok(def)
    }
}

impl Config {
    /// Refer to [Config::mqtt_listener]
    pub const DEF_MQTT_LISTENER: bool = true;
    /// Refer to [Config::mqtt_port]
    pub const DEF_MQTT_PORT: u16 = 1883;
    /// Refer to [Config::mqtt_max_packet_size]
    pub const DEF_MQTT_MAX_PACKET_SIZE: u32 = 1024 * 1024; // default is 1MB.
    /// Refer to [Config::mqtt_connect_timeout]
    pub const DEF_MQTT_CONNECT_TIMEOUT: u32 = 5; // in seconds.
    /// Refer to [Config::keep_alive_factor]
    pub const DEF_KEEP_ALIVE_FACTOR: f32 = 1.5; // suggested by the spec.
    /// Refer to [Config::mqtt_maximum_qos]
    pub const DEF_MQTT_MAX_QOS: u8 = 1;
    /// Refer to [Config::mqtt_receive_maximum]
    pub const DEF_MQTT_RECEIVE_MAXIMUM: u16 = 256;
    /// Refer to [Config::mqtt_retain_available]
    pub const DEF_MQTT_RETAIN_AVAILABLE: bool = true;
    /// Refer to [Config::mqtt_topic_alias_max]
    pub const DEF_MQTT_TOPIC_ALIAS_MAX: u16 = 65535;
}

impl Config {
    pub fn keep_alive(&self) -> Option<u16> {
        match self.mqtt_keep_alive {
            Some(0) | None => None,
            Some(val) => Some(val),
        }
    }

    pub fn keep_alive_factor(&self) -> f32 {
        f32::from(self.mqtt_keep_alive_factor)
    }

    pub fn topic_alias_max(&self) -> Option<u16> {
        match &self.mqtt_topic_alias_max {
            Some(0) => None,
            Some(val) => Some(*val),
            None => self.mqtt_topic_alias_max,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let val = self.mqtt_max_packet_size;
        if val > 268435456 {
            err!(InvalidInput, desc: "mqtt_max_packet_size is {}", val)
        } else {
            Ok(())
        }
    }
}
