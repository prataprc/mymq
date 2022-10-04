use crate::{Error, ErrorKind, Result};

#[derive(Clone)]
pub struct Config {
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
}

impl Default for Config {
    fn default() -> Config {
        Config {
            mqtt_port: Self::DEF_MQTT_PORT,
            mqtt_max_packet_size: Self::DEF_MQTT_MAX_PACKET_SIZE,
            mqtt_sock_read_timeout: None,
            mqtt_sock_write_timeout: None,
            mqtt_connect_timeout: Self::DEF_MQTT_CONNECT_TIMEOUT,
        }
    }
}

impl TryFrom<toml::Value> for Config {
    type Error = Error;

    fn try_from(val: toml::Value) -> Result<Config> {
        let mut def = Config::default();
        match val.as_table() {
            Some(t) => {
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
            }
            None => (),
        }

        Ok(def)
    }
}

impl Config {
    /// Refer to [Config::mqtt_port]
    pub const DEF_MQTT_PORT: u16 = 1883;
    /// Refer to [Config::mqtt_max_packet_size]
    pub const DEF_MQTT_MAX_PACKET_SIZE: u32 = 1024 * 1024; // default is 1MB.
    /// Refer to [Config::mqtt_connect_timeout]
    pub const DEF_MQTT_CONNECT_TIMEOUT: u32 = 5; // in seconds.
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let val = self.mqtt_max_packet_size;
        if val > 268435456 {
            err!(InvalidInput, desc: "mqtt_max_packet_size is {}", val)
        } else {
            Ok(())
        }
    }
}
