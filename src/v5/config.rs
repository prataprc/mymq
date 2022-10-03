use std::net;

use crate::v5::{Error, ErrorKind, Result};

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

    /// MQTT packets are drainded from queues and connections in batches, so that
    /// all queues will get evenly processed. This parameter defines the batch size
    /// while draining the message queues.
    ///
    /// * **Default**: [Config::DEF_MQTT_PKT_BATCH_SIZE]
    /// * **Mutable**: No
    pub mqtt_pkt_batch_size: u32,

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
}

impl Default for Config {
    fn default() -> Config {
        Config {
            mqtt_port: Self::DEF_MQTT_PORT,
            mqtt_max_packet_size: Self::DEF_MQTT_MAX_PACKET_SIZE,
            mqtt_pkt_batch_size: Self::DEF_MQTT_PKT_BATCH_SIZE,
            mqtt_sock_read_timeout: None,
            mqtt_sock_write_timeout: None,
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
                    t,
                    mqtt_pkt_batch_size,
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
    /// Refer to [Config::mqtt_pkt_batch_size]
    pub const DEF_MQTT_PKT_BATCH_SIZE: u32 = 1024; // default is 1MB.
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

/// Default listen address for MQTT packets: `0.0.0.0:1883`
pub fn mqtt_listen_address4(port: Option<u16>) -> net::SocketAddr {
    use std::net::{IpAddr, Ipv4Addr};

    let port = port.unwrap_or(Config::DEF_MQTT_PORT);
    net::SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port)
}
