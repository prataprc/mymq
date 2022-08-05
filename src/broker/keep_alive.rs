use std::{net, time};

use crate::{broker::Config, v5};
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Type implement keep-alive as per MQTT specification.
pub struct KeepAlive {
    pub prefix: String,
    pub interval: Option<u16>,
    pub alive_at: time::Instant,
}

impl KeepAlive {
    pub fn new(addr: net::SocketAddr, pkt: &v5::Connect, config: &Config) -> KeepAlive {
        let factor = config.mqtt_keep_alive_factor;
        let interval = match config.mqtt_keep_alive() {
            Some(val) => Some(((val as f32) * factor) as u16),
            None if pkt.keep_alive == 0 => None,
            None => Some(((pkt.keep_alive as f32) * factor) as u16),
        };
        let prefix = format!("{}:keepalive", addr);
        KeepAlive { prefix, interval, alive_at: time::Instant::now() }
    }

    pub fn keep_alive(&self) -> Option<u16> {
        self.interval
    }

    pub fn check_expired(&self) -> Result<time::Duration> {
        match self.interval {
            Some(interval) => {
                let interval = time::Duration::from_secs(interval as u64);
                let diff = (self.alive_at + interval) - time::Instant::now();
                if diff.is_zero() {
                    err!(
                        ProtocolError,
                        code: KeepAliveTimeout,
                        "{} keep alive expired alive_at:{:?} diff:{:?}",
                        self.prefix,
                        self.alive_at,
                        diff
                    )
                } else {
                    Ok(diff)
                }
            }
            None => Ok(time::Duration::from_secs(u64::MAX)),
        }
    }

    pub fn live(&mut self) {
        self.alive_at = time::Instant::now();
    }
}
