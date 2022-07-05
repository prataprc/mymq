use std::time;

use crate::{v5, Config};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub struct KeepAlive {
    prefix: String,
    interval: Option<u16>,
    alive_at: time::Instant,
}

impl KeepAlive {
    pub fn new(prefix: &str, pkt: &v5::Connect, config: &Config) -> KeepAlive {
        let factor = config.mqtt_keep_alive_factor();
        let interval = match config.mqtt_keep_alive() {
            Some(val) => Some(((val as f32) * factor) as u16),
            None if pkt.keep_alive == 0 => None,
            None => Some(((pkt.keep_alive as f32) * factor) as u16),
        };
        let prefix = format!("{}-keepalive", prefix);
        KeepAlive { prefix, interval, alive_at: time::Instant::now() }
    }

    pub fn keep_alive(&self) -> Option<u16> {
        self.interval
    }

    pub fn check_expired(&self) -> Result<()> {
        match self.interval {
            Some(interval) => {
                let now = time::Duration::from_secs(interval as u64);
                let inst = self.alive_at + now;
                if inst < time::Instant::now() {
                    Ok(())
                } else {
                    err!(
                        ProtocolError,
                        code: KeepAliveTimeout,
                        "{} keep alive expired alive_at:{:?} now:{:?}",
                        self.prefix,
                        self.alive_at,
                        now
                    )
                }
            }
            None => Ok(()),
        }
    }

    pub fn live(&mut self) {
        self.alive_at = time::Instant::now();
    }
}
