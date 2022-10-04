use std::time;

use crate::broker::SessionArgsActive;
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Type implement keep-alive as per MQTT specification.
pub struct KeepAlive {
    pub prefix: String,
    pub keep_alive: Option<u16>,
    pub interval: Option<u16>,
    pub alive_at: time::Instant,
}

impl KeepAlive {
    pub fn new(args: &SessionArgsActive) -> KeepAlive {
        let factor = args.config.mqtt_keep_alive_factor();
        let (keep_alive, interval) = match args.config.mqtt_keep_alive() {
            Some(val) => (Some(val as u16), Some(((val as f32) * factor) as u16)),
            None if args.connect.keep_alive == 0 => (None, None),
            None => {
                let ka = args.connect.keep_alive;
                (Some(ka), Some(((ka as f32) * factor) as u16))
            }
        };
        let prefix = format!("{}:keepalive", args.raddr);
        KeepAlive {
            prefix,
            keep_alive,
            interval,
            alive_at: time::Instant::now(),
        }
    }

    pub fn check_expired(&self) -> Result<time::Duration> {
        match self.interval {
            Some(intrvl) => {
                let now = time::Instant::now();
                match self.alive_at + time::Duration::from_secs(intrvl as u64) {
                    deadline if now > deadline => err!(
                        ProtocolError,
                        code: KeepAliveTimeout,
                        "{} keep alive expired alive_at:{:?} diff:{:?}",
                        self.prefix,
                        self.alive_at,
                        now - deadline
                    ),
                    deadline => Ok(deadline - now),
                }
            }
            None => Ok(time::Duration::from_secs(u64::MAX)),
        }
    }

    pub fn live(&mut self) {
        self.alive_at = time::Instant::now();
    }
}
