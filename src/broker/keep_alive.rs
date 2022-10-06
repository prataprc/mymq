use std::{net, time};

use crate::Protocol;
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Type implement keep-alive timer. A passive timer used by application to detect
/// keep-alive timeouts by calling [KeepAlive::check_expired]. Typically used to
/// keep track of network in-activity.
pub struct KeepAlive {
    pub prefix: String,
    pub keep_alive: Option<u16>,
    pub interval: Option<u16>,
    pub alive_at: time::Instant,
}

impl KeepAlive {
    /// Create a new keep-alive timer.
    pub fn new(proto: &Protocol, raddr: net::SocketAddr, keep_alive: u16) -> KeepAlive {
        let factor = proto.keep_alive_factor();
        let (keep_alive, interval) = match proto.keep_alive() {
            Some(val) => (Some(val as u16), Some(((val as f32) * factor) as u16)),
            None if keep_alive == 0 => (None, None),
            None => {
                let ka = u16::try_from(keep_alive).unwrap();
                (Some(ka), Some(((ka as f32) * factor) as u16))
            }
        };

        KeepAlive {
            prefix: format!("{}:keepalive", raddr),
            keep_alive,
            interval,
            alive_at: time::Instant::now(),
        }
    }

    /// Check whether this keep-alive timer has elapsed. Application should call this
    /// method to learn the timeout status. Returns elapsed time since timeout expiry.
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

    /// Reset the timeout. To be called when ever a network event has occured.
    pub fn live(&mut self) {
        self.alive_at = time::Instant::now();
    }
}
