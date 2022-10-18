use std::{net, time};

use crate::Protocol;
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Type implement keep-alive timer.
///
/// A passive timer used by application to detect keep-alive timeouts by calling
/// [KeepAlive::check_expired]. Typically used to keep track of network in-activity.
pub struct KeepAlive {
    prefix: String,
    keep_alive: Option<u16>,
    interval: Option<u16>,
    alive_at: time::Instant,
}

impl KeepAlive {
    /// Create a new keep-alive timer. `keep_alive` is client requested keep_alive.
    /// Protocol configured keep alive specified in `proto` take priority. If both
    /// `keep_alive` and `proto` is configured with keep_alive as ZERO seconds, then
    /// the session never expires.
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

    /// Return the negotiated keep alive for the session.
    pub fn to_keep_alive(&self) -> Option<u16> {
        self.keep_alive
    }

    /// Check whether this keep-alive timer has elapsed. Application should call this
    /// method to learn the timeout status. Returns elapsed time since timeout expiry.
    pub fn check_expired(&self) -> Result<time::Duration> {
        match self.interval {
            Some(interval) => {
                let now = time::Instant::now();
                let interval = time::Duration::from_secs(u64::from(interval));
                match self.alive_at + interval {
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
