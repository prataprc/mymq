use log::{error, info, trace};

use std::{thread, time};

use crate::thread::{Rx, Threadable};
use crate::{queue, MSG_CHANNEL_SIZE, SLEEP_10MS};
use crate::{Error, ReasonCode};

pub struct Flush {
    pub prefix: String,
    pub err: Option<Error>,
    pub queue: queue::Socket,
    pub flush_timeout: u32,
}

impl Threadable for Flush {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        use crate::miot::{rx_packets, Miot};
        use crate::packet::send_disconnect;

        let now = time::Instant::now();
        info!("{} flush connection at {:?}", self.prefix, now);

        let timeout = now + time::Duration::from_secs(self.flush_timeout as u64);
        loop {
            match Miot::send_upstream(&self.prefix, &mut self.queue) {
                Ok(true /*would_block*/) if time::Instant::now() > timeout => break,
                Ok(true) => thread::sleep(SLEEP_10MS),
                Ok(false) => break,
                Err(_) => break,
            }
        }

        loop {
            match Miot::flush_packets(&self.prefix, self.flush_timeout, &mut self.queue) {
                Ok(true /*would_block*/) if time::Instant::now() > timeout => break,
                Ok(true) => thread::sleep(SLEEP_10MS),
                Ok(false) => match rx_packets(&self.queue.wt.rx, MSG_CHANNEL_SIZE) {
                    (qs, _empty, false) => {
                        self.queue.wt.packets.extend_from_slice(&qs);
                        trace!("{} flush read from upstream", self.prefix);
                    }
                    (qs, _empty, true) => {
                        self.queue.wt.packets.extend_from_slice(&qs);
                        info!("{} upstream finished", self.prefix);
                        break;
                    }
                },
                Err(err) => error!("{} flush_packets {}", self.prefix, err),
            }
        }

        let code = self.err.as_ref().map(|err| err.code()).unwrap_or(ReasonCode::Success);
        send_disconnect(&self.prefix, timeout, code, &mut self.queue.conn).ok();

        self
    }
}
