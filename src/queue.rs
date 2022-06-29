use std::{net, sync::mpsc, time};

use crate::packet::{PacketRead, PacketWrite};
use crate::{v5, ClientID};

pub type QueueTx = mpsc::SyncSender<v5::Packet>;
pub type QueueRx = mpsc::Receiver<v5::Packet>;

pub struct Socket {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub token: mio::Token,
    pub rd: Source,
    pub wt: Sink,
}

pub struct Source {
    pub pr: PacketRead,
    pub timeout: Option<time::Instant>,
    pub tx: QueueTx,
    pub packets: Vec<v5::Packet>,
}

pub struct Sink {
    pub pw: PacketWrite,
    pub timeout: Option<time::Instant>,
    pub rx: QueueRx,
    pub packets: Vec<v5::Packet>,
}

impl Socket {
    pub fn read_elapsed(&self) -> bool {
        match &self.rd.timeout {
            Some(timeout) if timeout > &time::Instant::now() => true,
            Some(_) | None => false,
        }
    }

    pub fn write_elapsed(&self) -> bool {
        match &self.wt.timeout {
            Some(timeout) if timeout > &time::Instant::now() => true,
            Some(_) | None => false,
        }
    }

    pub fn set_read_timeout(&mut self, retry: bool, timeout: u32) {
        if retry && self.rd.timeout.is_none() {
            self.rd.timeout =
                Some(time::Instant::now() + time::Duration::from_secs(timeout as u64));
        } else if retry == false {
            self.rd.timeout = None;
        }
    }

    pub fn set_write_timeout(&mut self, retry: bool, timeout: u32) {
        if retry && self.wt.timeout.is_none() {
            self.wt.timeout =
                Some(time::Instant::now() + time::Duration::from_secs(timeout as u64));
        } else if retry == false {
            self.wt.timeout = None;
        }
    }
}

pub struct Inbound {
    //
}

pub struct Outbound {
    //
}

#[inline]
pub fn queue_channel(size: usize) -> (QueueTx, QueueRx) {
    mpsc::sync_channel(size)
}
