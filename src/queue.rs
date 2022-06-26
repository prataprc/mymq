use std::{net, sync::mpsc};

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
    pub retries: usize,
    pub tx: QueueTx,
    pub packets: Vec<v5::Packet>,
}

pub struct Sink {
    pub pw: PacketWrite,
    pub retries: usize,
    pub rx: QueueRx,
    pub packets: Vec<v5::Packet>,
    pub flush_retries: usize,
}

#[inline]
pub fn queue_channel(size: usize) -> (QueueTx, QueueRx) {
    mpsc::sync_channel(size)
}
