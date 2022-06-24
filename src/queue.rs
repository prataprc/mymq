use std::{net, sync::mpsc};

use crate::Result;
use crate::{packet::PacketRead, v5, ClientID};

pub type QueueTx = mpsc::SyncSender<v5::Packet>;
pub type QueueRx = mpsc::Receiver<Result<v5::Packet>>;

pub struct Queue {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub token: mio::Token,
    pub rd: SocketRd,
    pub wt: SocketWt,
}

pub struct SocketRd {
    pub pr: PacketRead,
    pub retry: bool,
    pub retries: usize,
    pub msg_tx: QueueTx,
    pub packets: Vec<v5::Packet>,
}

impl SocketRd {
    fn reset(mut self) -> Self {
        self.pr = self.pr.reset();
        self.retry = false;
        self.retries = 0;
        self.retry = false;
        self
    }
}

pub struct SocketWt {
    pub data: Vec<u8>,
    pub start: usize,
    pub msg_rx: QueueRx,
    pub packets: Vec<v5::Packet>,
}
