use std::{net, sync::mpsc};

use crate::packet::{PacketRead, PacketWrite};
use crate::{v5, ClientID};

pub type QueueTx = mpsc::SyncSender<v5::Packet>;
pub type QueueRx = mpsc::Receiver<v5::Packet>;

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
    pub retries: usize,
    pub msg_tx: QueueTx,
    pub packets: Vec<v5::Packet>,
}

pub struct SocketWt {
    pub pw: PacketWrite,
    pub retries: usize,
    pub msg_rx: QueueRx,
    pub packets: Vec<v5::Packet>,
    pub flush_retries: usize,
}
