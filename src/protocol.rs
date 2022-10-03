use std::{io, net};

use crate::v5;
use crate::{ClientID, QueueStatus, Result};

pub type QueuePkt = QueueStatus<QPacket>;

/// Enumerated list of support message-queue protocol.
pub enum Protocol {
    V5(v5::Protocol),
}

impl Protocol {
    pub fn max_packet_size(&self) -> u32 {
        match self {
            //Protocol::V5(proto) => proto.max_packet_size(),
            _ => unreachable!(),
        }
    }
}

pub enum Socket {
    V5(v5::Socket),
}

impl mio::event::Source for Socket {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        match self {
            Socket::V5(sock) => sock.register(registry, token, interests),
        }
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        match self {
            Socket::V5(sock) => sock.reregister(registry, token, interests),
        }
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        match self {
            Socket::V5(sock) => sock.deregister(registry),
        }
    }
}

impl Socket {
    pub fn peer_addr(&self) -> net::SocketAddr {
        match self {
            Socket::V5(sock) => sock.peer_addr(),
        }
    }

    pub fn to_client_id(&self) -> ClientID {
        match self {
            Socket::V5(sock) => sock.to_client_id(),
        }
    }

    pub fn to_mio_token(&self) -> mio::Token {
        match self {
            Socket::V5(sock) => sock.to_mio_token(),
        }
    }

    pub fn set_mio_token(&mut self, token: mio::Token) {
        match self {
            Socket::V5(sock) => sock.set_mio_token(token),
        }
    }

    /// Error shall be MalformedPacket or ProtocolError.
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    pub fn read_packets(&mut self, prefix: &str) -> Result<QueuePkt> {
        match self {
            Socket::V5(sock) => sock.read_packets(prefix),
        }
    }

    /// Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    pub fn write_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        match self {
            Socket::V5(sock) => sock.write_packets(prefix),
        }
    }

    /// Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    pub fn flush_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        match self {
            Socket::V5(sock) => sock.flush_packets(prefix),
        }
    }
}

pub enum QPacket {
    V5(v5::Packet),
}

impl From<QPacket> for v5::Packet {
    fn from(val: QPacket) -> v5::Packet {
        match val {
            QPacket::V5(pkt) => pkt,
        }
    }
}

impl From<v5::Packet> for QPacket {
    fn from(val: v5::Packet) -> QPacket {
        QPacket::V5(val)
    }
}
