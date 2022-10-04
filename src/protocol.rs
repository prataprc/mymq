use std::{io, net};

use crate::v5;
use crate::{Blob, ClientID, Packetize, QueueStatus, Result};

pub type QueuePkt = QueueStatus<QPacket>;

/// Enumerated list of support message-queue protocol.
#[derive(Clone)]
pub enum Protocol {
    V5(v5::Protocol),
}

impl Protocol {
    pub fn new_v5_protocol(config: toml::Value) -> Result<Self> {
        Ok(Protocol::V5(v5::Protocol::new(config)?))
    }
}

impl Protocol {
    pub fn to_listen_address(&self) -> net::SocketAddr {
        match self {
            Protocol::V5(proto) => proto.to_listen_address(),
        }
    }

    pub fn to_listen_port(&self) -> u16 {
        match self {
            Protocol::V5(proto) => proto.to_listen_port(),
        }
    }

    pub fn max_packet_size(&self) -> u32 {
        match self {
            Protocol::V5(proto) => proto.max_packet_size(),
        }
    }
}

impl Protocol {
    pub fn handshake(&self, prefix: &str, conn: mio::net::TcpStream) -> Result<Socket> {
        match self {
            Protocol::V5(proto) => Ok(Socket::V5(proto.handshake(prefix, conn)?)),
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

    pub fn to_protocol(&self) -> Protocol {
        match self {
            Socket::V5(sock) => Protocol::V5(sock.to_protocol()),
        }
    }
}

impl Socket {
    /// Error shall be MalformedPacket or ProtocolError.
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    pub fn read_packet(&mut self, prefix: &str) -> Result<QueuePkt> {
        match self {
            Socket::V5(sock) => sock.read_packet(prefix),
        }
    }

    /// Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    pub fn write_packet(&mut self, prefix: &str, blob: Option<Blob>) -> QueuePkt {
        match self {
            Socket::V5(sock) => sock.write_packet(prefix, blob),
        }
    }
}

#[derive(Clone)]
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

impl Packetize for QPacket {
    fn decode<T: AsRef<[u8]>>(_stream: T) -> Result<(Self, usize)> {
        unimplemented!()
    }

    fn encode(&self) -> Result<Blob> {
        match self {
            QPacket::V5(pkt) => pkt.encode(),
        }
    }
}
