#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{fmt, io, net, result};

use crate::v5;
use crate::{Blob, ClientID, PacketID, PacketType, Packetize, QoS, QueueStatus};
use crate::{ReasonCode, Result};
use crate::{Subscription, TopicName};

pub type QueuePkt = QueueStatus<QPacket>;

/// Enumerated list of supported message-queue protocol.
#[derive(Clone, Eq, PartialEq)]
pub enum Protocol {
    V5(v5::Protocol),
    None,
}

impl Default for Protocol {
    fn default() -> Protocol {
        Protocol::None
    }
}

impl From<v5::Protocol> for Protocol {
    fn from(proto: v5::Protocol) -> Protocol {
        Protocol::V5(proto)
    }
}

impl Protocol {
    /// Whether to launch a listener for this protocol. Broker side configuration.
    #[inline]
    pub fn is_listen(&self) -> bool {
        match self {
            Protocol::V5(proto) => proto.is_listen(),
            Protocol::None => unreachable!(),
        }
    }

    /// Listen socket-address for this protocol. Broker side configuration.
    #[inline]
    pub fn to_listen_address(&self) -> net::SocketAddr {
        match self {
            Protocol::V5(proto) => proto.to_listen_address(),
            Protocol::None => unreachable!(),
        }
    }

    /// Listen port for this protocol. Broker side configuration.
    #[inline]
    pub fn to_listen_port(&self) -> u16 {
        match self {
            Protocol::V5(proto) => proto.to_listen_port(),
            Protocol::None => unreachable!(),
        }
    }

    /// Maximum supported QoS by this protocol. Broker side configuration.
    #[inline]
    pub fn maximum_qos(&self) -> QoS {
        match self {
            Protocol::V5(proto) => proto.maximum_qos(),
            Protocol::None => unreachable!(),
        }
    }

    /// Whether publish-retain feature is supported by this protocol. Broker side
    /// configuration.
    #[inline]
    pub fn retain_available(&self) -> bool {
        match self {
            Protocol::V5(proto) => proto.retain_available(),
            Protocol::None => unreachable!(),
        }
    }

    /// Maximum packet size allowed by this protocol. Broker side configuration.
    #[inline]
    pub fn max_packet_size(&self) -> u32 {
        match self {
            Protocol::V5(proto) => proto.max_packet_size(),
            Protocol::None => unreachable!(),
        }
    }

    /// Keep alive, in seconds, ticker supported by this protocol. Broker side
    /// configuration.
    #[inline]
    pub fn keep_alive(&self) -> Option<u16> {
        match self {
            Protocol::V5(proto) => proto.keep_alive(),
            Protocol::None => unreachable!(),
        }
    }

    /// Keep alive factor, configured for this protocol. Broker side configuration.
    #[inline]
    pub fn keep_alive_factor(&self) -> f32 {
        match self {
            Protocol::V5(proto) => proto.keep_alive_factor(),
            Protocol::None => unreachable!(),
        }
    }

    /// Whether topic-alias feature is supported and its configuration for this protocol.
    /// Broker side configuration.
    #[inline]
    pub fn topic_alias_max(&self) -> Option<u16> {
        match self {
            Protocol::V5(proto) => proto.topic_alias_max(),
            Protocol::None => unreachable!(),
        }
    }

    /// Broker side configuration.
    #[inline]
    pub fn session_expiry_interval(&self) -> Option<u32> {
        match self {
            Protocol::V5(proto) => proto.session_expiry_interval(),
            Protocol::None => unreachable!(),
        }
    }
}

impl Protocol {
    /// Handshake with remote client and complete the connection process.
    #[inline]
    pub fn handshake(&self, prefix: &str, conn: mio::net::TcpStream) -> Result<Socket> {
        match self {
            Protocol::V5(proto) => Ok(Socket::V5(proto.handshake(prefix, conn)?)),
            Protocol::None => unreachable!(),
        }
    }
}

impl Protocol {
    /// Create a new ping-response packet.
    pub fn new_ping_resp(&self, ping_req: QPacket) -> QPacket {
        match self {
            Protocol::V5(proto) => match ping_req {
                QPacket::V5(ping_req) => proto.new_ping_resp(ping_req),
            },
            Protocol::None => unreachable!(),
        }
    }

    /// Create a new publish acknowledge packet.
    pub fn new_pub_ack(&self, packet_id: PacketID) -> QPacket {
        match self {
            Protocol::V5(proto) => proto.new_pub_ack(packet_id),
            Protocol::None => unreachable!(),
        }
    }

    /// Create a new subscribe acknowledge packet.
    pub fn new_sub_ack(&self, sub: &QPacket, rcodes: Vec<ReasonCode>) -> QPacket {
        match self {
            Protocol::V5(proto) => match sub {
                QPacket::V5(sub) => proto.new_sub_ack(sub, rcodes),
            },
            Protocol::None => unreachable!(),
        }
    }

    /// Create a new un-subscribe acknowledge packet.
    pub fn new_unsub_ack(&self, unsub: &QPacket, rcodes: Vec<ReasonCode>) -> QPacket {
        match self {
            Protocol::V5(proto) => match unsub {
                QPacket::V5(unsub) => proto.new_unsub_ack(unsub, rcodes),
            },
            Protocol::None => unreachable!(),
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
    /// Set [mio::Token] for underlying connection. Used for non-blocking r/w.
    #[inline]
    pub fn set_mio_token(&mut self, token: mio::Token) {
        match self {
            Socket::V5(sock) => sock.set_mio_token(token),
        }
    }

    /// Set shard-id for this socket. This happens after handshake and before socket
    /// is booked as a session.
    #[inline]
    pub fn set_shard_id(&mut self, shard_id: u32) {
        match self {
            Socket::V5(sock) => sock.set_shard_id(shard_id),
        }
    }

    /// Broker can assign client id if not specified by the remote. Note that calling
    /// this function will set the client_id only if it is not set by remote.
    #[inline]
    pub fn assign_client_id(&mut self, client_id: ClientID) {
        match self {
            Socket::V5(sock) => sock.assign_client_id(client_id),
        }
    }
}

impl Socket {
    /// Return the remote's socket-address.
    #[inline]
    pub fn peer_addr(&self) -> net::SocketAddr {
        match self {
            Socket::V5(sock) => sock.peer_addr(),
        }
    }

    /// Return the client_id for this conn/socket/session.
    #[inline]
    pub fn as_client_id(&self) -> &ClientID {
        match self {
            Socket::V5(sock) => sock.as_client_id(),
        }
    }

    #[inline]
    pub fn to_mio_token(&self) -> mio::Token {
        match self {
            Socket::V5(sock) => sock.to_mio_token(),
        }
    }

    #[inline]
    pub fn to_protocol(&self) -> Protocol {
        match self {
            Socket::V5(sock) => Protocol::V5(sock.to_protocol()),
        }
    }

    #[inline]
    pub fn client_keep_alive(&self) -> u16 {
        match self {
            Socket::V5(sock) => sock.client_keep_alive(),
        }
    }

    #[inline]
    pub fn client_receive_maximum(&self) -> u16 {
        match self {
            Socket::V5(sock) => sock.client_receive_maximum(),
        }
    }

    #[inline]
    pub fn client_session_expiry_interval(&self) -> Option<u32> {
        match self {
            Socket::V5(sock) => sock.client_session_expiry_interval(),
        }
    }

    #[inline]
    pub fn is_clean_start(&self) -> bool {
        match self {
            Socket::V5(sock) => sock.is_clean_start(),
        }
    }
}

impl Socket {
    /// Return acknowledgement packet with reason code `rcode` to send back to client.
    #[inline]
    pub fn new_conn_ack(&self, rcode: ReasonCode) -> QPacket {
        match self {
            Socket::V5(sock) => sock.new_conn_ack(rcode),
        }
    }

    /// Send disconnect packet with reason code `rcode.
    #[inline]
    pub fn send_disconnect(&mut self, prefix: &str, rcode: ReasonCode) {
        match self {
            Socket::V5(sock) => sock.send_disconnect(prefix, rcode),
        }
    }

    /// Return the will message, for session, treated as publish after session's death.
    #[inline]
    pub fn to_will_publish(&self) -> Option<QPacket> {
        match self {
            Socket::V5(sock) => sock.to_will_publish(),
        }
    }

    /// Return the will delay interval to be used along with [Self::to_will_publish].
    #[inline]
    pub fn will_delay_interval(&self) -> u32 {
        match self {
            Socket::V5(sock) => sock.will_delay_interval(),
        }
    }
}

impl Socket {
    /// Error shall be MalformedPacket or ProtocolError.
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    #[inline]
    pub fn read_packet(&mut self, prefix: &str) -> Result<QueuePkt> {
        match self {
            Socket::V5(sock) => sock.read_packet(prefix),
        }
    }

    /// Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    /// Returned QueueStatus shall not carry any packets, packets are booked in Socket.
    #[inline]
    pub fn write_packet(&mut self, prefix: &str, blob: Option<Blob>) -> QueuePkt {
        match self {
            Socket::V5(sock) => sock.write_packet(prefix, blob),
        }
    }
}

/// Enumerated types of packets supported by protocol.
///
/// There shall be one-to-one correspondence between [Protocol] variants and [QPacket]
/// variants.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum QPacket {
    V5(v5::Packet),
}

impl fmt::Display for QPacket {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            QPacket::V5(pkt) => write!(f, "{}", pkt),
        }
    }
}

#[cfg(any(feature = "fuzzy", test))]
use std::cmp;

#[cfg(any(feature = "fuzzy", test))]
impl PartialOrd for QPacket {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.as_topic_name().partial_cmp(other.as_topic_name())
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl Ord for QPacket {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.as_topic_name().cmp(other.as_topic_name())
    }
}

impl From<QPacket> for v5::Packet {
    #[inline]
    fn from(val: QPacket) -> v5::Packet {
        match val {
            QPacket::V5(pkt) => pkt,
        }
    }
}

impl From<v5::Packet> for QPacket {
    #[inline]
    fn from(val: v5::Packet) -> QPacket {
        QPacket::V5(val)
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for QPacket {
    #[inline]
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        Ok(QPacket::V5(uns.arbitrary()?))
    }
}

impl Packetize for QPacket {
    #[inline]
    fn decode<T: AsRef<[u8]>>(_stream: T) -> Result<(Self, usize)> {
        unimplemented!()
    }

    #[inline]
    fn encode(&self) -> Result<Blob> {
        match self {
            QPacket::V5(pkt) => pkt.encode(),
        }
    }
}

impl QPacket {
    #[inline]
    pub fn set_packet_id(&mut self, packet_id: u16) {
        match self {
            QPacket::V5(pkt) => pkt.set_packet_id(packet_id),
        }
    }

    #[inline]
    pub fn set_retain(&mut self, retain: bool) {
        match self {
            QPacket::V5(pkt) => pkt.set_retain(retain),
        }
    }

    #[inline]
    pub fn set_fixed_header(&mut self, retain: bool, qos: QoS, dup: bool) {
        match self {
            QPacket::V5(pkt) => pkt.set_fixed_header(retain, qos, dup),
        }
    }

    #[inline]
    pub fn set_subscription_ids(&mut self, ids: Vec<u32>) {
        match self {
            QPacket::V5(pkt) => pkt.set_subscription_ids(ids),
        }
    }

    #[inline]
    pub fn set_session_present(&mut self, session_present: bool) {
        match self {
            QPacket::V5(pkt) => pkt.set_session_present(session_present),
        }
    }
}

impl QPacket {
    #[inline]
    pub fn to_packet_type(&self) -> PacketType {
        match self {
            QPacket::V5(pkt) => pkt.to_packet_type(),
        }
    }

    #[inline]
    pub fn to_packet_id(&self) -> Option<u16> {
        match self {
            QPacket::V5(pkt) => pkt.to_packet_id(),
        }
    }

    #[inline]
    pub fn to_qos(&self) -> QoS {
        match self {
            QPacket::V5(pkt) => pkt.to_qos(),
        }
    }

    pub fn to_subscription_id(&self) -> Option<u32> {
        match self {
            QPacket::V5(pkt) => pkt.to_subscription_id(),
        }
    }

    pub fn to_subscriptions(&self) -> Vec<Subscription> {
        match self {
            QPacket::V5(pkt) => pkt.to_subscriptions(),
        }
    }

    pub fn to_unsubscriptions(&self, client_id: ClientID) -> Vec<Subscription> {
        match self {
            QPacket::V5(pkt) => pkt.to_unsubscriptions(client_id),
        }
    }

    pub fn to_topic_alias(&self) -> Option<u16> {
        match self {
            QPacket::V5(pkt) => pkt.to_topic_alias(),
        }
    }

    pub fn to_disconnect_code(&self) -> ReasonCode {
        match self {
            QPacket::V5(pkt) => pkt.to_disconnect_code(),
        }
    }

    pub fn to_reason_string(&self) -> Option<String> {
        match self {
            QPacket::V5(pkt) => pkt.to_reason_string(),
        }
    }

    pub fn as_topic_name(&self) -> &TopicName {
        match self {
            QPacket::V5(pkt) => pkt.as_topic_name(),
        }
    }

    #[inline]
    pub fn is_qos0(&self) -> bool {
        match self {
            QPacket::V5(pkt) => pkt.is_qos0(),
        }
    }

    #[inline]
    pub fn is_qos12(&self) -> bool {
        match self {
            QPacket::V5(pkt) => pkt.is_qos12(),
        }
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        match self {
            QPacket::V5(pkt) => pkt.is_retain(),
        }
    }

    #[inline]
    pub fn has_payload(&self) -> bool {
        match self {
            QPacket::V5(pkt) => pkt.has_payload(),
        }
    }

    #[inline]
    pub fn message_expiry_interval(&self) -> Option<u32> {
        match self {
            QPacket::V5(pkt) => pkt.message_expiry_interval(),
        }
    }
}
