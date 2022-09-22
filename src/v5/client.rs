//! Module implement MQTT Client.

#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;
use log::{error, trace, warn};

#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::unix::io::{FromRawSocket, IntoRawSocket};

use std::{collections::VecDeque, fmt, io, mem, net, result, time};

use crate::{v5, ClientID, MQTTRead, MQTTWrite, MqttProtocol, PacketID, Packetize};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub const CLIENT_MAX_PACKET_SIZE: u32 = 1024 * 1024;

/// MQTT CONNECT flags and headers
#[derive(Clone, Copy)]
pub struct ConnectOptions {
    pub will_qos: Option<v5::QoS>,
    pub will_retain: Option<bool>,
    pub keep_alive: u16,
}

impl Default for ConnectOptions {
    fn default() -> ConnectOptions {
        ConnectOptions { will_qos: None, will_retain: None, keep_alive: 0 }
    }
}

/// ClientBuilder to create a customized [Client].
pub struct ClientBuilder {
    pub protocol_version: MqttProtocol,
    /// Provide unique client identifier, if missing, will be sent empty in CONNECT.
    pub client_id: Option<ClientID>,
    /// Socket settings for blocking io, refer [net::TcpStream::connect_timeout].
    /// Defaults to None.
    pub connect_timeout: Option<time::Duration>,
    /// Socket settings for blocking io, refer [net::TcpStream::set_read_timeout].
    /// Defaults to None.
    pub read_timeout: Option<time::Duration>,
    /// Socket settings for blocking io, refer [net::TcpStream::set_write_timeout].
    /// Defaults to None.
    pub write_timeout: Option<time::Duration>,
    /// Socket settings, refer [net::TcpStream::set_nodelay].
    /// Defaults to None.
    pub nodelay: Option<bool>,
    /// Socket settings, refer [net::TcpStream::set_ttl].
    /// Defaults to None.
    pub ttl: Option<u32>,
    /// Maximum packet size,
    pub max_packet_size: u32,
    // CONNECT options
    pub connopts: ConnectOptions,
    pub connect_properties: Option<v5::ConnectProperties>,
    pub connect_payload: v5::ConnectPayload,
}

impl Default for ClientBuilder {
    fn default() -> ClientBuilder {
        ClientBuilder {
            client_id: Some(ClientID::new_uuid_v4()),
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            nodelay: None,
            ttl: None,
            max_packet_size: CLIENT_MAX_PACKET_SIZE,
            // CONNECT options
            connopts: ConnectOptions::default(),
            connect_properties: Some(v5::ConnectProperties::default()),
            connect_payload: v5::ConnectPayload::default(),

            protocol_version: MqttProtocol::V5,
        }
    }
}

impl ClientBuilder {
    /// Connection with `remote` and start a synchronous client. All read/write calls
    /// and other communication methods, on the returned client, shall block.
    ///
    /// NOTE: This call shall block until CONNACK is successfully received from remote.
    pub fn connect(self, raddr: net::SocketAddr) -> io::Result<Client> {
        let sock = match self.connect_timeout {
            Some(timeout) => net::TcpStream::connect_timeout(&raddr, timeout)?,
            None => net::TcpStream::connect(&raddr)?,
        };
        sock.set_read_timeout(self.read_timeout)?;
        sock.set_write_timeout(self.write_timeout)?;
        if let Some(nodelay) = self.nodelay {
            sock.set_nodelay(nodelay)?
        }
        if let Some(ttl) = self.ttl {
            sock.set_ttl(ttl)?
        }

        let mut client = self.into_client(raddr);

        let (cio, connack) = {
            let connect = client.to_connect(true /*clean_start*/);
            let blocking = true;
            ClientIO::handshake(&mut client, connect, sock, blocking)?
        };

        client.cio = cio;
        client.next_packet_ids = (1..connack.receive_maximum()).collect();
        client.connack = connack;

        Ok(client)
    }

    /// Connection with `remote` and start an asynchronous client. All read/write calls
    /// and other communication methods, on the returned client, shall not block.
    /// Application will have to check for [io::ErrorKind::WouldBlock] and
    /// [io::ErrorKind::Interrupted] returns.
    ///
    /// NOTE: This call shall block until CONNACK is successfully received from remote.
    pub fn connect_noblock(self, raddr: net::SocketAddr) -> io::Result<Client> {
        let sock = net::TcpStream::connect(raddr)?;
        if let Some(nodelay) = self.nodelay {
            sock.set_nodelay(nodelay)?
        }
        if let Some(ttl) = self.ttl {
            sock.set_ttl(ttl)?
        }

        let mut client = self.into_client(raddr);

        let (cio, connack) = {
            let connect = client.to_connect(true /*clean_start*/);
            let blocking = false;
            ClientIO::handshake(&mut client, connect, sock, blocking)?
        };

        client.cio = cio;
        client.next_packet_ids = (1..connack.receive_maximum()).collect();
        client.connack = connack;

        Ok(client)
    }

    fn into_client(self, raddr: net::SocketAddr) -> Client {
        Client {
            client_id: self.client_id.unwrap_or_else(|| ClientID::new_uuid_v4()),
            raddr,
            protocol_version: self.protocol_version,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            nodelay: self.nodelay,
            ttl: self.ttl,
            max_packet_size: self.max_packet_size,
            connopts: self.connopts,
            connect_properties: self.connect_properties.clone(),
            connect_payload: self.connect_payload.clone(),
            // CONNACK options
            connack: v5::ConnAck::default(),

            last_sent: time::Instant::now(),
            last_rcvd: time::Instant::now(),
            rd_deadline: None,
            wt_deadline: None,
            next_packet_ids: VecDeque::default(),

            in_packets: VecDeque::default(),
            cio: ClientIO::None,
        }
    }
}

/// Type to interface with MQTT broker.
pub struct Client {
    client_id: ClientID,
    raddr: net::SocketAddr,
    protocol_version: MqttProtocol,
    connect_timeout: Option<time::Duration>,
    read_timeout: Option<time::Duration>,
    write_timeout: Option<time::Duration>,
    nodelay: Option<bool>,
    ttl: Option<u32>,
    max_packet_size: u32,
    // CONNECT options
    connopts: ConnectOptions,
    connect_properties: Option<v5::ConnectProperties>,
    connect_payload: v5::ConnectPayload,
    // CONNACK options
    connack: v5::ConnAck,

    last_rcvd: time::Instant,
    last_sent: time::Instant,
    rd_deadline: Option<time::Instant>, // defaults to None
    wt_deadline: Option<time::Instant>, // defaults to None
    next_packet_ids: VecDeque<PacketID>,

    in_packets: VecDeque<v5::Packet>,
    cio: ClientIO,
}

impl Drop for Client {
    fn drop(&mut self) {
        match &self.cio {
            ClientIO::None => (),
            _ => {
                let disconnect = {
                    let code = DisconnReasonCode::NormalDisconnect as u8;
                    v5::Disconnect {
                        code: ReasonCode::try_from(code).unwrap(),
                        properties: None,
                    }
                };
                if let Err(err) = self.write(v5::Packet::Disconnect(disconnect)) {
                    error!(
                        "client_id:{:?} raddr:{:?} drop error:{}",
                        self.client_id, self.raddr, err
                    )
                }
            }
        }
    }
}

/// Client initialization and setup
impl Client {
    /// Call this immediately after `connect` or `connect_noblock` on the ClientBuilder,
    /// else this call might panic. Returns a tuple of
    /// (read-only-client, write-onlyu-client).
    pub fn split_rw(mut self) -> io::Result<(Client, Client)> {
        let cio = mem::replace(&mut self.cio, ClientIO::None);

        let (rd_cio, wt_cio) = cio.split_sock()?;
        let reader = Client {
            client_id: self.client_id.clone(),
            raddr: self.raddr,
            protocol_version: self.protocol_version,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            nodelay: self.nodelay,
            ttl: self.ttl,
            max_packet_size: self.max_packet_size,
            // CONNECT options
            connopts: self.connopts,
            connect_properties: self.connect_properties.clone(),
            connect_payload: self.connect_payload.clone(),
            // CONNACK options
            connack: self.connack.clone(),

            last_rcvd: self.last_sent,
            last_sent: self.last_rcvd,
            rd_deadline: None,
            wt_deadline: None,
            next_packet_ids: VecDeque::default(),

            in_packets: VecDeque::default(),
            cio: rd_cio,
        };

        let _none = mem::replace(&mut self.cio, wt_cio);
        Ok((reader, self))
    }

    /// If error is detected on this `Client` instance call this method. Reconnecting
    /// will connect with the same `remote`, either in block or no-block configuration
    /// as before.
    pub fn reconnect(mut self) -> io::Result<Self> {
        let sock = match self.connect_timeout {
            Some(timeout) => net::TcpStream::connect_timeout(&self.raddr, timeout)?,
            None => net::TcpStream::connect(&self.raddr)?,
        };
        sock.set_read_timeout(self.read_timeout)?;
        sock.set_write_timeout(self.write_timeout)?;
        if let Some(nodelay) = self.nodelay {
            sock.set_nodelay(nodelay)?
        }
        if let Some(ttl) = self.ttl {
            sock.set_ttl(ttl)?
        }

        let (cio, connack) = {
            let connect = self.to_connect(false /*clean_start*/);
            let blocking = self.cio.is_blocking();
            ClientIO::handshake(&mut self, connect, sock, blocking)?
        };
        self.cio = cio;
        self.next_packet_ids = (1..connack.receive_maximum()).collect();
        self.connack = connack;

        Ok(self)
    }

    fn to_connect(&self, clean_start: bool) -> v5::Connect {
        let mut flags = vec![];

        if clean_start {
            flags.push(v5::ConnectFlags::CLEAN_START)
        }
        if self.is_will() {
            flags.push(v5::ConnectFlags::WILL_FLAG);
            flags.push(match self.connopts.will_qos.unwrap_or(v5::QoS::AtMostOnce) {
                v5::QoS::AtMostOnce => v5::ConnectFlags::WILL_QOS0,
                v5::QoS::AtLeastOnce => v5::ConnectFlags::WILL_QOS1,
                v5::QoS::ExactlyOnce => v5::ConnectFlags::WILL_QOS2,
            });
            match self.connopts.will_retain {
                Some(true) => flags.push(v5::ConnectFlags::WILL_RETAIN),
                Some(_) | None => (),
            }
        }
        match &self.connect_payload.username {
            Some(_) => flags.push(v5::ConnectFlags::USERNAME),
            None => (),
        }
        match &self.connect_payload.password {
            Some(_) => flags.push(v5::ConnectFlags::PASSWORD),
            None => (),
        }

        let mut connect = v5::Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: self.protocol_version,
            flags: v5::ConnectFlags::new(&flags),
            keep_alive: self.connopts.keep_alive,
            properties: self.connect_properties.clone(),
            payload: self.connect_payload.clone(),
        };
        connect.normalize();

        connect
    }

    fn is_will(&self) -> bool {
        self.connect_payload.will_topic.is_some()
            && self.connect_payload.will_properties.is_some()
            && self.connect_payload.will_payload.is_some()
    }
}

/// Maintanence methods
impl Client {
    /// Returns the socket address of the local half of this TCP connection.
    pub fn local_addr(&self) -> io::Result<net::SocketAddr> {
        self.cio.local_addr()
    }

    /// Returns the socket address of the remote peer of this TCP connection.
    pub fn peer_addr(&self) -> io::Result<net::SocketAddr> {
        self.cio.peer_addr()
    }

    /// Gets the value of the TCP_NODELAY option on this socket.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.cio.nodelay()
    }

    /// Returns the read timeout of this socket.
    pub fn read_timeout(&self) -> io::Result<Option<time::Duration>> {
        self.cio.read_timeout()
    }

    /// Returns the write timeout of this socket.
    pub fn write_timeout(&self) -> io::Result<Option<time::Duration>> {
        self.cio.write_timeout()
    }

    /// Gets the value of the IP_TTL option for this socket.
    pub fn ttl(&self) -> io::Result<u32> {
        self.cio.ttl()
    }
}

/// Keep alive and ping-pong.
impl Client {
    /// Return the server recommended keep_alive or configured keep_alive, in seconds.
    /// If returned keep_alive is non-ZERO, application shall make sure that there
    /// is MQTT activity within the time-period.
    pub fn keep_alive(&self) -> u16 {
        match &self.connack.properties {
            Some(props) => match props.server_keep_alive {
                Some(keep_alive) => keep_alive,
                None => self.connopts.keep_alive,
            },
            None => self.connopts.keep_alive,
        }
    }

    /// Return the duration since last server communication.
    pub fn elapsed(&self) -> time::Duration {
        self.last_rcvd.elapsed()
    }

    /// Return whether, if keep_alive non-ZERO, client's communication has exceeded 1.5
    /// times the configured `keep_alive`.
    pub fn expired(&self) -> bool {
        match self.keep_alive() {
            0 => false,
            keep_alive => {
                let keep_alive = time::Duration::from_secs(keep_alive as u64).as_micros();
                keep_alive < self.last_sent.elapsed().as_micros()
            }
        }
    }

    /// Send a PingReq to server.
    pub fn ping(&mut self) -> io::Result<()> {
        self.write(v5::Packet::PingReq)?;

        match self.read()? {
            v5::Packet::PingResp => Ok(()),
            pkt => {
                let msg = format!("expected PingResp, got {:?}", pkt.to_packet_type());
                Err(io::Error::new(io::ErrorKind::InvalidData, msg))
            }
        }?;

        Ok(())
    }
}

/// IO methods
impl Client {
    /// Subscribe one or more filters with broker. Below is an example of how to
    /// subscribe for a single filter.
    ///
    /// ```ignore
    ///     let mut sub = v5::Subscribe::default();
    ///
    ///     let filter: TopicFilter = "#".into();
    ///     let opt = {
    ///         let fwdrule = v5::RetainForwardRule::OnNewSubscribe;
    ///         let retain_as_published = true;
    ///         let no_local = true;
    ///         let qos = QoS::AtMostOnce;
    ///         v5::SubscriptionOpt::new(fwdrule, retain_as_published, no_local, qos)
    ///     };
    ///     sub.add_filter(filter, opt);
    ///
    ///     sub.set_subscription_id(0x1003);
    ///
    ///     let suback = client.subscribe(sub).expect("failed to subscribe");
    /// ```
    ///
    /// Note that this call shall block until the subscription message is sent to the
    /// remote and subscribe-ack is recieved with the same `packet_id`. [Client] shall
    /// automatically choose a `packet_id` for this subscription.
    pub fn subscribe(&mut self, mut sub: v5::Subscribe) -> io::Result<v5::SubAck> {
        sub.packet_id = self.aquire_packet_id(false /*is_publish*/).ok().unwrap();
        self.write(v5::Packet::Subscribe(sub.clone()))?;
        self.cio_read_sub_ack(&sub)
    }

    /// Disconnect with remote. Client can read packets after this call, but typically
    /// the connection is gone. But [Self::reconnect] should work.
    ///
    /// NOTE: Applications can also compose their own disconnect message via
    /// [v5::Disconnect] and send it via [Self::write] method.
    pub fn disconnect(&mut self, code: DisconnReasonCode) -> io::Result<()> {
        let code = match ReasonCode::try_from(code as u8) {
            Ok(val) => Ok(val),
            Err(err) => Err(io::Error::new(io::ErrorKind::InvalidInput, err.to_string())),
        }?;
        let disconnect = v5::Disconnect { code, properties: None };
        self.write(v5::Packet::Disconnect(disconnect))
    }

    /// Read a single packet from connection, block until a packet is available.
    pub fn read(&mut self) -> io::Result<v5::Packet> {
        match self.in_packets.pop_front() {
            Some(packet) => Ok(packet),
            None => {
                self.cio_read()?;
                Ok(self.in_packets.pop_front().unwrap())
            }
        }
    }

    /// Same as [Self::read], but does not block. Application must be prepared to
    /// handle [io::ErrorKind::WouldBlock], and [io::ErrorKind::Interrupted]
    pub fn read_noblock(&mut self) -> io::Result<v5::Packet> {
        match self.in_packets.pop_front() {
            Some(packet) => Ok(packet),
            None => {
                self.cio_read_noblock()?;
                Ok(self.in_packets.pop_front().unwrap())
            }
        }
    }

    /// Write a single packet on the connectio, block until a packet is available.
    pub fn write(&mut self, packet: v5::Packet) -> io::Result<()> {
        self.try_read()?;

        let mut cio = mem::replace(&mut self.cio, ClientIO::None);
        let res = cio.write(self, packet);
        let _none = mem::replace(&mut self.cio, cio);

        self.last_sent = time::Instant::now();
        res
    }

    /// Same as [Self::write], but does not block. Application must be prepared to
    /// handle [io::ErrorKind::WouldBlock], and [io::ErrorKind::Interrupted].
    ///
    /// To finish writing previous write pass `packet` as None. Returns [Result::Ok]
    /// only when write has finished.
    pub fn write_noblock(&mut self, packet: Option<v5::Packet>) -> io::Result<()> {
        self.try_read()?;

        let mut cio = mem::replace(&mut self.cio, ClientIO::None);
        let res = cio.write_noblock(self, packet);
        let _none = mem::replace(&mut self.cio, cio);

        self.last_sent = time::Instant::now();
        res
    }

    fn try_read(&mut self) -> io::Result<()> {
        match self.read_noblock() {
            Ok(packet) => {
                self.in_packets.push_back(packet);
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn cio_read(&mut self) -> io::Result<()> {
        let mut cio = mem::replace(&mut self.cio, ClientIO::None);
        let res = cio.read(self);
        let _none = mem::replace(&mut self.cio, cio);

        self.in_packets.push_back(res?);

        self.last_rcvd = time::Instant::now();
        Ok(())
    }

    fn cio_read_noblock(&mut self) -> io::Result<()> {
        let mut cio = mem::replace(&mut self.cio, ClientIO::None);
        let res = cio.read_noblock(self);
        let _none = mem::replace(&mut self.cio, cio);

        self.in_packets.push_back(res?);

        self.last_rcvd = time::Instant::now();
        Ok(())
    }

    fn cio_read_sub_ack(&mut self, sub: &v5::Subscribe) -> io::Result<v5::SubAck> {
        loop {
            self.cio_read()?;
            match self.in_packets.pop_front() {
                Some(v5::Packet::SubAck(sa)) if sa.packet_id == sub.packet_id => {
                    break Ok(sa);
                }
                Some(v5::Packet::SubAck(sa)) => warn!(
                    "sub-ack mismatch in packet_id {} != {}",
                    sa.packet_id, sub.packet_id
                ),
                Some(pkt) => self.in_packets.push_back(pkt),
                None => unreachable!(),
            }
        }
    }
}

impl Client {
    /// Obtain the underlying [mio] socket to register with [mio::Poll]. This can be
    /// used to create an async wrapper. Calling this method on blocking connection
    /// will panic.
    pub fn as_mut_mio_tcpstream(&mut self) -> &mut mio::net::TcpStream {
        use ClientIO::*;

        match &mut self.cio {
            Blocking { .. } | BlockRd { .. } | BlockWt { .. } => {
                panic!("cannot use mio on standard socket")
            }
            NoBlock { sock, .. } | NoBlockRd { sock, .. } | NoBlockWt { sock, .. } => {
                sock
            }
            ClientIO::None => unreachable!(),
        }
    }

    fn acquire_packet_id(&mut self, publish: bool) -> Result<PacketID> {
        if publish {
            match self.next_packet_ids.pop_front() {
                Some(packet_id) => Ok(packet_id),
                None => err!(ProtocolError, code: ExceededReceiveMaximum, ""),
            }
        } else {
            Ok(0)
        }
    }

    fn release_packet_id(&mut self, packet_id: PacketID) {
        self.next_packet_ids.push_back();
    }
}

#[allow(dead_code)]
enum ClientIO {
    Blocking {
        sock: net::TcpStream,
        pktr: MQTTRead,
        pktw: MQTTWrite,
    },
    NoBlock {
        sock: mio::net::TcpStream,
        pktr: MQTTRead,
        pktw: MQTTWrite,
    },
    BlockRd {
        sock: net::TcpStream,
        pktr: MQTTRead,
    },
    BlockWt {
        sock: net::TcpStream,
        pktw: MQTTWrite,
    },
    NoBlockRd {
        sock: mio::net::TcpStream,
        pktr: MQTTRead,
    },
    NoBlockWt {
        sock: mio::net::TcpStream,
        pktw: MQTTWrite,
    },
    None,
}

impl fmt::Debug for ClientIO {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            ClientIO::Blocking { .. } => write!(f, "ClientIO::Blocking"),
            ClientIO::NoBlock { .. } => write!(f, "ClientIO::NoBlock"),
            ClientIO::BlockRd { .. } => write!(f, "ClientIO::BlockRd"),
            ClientIO::BlockWt { .. } => write!(f, "ClientIO::BlockWt"),
            ClientIO::NoBlockRd { .. } => write!(f, "ClientIO::NoBlockRd"),
            ClientIO::NoBlockWt { .. } => write!(f, "ClientIO::NoBlockWt"),
            ClientIO::None { .. } => write!(f, "ClientIO::None"),
        }
    }
}

impl ClientIO {
    fn handshake(
        client: &mut Client,
        connect: v5::Connect,
        mut sock: net::TcpStream,
        blocking: bool,
    ) -> io::Result<(ClientIO, v5::ConnAck)> {
        let max_packet_size = connect.max_packet_size(client.max_packet_size);

        let mut pktr = MQTTRead::new(max_packet_size);
        let mut pktw = MQTTWrite::new(&[], max_packet_size);

        write_packet(
            client,
            &mut sock,
            &mut pktw,
            Some(v5::Packet::Connect(connect)),
            true, // block
        )?;

        let (val, connack) = match read_packet(client, &mut sock, &mut pktr, true)? {
            v5::Packet::ConnAck(connack) => (pktr, connack),
            pkt => {
                let msg = format!("unexpected in handshake {:?}", pkt.to_packet_type());
                Err(io::Error::new(io::ErrorKind::InvalidData, msg))?
            }
        };

        pktr = val;

        let cio = match blocking {
            true => ClientIO::Blocking { sock, pktr, pktw },
            false => {
                let sock = mio::net::TcpStream::from_std(sock);
                ClientIO::NoBlock { sock, pktr, pktw }
            }
        };

        Ok((cio, connack))
    }

    fn split_sock(self) -> io::Result<(ClientIO, ClientIO)> {
        match self {
            ClientIO::Blocking { sock: rd_sock, pktr, pktw } => {
                let wt_sock = rd_sock.try_clone()?;
                rd_sock.shutdown(net::Shutdown::Write)?;
                wt_sock.shutdown(net::Shutdown::Read)?;
                let rd = ClientIO::BlockRd { sock: rd_sock, pktr };
                let wt = ClientIO::BlockWt { sock: wt_sock, pktw };
                Ok((rd, wt))
            }
            ClientIO::NoBlock { sock, pktr, pktw } => {
                #[cfg(windows)]
                let rd_sock =
                    unsafe { net::TcpStream::from_raw_socket(sock.into_raw_socket()) };
                #[cfg(unix)]
                let rd_sock = unsafe { net::TcpStream::from_raw_fd(sock.into_raw_fd()) };

                let wt_sock = rd_sock.try_clone()?;
                rd_sock.shutdown(net::Shutdown::Write)?;
                wt_sock.shutdown(net::Shutdown::Read)?;
                let rd = ClientIO::NoBlockRd {
                    sock: mio::net::TcpStream::from_std(rd_sock),
                    pktr,
                };
                let wt = ClientIO::NoBlockWt {
                    sock: mio::net::TcpStream::from_std(wt_sock),
                    pktw,
                };
                Ok((rd, wt))
            }
            _ => unreachable!(),
        }
    }

    fn local_addr(&self) -> io::Result<net::SocketAddr> {
        match self {
            ClientIO::Blocking { sock, .. } => sock.local_addr(),
            ClientIO::NoBlock { sock, .. } => sock.local_addr(),
            ClientIO::BlockRd { sock, .. } => sock.local_addr(),
            ClientIO::BlockWt { sock, .. } => sock.local_addr(),
            ClientIO::NoBlockRd { sock, .. } => sock.local_addr(),
            ClientIO::NoBlockWt { sock, .. } => sock.local_addr(),
            ClientIO::None => unreachable!(),
        }
    }

    fn peer_addr(&self) -> io::Result<net::SocketAddr> {
        match self {
            ClientIO::Blocking { sock, .. } => sock.peer_addr(),
            ClientIO::NoBlock { sock, .. } => sock.peer_addr(),
            ClientIO::BlockRd { sock, .. } => sock.peer_addr(),
            ClientIO::BlockWt { sock, .. } => sock.peer_addr(),
            ClientIO::NoBlockRd { sock, .. } => sock.peer_addr(),
            ClientIO::NoBlockWt { sock, .. } => sock.peer_addr(),
            ClientIO::None => unreachable!(),
        }
    }

    fn nodelay(&self) -> io::Result<bool> {
        match self {
            ClientIO::Blocking { sock, .. } => sock.nodelay(),
            ClientIO::NoBlock { sock, .. } => sock.nodelay(),
            ClientIO::BlockRd { sock, .. } => sock.nodelay(),
            ClientIO::BlockWt { sock, .. } => sock.nodelay(),
            ClientIO::NoBlockRd { sock, .. } => sock.nodelay(),
            ClientIO::NoBlockWt { sock, .. } => sock.nodelay(),
            ClientIO::None => unreachable!(),
        }
    }

    fn read_timeout(&self) -> io::Result<Option<time::Duration>> {
        match self {
            ClientIO::Blocking { sock, .. } => sock.read_timeout(),
            ClientIO::NoBlock { .. } => Ok(None),
            ClientIO::BlockRd { sock, .. } => sock.read_timeout(),
            ClientIO::BlockWt { sock, .. } => sock.read_timeout(),
            ClientIO::NoBlockRd { .. } => Ok(None),
            ClientIO::NoBlockWt { .. } => Ok(None),
            ClientIO::None => unreachable!(),
        }
    }

    fn write_timeout(&self) -> io::Result<Option<time::Duration>> {
        match self {
            ClientIO::Blocking { sock, .. } => sock.write_timeout(),
            ClientIO::NoBlock { .. } => Ok(None),
            ClientIO::BlockRd { sock, .. } => sock.write_timeout(),
            ClientIO::BlockWt { sock, .. } => sock.write_timeout(),
            ClientIO::NoBlockRd { .. } => Ok(None),
            ClientIO::NoBlockWt { .. } => Ok(None),
            ClientIO::None => unreachable!(),
        }
    }

    fn ttl(&self) -> io::Result<u32> {
        match self {
            ClientIO::Blocking { sock, .. } => sock.ttl(),
            ClientIO::NoBlock { sock, .. } => sock.ttl(),
            ClientIO::BlockRd { sock, .. } => sock.ttl(),
            ClientIO::BlockWt { sock, .. } => sock.ttl(),
            ClientIO::NoBlockRd { sock, .. } => sock.ttl(),
            ClientIO::NoBlockWt { sock, .. } => sock.ttl(),
            ClientIO::None => unreachable!(),
        }
    }

    fn is_blocking(&self) -> bool {
        use ClientIO::*;

        match self {
            Blocking { .. } | BlockRd { .. } | BlockWt { .. } => true,
            NoBlock { .. } | NoBlockRd { .. } | NoBlockWt { .. } => false,
            None => unreachable!(),
        }
    }
}

impl ClientIO {
    fn read(&mut self, client: &mut Client) -> io::Result<v5::Packet> {
        match self {
            ClientIO::Blocking { sock, pktr, .. } => {
                read_packet(client, sock, pktr, true)
            }
            ClientIO::NoBlock { sock, pktr, .. } => {
                read_packet(client, sock, pktr, false)
            }
            ClientIO::BlockRd { sock, pktr, .. } => {
                //
                read_packet(client, sock, pktr, true)
            }
            ClientIO::NoBlockRd { sock, pktr, .. } => {
                read_packet(client, sock, pktr, false)
            }
            ClientIO::None => {
                let s = format!("disconnected while Client::read");
                Err(io::Error::new(io::ErrorKind::ConnectionReset, s))
            }
            _ => unreachable!(),
        }
    }

    fn read_noblock(&mut self, client: &mut Client) -> io::Result<v5::Packet> {
        match self {
            ClientIO::Blocking { sock, pktr, .. } => {
                sock.set_nonblocking(true)?;
                let res = read_packet(client, sock, pktr, false);
                sock.set_nonblocking(false)?;
                res
            }
            ClientIO::NoBlock { sock, pktr, .. } => {
                read_packet(client, sock, pktr, false)
            }
            ClientIO::BlockRd { sock, pktr, .. } => {
                sock.set_nonblocking(true)?;
                let res = read_packet(client, sock, pktr, false);
                sock.set_nonblocking(false)?;
                res
            }
            ClientIO::NoBlockRd { sock, pktr, .. } => {
                read_packet(client, sock, pktr, false)
            }
            ClientIO::None => {
                let s = format!("disconnected while Client::read_noblock");
                Err(io::Error::new(io::ErrorKind::ConnectionReset, s))
            }
            _ => unreachable!(),
        }
    }

    fn write(&mut self, client: &mut Client, pkt: v5::Packet) -> io::Result<()> {
        match self {
            ClientIO::Blocking { sock, pktw, .. } => {
                write_packet(client, sock, pktw, Some(pkt), true)
            }
            ClientIO::NoBlock { sock, pktw, .. } => {
                write_packet(client, sock, pktw, Some(pkt), true)
            }
            ClientIO::BlockWt { sock, pktw, .. } => {
                write_packet(client, sock, pktw, Some(pkt), true)
            }
            ClientIO::NoBlockWt { sock, pktw, .. } => {
                write_packet(client, sock, pktw, Some(pkt), true)
            }
            ClientIO::None => {
                let s = format!("disconnected while Client::write");
                Err(io::Error::new(io::ErrorKind::ConnectionReset, s))
            }
            cio => unreachable!("{:?}", cio),
        }
    }

    fn write_noblock(
        &mut self,
        client: &mut Client,
        pkt: Option<v5::Packet>,
    ) -> io::Result<()> {
        match self {
            ClientIO::Blocking { sock, pktw, .. } => {
                write_packet(client, sock, pktw, pkt, false)
            }
            ClientIO::NoBlock { sock, pktw, .. } => {
                write_packet(client, sock, pktw, pkt, false)
            }
            ClientIO::BlockWt { sock, pktw, .. } => {
                write_packet(client, sock, pktw, pkt, false)
            }
            ClientIO::NoBlockWt { sock, pktw, .. } => {
                write_packet(client, sock, pktw, pkt, false)
            }
            ClientIO::None => {
                let s = format!("disconnected while Client::write_noblock");
                Err(io::Error::new(io::ErrorKind::ConnectionReset, s))
            }
            cio => unreachable!("{:?}", cio),
        }
    }
}

impl Client {
    fn read_elapsed(&self) -> bool {
        match &self.rd_deadline {
            Some(deadline) if &time::Instant::now() > deadline => true,
            Some(_) | None => false,
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<time::Duration>) {
        match timeout {
            Some(timeout) => {
                let now = time::Instant::now();
                self.rd_deadline = Some(now.checked_add(timeout).unwrap());
            }
            None => {
                self.rd_deadline = None;
            }
        }
    }

    fn write_elapsed(&self) -> bool {
        match &self.wt_deadline {
            Some(deadline) if &time::Instant::now() > deadline => true,
            Some(_) | None => false,
        }
    }

    fn set_write_timeout(&mut self, timeout: Option<time::Duration>) {
        match timeout {
            Some(timeout) => {
                let now = time::Instant::now();
                self.rd_deadline = Some(now.checked_add(timeout).unwrap());
            }
            None => {
                self.rd_deadline = None;
            }
        }
    }
}

fn read_packet<R>(
    client: &mut Client,
    sock: &mut R,
    pktr: &mut MQTTRead,
    block: bool,
) -> io::Result<v5::Packet>
where
    R: io::Read,
{
    use crate::MQTTRead::{Fin, Header, Init, Remain};

    client.set_read_timeout(client.read_timeout);

    let mut pr = mem::replace(pktr, MQTTRead::default());
    let max_packet_size = pr.to_max_packet_size();

    let res = loop {
        pr = match pr.read(sock) {
            Ok((val, true)) if !block => {
                pr = val;
                break Err(io::Error::new(io::ErrorKind::WouldBlock, ""));
            }
            Ok((val, _)) => val,
            Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                pr = MQTTRead::new(max_packet_size);
                let s = format!("malformed packet from MQTTRead");
                break Err(io::Error::new(io::ErrorKind::InvalidData, s));
            }
            Err(err) if err.kind() == ErrorKind::ProtocolError => {
                pr = MQTTRead::new(max_packet_size);
                let s = format!("protocol error from MQTTRead");
                break Err(io::Error::new(io::ErrorKind::InvalidData, s));
            }
            Err(err) if err.kind() == ErrorKind::Disconnected => {
                pr = MQTTRead::new(max_packet_size);
                let s = format!("client disconnected in MQTTRead");
                break Err(io::Error::new(io::ErrorKind::ConnectionReset, s));
            }
            Err(err) => unreachable!("unexpected error {}", err),
        };

        match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !client.read_elapsed() => {
                trace!("read retrying");
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                let s = format!("disconnect, read timesout {:?}", client.read_timeout);
                break Err(io::Error::new(io::ErrorKind::TimedOut, s));
            }
            Fin { .. } => {
                client.set_read_timeout(None);
                match pr.parse() {
                    Ok(pkt) => {
                        pr = pr.reset();
                        break Ok(pkt);
                    }
                    Err(err) => {
                        pr = pr.reset();
                        let s = err.to_string();
                        break Err(io::Error::new(io::ErrorKind::InvalidData, s));
                    }
                }
            }
            MQTTRead::None => unreachable!(),
        };
    };

    let _none = mem::replace(pktr, pr);
    res
}

fn write_packet<W>(
    client: &mut Client,
    sock: &mut W,
    pktw: &mut MQTTWrite,
    pkt: Option<v5::Packet>,
    block: bool,
) -> io::Result<()>
where
    W: io::Write,
{
    use crate::MQTTWrite::{Fin, Init, Remain};

    client.set_write_timeout(client.write_timeout);

    let mut pw = mem::replace(pktw, MQTTWrite::default());
    let max_packet_size = pw.to_max_packet_size();

    if let Some(pkt) = pkt {
        match pkt.encode() {
            Ok(blob) => {
                pw = pw.reset(blob.as_ref());
            }
            Err(err) => {
                pw = mem::replace(pktw, pw);
                Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            }
        }
    }

    let res = loop {
        pw = match pw.write(sock) {
            Ok((val, true)) if !block => {
                pw = val;
                break Err(io::Error::new(io::ErrorKind::WouldBlock, ""));
            }
            Ok((val, _)) => val,
            Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                pw = MQTTWrite::new(&[], max_packet_size);
                let s = format!("malformed packet in MQTTWrite");
                break Err(io::Error::new(io::ErrorKind::InvalidData, s));
            }
            Err(err) if err.kind() == ErrorKind::ProtocolError => {
                pw = MQTTWrite::new(&[], max_packet_size);
                let s = format!("protocol error in MQTTWrite");
                break Err(io::Error::new(io::ErrorKind::InvalidData, s));
            }
            Err(err) if err.kind() == ErrorKind::Disconnected => {
                pw = MQTTWrite::new(&[], max_packet_size);
                let s = format!("client disconnected in MQTTWrite");
                break Err(io::Error::new(io::ErrorKind::ConnectionReset, s));
            }
            Err(err) => unreachable!("unexpected error {}", err),
        };

        match &pw {
            Init { .. } | Remain { .. } if !client.write_elapsed() => {
                trace!("write retrying");
            }
            Init { .. } | Remain { .. } => {
                let s = format!("packet write fail after {:?}", client.write_timeout);
                break Err(io::Error::new(io::ErrorKind::TimedOut, s));
            }
            Fin { .. } => {
                client.set_write_timeout(None);
                break Ok(());
            }
            MQTTWrite::None => unreachable!(),
        };
    };

    let _none = mem::replace(pktw, pw);
    res
}

const PP: &'static str = "Client::Disconnect";

#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum DisconnReasonCode {
    NormalDisconnect = 0x00,
    DisconnectWillMessage = 0x04,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationError = 0x83,
    TopicNameInvalid = 0x90,
    ExceededReceiveMaximum = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    ExceedMessageRate = 0x96,
    QuotaExceeded = 0x97,
    AdminAction = 0x98,
    PayloadFormatInvalid = 0x99,
}

impl From<DisconnReasonCode> for u8 {
    fn from(val: DisconnReasonCode) -> u8 {
        use DisconnReasonCode::*;

        match val {
            NormalDisconnect => 0x00,
            DisconnectWillMessage => 0x04,
            UnspecifiedError => 0x80,
            MalformedPacket => 0x81,
            ProtocolError => 0x82,
            ImplementationError => 0x83,
            TopicNameInvalid => 0x90,
            ExceededReceiveMaximum => 0x93,
            TopicAliasInvalid => 0x94,
            PacketTooLarge => 0x95,
            ExceedMessageRate => 0x96,
            QuotaExceeded => 0x97,
            AdminAction => 0x98,
            PayloadFormatInvalid => 0x99,
        }
    }
}

impl TryFrom<u8> for DisconnReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<DisconnReasonCode> {
        match val {
            0x00 => Ok(DisconnReasonCode::NormalDisconnect),
            0x04 => Ok(DisconnReasonCode::DisconnectWillMessage),
            0x80 => Ok(DisconnReasonCode::UnspecifiedError),
            0x81 => Ok(DisconnReasonCode::MalformedPacket),
            0x82 => Ok(DisconnReasonCode::ProtocolError),
            0x83 => Ok(DisconnReasonCode::ImplementationError),
            0x90 => Ok(DisconnReasonCode::TopicNameInvalid),
            0x93 => Ok(DisconnReasonCode::ExceededReceiveMaximum),
            0x94 => Ok(DisconnReasonCode::TopicAliasInvalid),
            0x95 => Ok(DisconnReasonCode::PacketTooLarge),
            0x96 => Ok(DisconnReasonCode::ExceedMessageRate),
            0x97 => Ok(DisconnReasonCode::QuotaExceeded),
            0x98 => Ok(DisconnReasonCode::AdminAction),
            0x99 => Ok(DisconnReasonCode::PayloadFormatInvalid),
            val => {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    " {} reason-code {}",
                    PP,
                    val
                )
            }
        }
    }
}
