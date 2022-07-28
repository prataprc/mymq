//! Clients can send CONNECT, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP,
//! SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT, AUTH packets
//!
//! Clients can recieve CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP,
//! SUBACK, UNSUBACK, PINGRESP, DISCONNECT, AUTH packets

#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::unix::io::{FromRawSocket, IntoRawSocket};

use std::{io, mem, net, time};

use crate::{v5, ClientID, Config, MQTTRead, MQTTWrite, MqttProtocol, Packetize};

pub struct ClientBuilder {
    /// Provide a unique client identifier, if missing, will be sent empty in CONNECT.
    pub client_id: Option<ClientID>,
    /// Socket settings for blocking io, refer [io::TcpStream::connect_timeout]
    pub connect_timeout: Option<time::Duration>,
    /// Socket settings for blocking io, refer [i ::TcpStream::set_read_timeout]
    pub read_timeout: Option<time::Duration>,
    /// Socket settings for blocking io, refer [io::TcpStream::set_write_timeout]
    pub write_timeout: Option<time::Duration>,
    /// Socket settings, refer [io::TcpStream::set_nodelay].
    pub nodelay: Option<bool>,
    /// Socket settings, refer [io::TcpStream::set_ttl].
    pub ttl: Option<u32>,
    // CONNECT options
    pub clean_start: bool,
    pub will_qos: Option<v5::QoS>,
    pub will_retain: Option<bool>,
    pub keep_alive: u16,
    pub connect_properties: Option<v5::ConnectProperties>,
    pub connect_payload: v5::ConnectPayload,

    protocol_version: MqttProtocol,
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
            // CONNECT options
            clean_start: true,
            keep_alive: 0,
            will_qos: None,
            will_retain: None,
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
    pub fn connect(self, remote: net::SocketAddr) -> io::Result<Client> {
        let sock = match self.connect_timeout {
            Some(timeout) => net::TcpStream::connect_timeout(&remote, timeout)?,
            None => net::TcpStream::connect(&remote)?,
        };
        sock.set_read_timeout(self.read_timeout)?;
        sock.set_write_timeout(self.write_timeout)?;
        if let Some(nodelay) = self.nodelay {
            sock.set_nodelay(nodelay)?
        }
        if let Some(ttl) = self.ttl {
            sock.set_ttl(ttl)?
        }

        self.handshake(sock, true /*blocking*/)
    }

    /// Connection with `remote` and start an asynchronous client. All read/write calls
    /// and other communication methods, on the returned client, shall not block.
    /// Application will have to for [io::ErrorKind::WouldBlock] and
    /// [io::ErrorKind::Interrupted] returns.
    ///
    /// NOTE: This call shall block until CONNACK is successfully received from remote.
    pub fn connect_noblock(self, remote: net::SocketAddr) -> io::Result<Client> {
        let sock = net::TcpStream::connect(remote)?;
        if let Some(nodelay) = self.nodelay {
            sock.set_nodelay(nodelay)?
        }
        if let Some(ttl) = self.ttl {
            sock.set_ttl(ttl)?
        }

        self.handshake(sock, false /*blocking*/)
    }

    fn to_connect(&self) -> v5::Connect {
        let mut flags = vec![];
        if self.clean_start {
            flags.push(v5::ConnectFlags::CLEAN_START);
        }
        if self.is_will() {
            flags.push(v5::ConnectFlags::WILL_FLAG);
            flags.push(match self.will_qos.unwrap_or(v5::QoS::AtMostOnce) {
                v5::QoS::AtMostOnce => v5::ConnectFlags::WILL_QOS0,
                v5::QoS::AtLeastOnce => v5::ConnectFlags::WILL_QOS1,
                v5::QoS::ExactlyOnce => v5::ConnectFlags::WILL_QOS2,
            });
            match self.will_retain {
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

        let mut pkt = v5::Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: self.protocol_version,
            flags: v5::ConnectFlags::new(&flags),
            keep_alive: self.keep_alive,
            properties: self.connect_properties.clone(),
            payload: self.connect_payload.clone(),
        };
        pkt.normalize();

        pkt
    }

    fn is_will(&self) -> bool {
        self.connect_payload.will_topic.is_some()
            && self.connect_payload.will_properties.is_some()
            && self.connect_payload.will_payload.is_some()
    }

    fn handshake(self, mut sock: net::TcpStream, blocking: bool) -> io::Result<Client> {
        let max_packet_size = self.max_packet_size();
        let remote = sock.peer_addr()?;
        let mut pktr = MQTTRead::new(max_packet_size);
        let mut pktw = MQTTWrite::new(&[], max_packet_size);

        // handshake with remote
        pktw = write_pkt(pktw, &mut sock, v5::Packet::Connect(self.to_connect()))?;
        let (val, connack) = match read_pkt(pktr, &mut sock)? {
            (val, v5::Packet::ConnAck(connack)) => (val, connack),
            (_, pkt) => {
                let msg =
                    format!("unexpected packet in handshake {:?}", pkt.to_packet_type());
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

        let mut client = self.into_client(remote);
        client.cio = cio;
        client.connack = connack;

        Ok(client)
    }

    fn max_packet_size(&self) -> u32 {
        match &self.connect_properties {
            Some(props) => props.max_packet_size(),
            None => Config::DEF_MQTT_MAX_PACKET_SIZE,
        }
    }

    fn into_client(self, remote: net::SocketAddr) -> Client {
        Client {
            client_id: self.client_id.unwrap_or_else(|| ClientID::new_uuid_v4()),
            remote,
            protocol_version: self.protocol_version,
            // CONNECT options
            clean_start: self.clean_start,
            will_qos: self.will_qos,
            will_retain: self.will_retain,
            keep_alive: self.keep_alive,
            connect_properties: self.connect_properties.clone(),
            connect_payload: self.connect_payload.clone(),
            // CONNACK options
            connack: v5::ConnAck::default(),

            last_tx: time::Instant::now(),
            last_rx: time::Instant::now(),
            cio: ClientIO::None,
        }
    }
}

pub struct Client {
    client_id: ClientID,
    remote: net::SocketAddr,
    protocol_version: MqttProtocol,
    // CONNECT options
    clean_start: bool,
    will_qos: Option<v5::QoS>,
    will_retain: Option<bool>,
    keep_alive: u16,
    connect_properties: Option<v5::ConnectProperties>,
    connect_payload: v5::ConnectPayload,
    // CONNACK options
    connack: v5::ConnAck,

    last_rx: time::Instant,
    last_tx: time::Instant,
    cio: ClientIO,
}

/// Client administration methods.
impl Client {
    /// Call this immediately after `connect` or `connect_noblock` on the ClientBuilder,
    /// else this call might panic. Returns a clone of underlying socket with read-only
    /// permission. After calling this method, `self` becomes a write-only instance.
    pub fn clone_read(&mut self) -> io::Result<Client> {
        let cio = mem::replace(&mut self.cio, ClientIO::None);
        let (rd_cio, wt_cio) = cio.split_sock()?;
        let reader = Client {
            client_id: self.client_id.clone(),
            remote: self.remote,
            protocol_version: self.protocol_version,
            // CONNECT options
            clean_start: self.clean_start,
            will_qos: self.will_qos,
            will_retain: self.will_retain,
            keep_alive: self.keep_alive,
            connect_properties: self.connect_properties.clone(),
            connect_payload: self.connect_payload.clone(),
            // CONNACK options
            connack: self.connack.clone(),

            last_rx: self.last_tx,
            last_tx: self.last_rx,
            cio: rd_cio,
        };

        let _none = mem::replace(&mut self.cio, wt_cio);
        Ok(reader)
    }

    /// Return the server recommended keep_alive or configured keep_alive, in seconds.
    /// If returned keep_alive is non-ZERO, application shall make sure that there
    /// is MQTT activity within the time-period.
    pub fn keep_alive(&self) -> u16 {
        match &self.connack.properties {
            Some(props) => match props.server_keep_alive {
                Some(keep_alive) => keep_alive,
                None => self.keep_alive,
            },
            None => self.keep_alive,
        }
    }

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

    /// Return the duration since last server communication.
    pub fn elapsed(&self) -> time::Duration {
        self.last_rx.elapsed()
    }

    /// Return whether, if keep_alive non-ZERO, client's communication has exceeded 1.5
    /// times the configured `keep_alive`.
    pub fn expired(&self) -> bool {
        if self.keep_alive == 0 {
            false
        } else {
            let keep_alive = u64::from(self.keep_alive);
            let micros = time::Duration::from_secs(keep_alive).as_micros() as f64;
            ((micros * 1.5) as u128) < self.last_tx.elapsed().as_micros()
        }
    }
}

/// Client communication methods.
impl Client {
    /// Send a PingReq to server.
    pub fn ping(&mut self) -> io::Result<()> {
        self.last_tx = time::Instant::now();
        todo!()
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
}

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

impl ClientIO {
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
            ClientIO::Blocking { sock, .. } => sock.read_timeout(),
            ClientIO::NoBlock { .. } => Ok(None),
            ClientIO::BlockRd { sock, .. } => sock.read_timeout(),
            ClientIO::BlockWt { sock, .. } => sock.read_timeout(),
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

    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        todo!()
    }

    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        todo!()
    }
}

fn write_pkt<W>(
    mut pktw: MQTTWrite,
    sock: &mut W,
    pkt: v5::Packet,
) -> io::Result<MQTTWrite>
where
    W: io::Write,
{
    use std::error::Error;

    let blob = pkt
        .encode()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    pktw = pktw.reset(blob.as_ref());

    loop {
        pktw = match pktw.write(sock) {
            Ok((pktw, false)) => break Ok(pktw),
            Ok((pktw, true)) => pktw,
            Err(err) => match err.source() {
                Some(source) => match source.downcast_ref::<io::Error>() {
                    Some(err) => break Err(err.kind().into()),
                    None => break Err(io::ErrorKind::Other.into()),
                },
                None => break Err(io::ErrorKind::Other.into()),
            },
        }
    }
}

fn read_pkt<R>(mut pktr: MQTTRead, sock: &mut R) -> io::Result<(MQTTRead, v5::Packet)>
where
    R: io::Read,
{
    use std::error::Error;
    pktr = pktr.reset();

    let res: io::Result<MQTTRead> = loop {
        pktr = match pktr.read(sock) {
            Ok((pktr, false)) => break Ok(pktr),
            Ok((pktr, true)) => pktr,
            Err(err) => match err.source() {
                Some(source) => match source.downcast_ref::<io::Error>() {
                    Some(err) => break Err(err.kind().into()),
                    None => break Err(io::ErrorKind::Other.into()),
                },
                None => break Err(io::ErrorKind::Other.into()),
            },
        }
    };

    pktr = res?;

    match pktr.parse() {
        Ok(pkt) => Ok((pktr, pkt)),
        Err(err) => match err.source() {
            Some(source) => match source.downcast_ref::<io::Error>() {
                Some(err) => Err(err.kind().into()),
                None => Err(io::ErrorKind::Other.into()),
            },
            None => Err(io::ErrorKind::Other.into()),
        },
    }
}
