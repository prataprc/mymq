//! Module implement MQTT Client.

use log::trace;

#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::unix::io::{FromRawSocket, IntoRawSocket};

use std::{io, mem, net, time};

use crate::{v5, ClientID, MQTTRead, MQTTWrite, MqttProtocol, Packetize};

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
    /// Socket settings for blocking io, refer [net::TcpStream::connect_timeout]
    pub connect_timeout: Option<time::Duration>,
    /// Socket settings for blocking io, refer [net::TcpStream::set_read_timeout]
    pub read_timeout: Option<time::Duration>,
    /// Socket settings for blocking io, refer [net::TcpStream::set_write_timeout]
    pub write_timeout: Option<time::Duration>,
    /// Socket settings, refer [net::TcpStream::set_nodelay].
    pub nodelay: Option<bool>,
    /// Socket settings, refer [net::TcpStream::set_ttl].
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

        let mut client = self.into_client(remote);

        let (cio, connack) = {
            let connect = client.to_connect(true /*clean_start*/);
            let blocking = true;
            ClientIO::handshake(&client, connect, sock, blocking)?
        };

        client.cio = cio;
        client.connack = connack;

        Ok(client)
    }

    /// Connection with `remote` and start an asynchronous client. All read/write calls
    /// and other communication methods, on the returned client, shall not block.
    /// Application will have to check for [io::ErrorKind::WouldBlock] and
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

        let mut client = self.into_client(remote);

        let (cio, connack) = {
            let connect = client.to_connect(true /*clean_start*/);
            let blocking = false;
            ClientIO::handshake(&client, connect, sock, blocking)?
        };

        client.cio = cio;
        client.connack = connack;

        Ok(client)
    }

    fn into_client(self, remote: net::SocketAddr) -> Client {
        Client {
            client_id: self.client_id.unwrap_or_else(|| ClientID::new_uuid_v4()),
            remote,
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
            cio: ClientIO::None,
        }
    }
}

/// Type to interface with MQTT broker.
pub struct Client {
    client_id: ClientID,
    remote: net::SocketAddr,
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
    cio: ClientIO,
}

/// Client initialization and setup
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
            cio: rd_cio,
        };

        let _none = mem::replace(&mut self.cio, wt_cio);
        Ok(reader)
    }

    /// If error is detected on this `Client` instance call this method. Reconnecting
    /// will connect with the same `remote`, either in block or no-block configuration
    /// as before.
    pub fn reconnect(mut self) -> io::Result<Self> {
        let sock = match self.connect_timeout {
            Some(timeout) => net::TcpStream::connect_timeout(&self.remote, timeout)?,
            None => net::TcpStream::connect(&self.remote)?,
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
            ClientIO::handshake(&self, connect, sock, self.cio.is_blocking())?
        };
        self.cio = cio;
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
        if self.connopts.keep_alive == 0 {
            false
        } else {
            let keep_alive = u64::from(self.connopts.keep_alive);
            let micros = time::Duration::from_secs(keep_alive).as_micros() as f64;
            ((micros * 1.5) as u128) < self.last_sent.elapsed().as_micros()
        }
    }

    /// Send a PingReq to server.
    pub fn ping(&mut self) -> io::Result<()> {
        self.last_sent = time::Instant::now();
        let mut cio = mem::replace(&mut self.cio, ClientIO::None);

        cio.write_packet(self, v5::Packet::PingReq)?;

        match cio.read_packet(self)? {
            v5::Packet::PingResp => Ok(()),
            pkt => {
                let msg = format!("expected PingResp, got {:?}", pkt.to_packet_type());
                Err(io::Error::new(io::ErrorKind::InvalidData, msg))
            }
        }?;

        let _none = mem::replace(&mut self.cio, cio);

        Ok(())
    }
}

/// IO methods
impl Client {
    #[cfg(feature = "fuzzy")]
    pub fn read_packet(&mut self) -> io::Result<v5::Packet> {
        let mut cio = mem::replace(&mut self.cio, ClientIO::None);
        let packet = cio.read_packet(self)?;
        let _none = mem::replace(&mut self.cio, cio);
        Ok(packet)
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

impl ClientIO {
    fn handshake(
        client: &Client,
        connect: v5::Connect,
        mut sock: net::TcpStream,
        blocking: bool,
    ) -> io::Result<(ClientIO, v5::ConnAck)> {
        let max_packet_size = connect.max_packet_size(client.max_packet_size);

        let mut pktr = MQTTRead::new(max_packet_size);
        let mut pktw = MQTTWrite::new(&[], max_packet_size);

        write_packet(client, &mut sock, &mut pktw, v5::Packet::Connect(connect))?;

        let (val, connack) = match read_packet(client, &mut sock, &mut pktr)? {
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
    fn read_packet(&mut self, client: &Client) -> io::Result<v5::Packet> {
        match self {
            ClientIO::Blocking { sock, pktr, .. } => read_packet(client, sock, pktr),
            ClientIO::NoBlock { sock, pktr, .. } => read_packet(client, sock, pktr),
            ClientIO::BlockRd { sock, pktr, .. } => read_packet(client, sock, pktr),
            ClientIO::NoBlockRd { sock, pktr, .. } => read_packet(client, sock, pktr),
            _ => unreachable!(),
        }
    }

    fn write_packet(&mut self, client: &Client, pkt: v5::Packet) -> io::Result<()> {
        match self {
            ClientIO::Blocking { sock, pktw, .. } => {
                write_packet(client, sock, pktw, pkt)
            }
            ClientIO::NoBlock { sock, pktw, .. } => write_packet(client, sock, pktw, pkt),
            ClientIO::BlockWt { sock, pktw, .. } => write_packet(client, sock, pktw, pkt),
            ClientIO::NoBlockWt { sock, pktw, .. } => {
                write_packet(client, sock, pktw, pkt)
            }
            _ => unreachable!(),
        }
    }
}

fn read_packet<R>(
    client: &Client,
    sock: &mut R,
    pktr: &mut MQTTRead,
) -> io::Result<v5::Packet>
where
    R: io::Read,
{
    use crate::MQTTRead::{Fin, Header, Init, Remain};

    let mut timeout = RwTimeout::default();
    timeout.set_read_timeout(client.read_timeout);

    let mut pr = mem::replace(pktr, MQTTRead::default());

    let res = loop {
        let (val, _would_block) = match pr.read(sock) {
            Ok(tuple) => tuple,
            Err(err) => Err(io::Error::new(io::ErrorKind::BrokenPipe, err.to_string()))?,
        };
        pr = val;

        match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !timeout.read_elapsed() => {
                trace!("read retrying");
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                let s = format!("disconnect, pkt-read timesout {:?}", timeout);
                break Err(io::Error::new(io::ErrorKind::TimedOut, s));
            }
            Fin { .. } => {
                let pkt = match pr.parse() {
                    Ok(pkt) => pkt,
                    Err(err) => {
                        Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?
                    }
                };
                pr = pr.reset();
                break Ok(pkt);
            }
            MQTTRead::None => unreachable!(),
        };
    };

    let _none = mem::replace(pktr, pr);
    res
}

fn write_packet<W>(
    client: &Client,
    sock: &mut W,
    pktw: &mut MQTTWrite,
    pkt: v5::Packet,
) -> io::Result<()>
where
    W: io::Write,
{
    use crate::MQTTWrite::{Fin, Init, Remain};

    let mut timeout = RwTimeout::default();
    timeout.set_write_timeout(client.write_timeout);

    let mut pw = mem::replace(pktw, MQTTWrite::default());

    let blob = pkt
        .encode()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    pw = pw.reset(blob.as_ref());

    pw = loop {
        let (val, _would_block) = pw
            .write(sock)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))?;
        pw = val;

        match &pw {
            Init { .. } | Remain { .. } if timeout.write_elapsed() => {
                trace!("write retrying");
            }
            Init { .. } | Remain { .. } => {
                let s = format!("packet write fail after {:?}", timeout);
                break Err(io::Error::new(io::ErrorKind::TimedOut, s));
            }
            Fin { .. } => break Ok(pw),
            MQTTWrite::None => unreachable!(),
        };
    }?;

    let _pw_none = mem::replace(pktw, pw);
    Ok(())
}

#[derive(Default, Debug)]
struct RwTimeout {
    deadline: Option<time::Instant>,
}

impl RwTimeout {
    fn read_elapsed(&self) -> bool {
        match &self.deadline {
            Some(deadline) if &time::Instant::now() > deadline => true,
            Some(_) | None => false,
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<time::Duration>) {
        if let Some(timeout) = timeout {
            let now = time::Instant::now();
            self.deadline = Some(now.checked_add(timeout).unwrap());
        }
    }

    fn write_elapsed(&self) -> bool {
        match &self.deadline {
            Some(deadline) if &time::Instant::now() > deadline => true,
            Some(_) | None => false,
        }
    }

    fn set_write_timeout(&mut self, timeout: Option<time::Duration>) {
        if let Some(timeout) = timeout {
            self.deadline = Some(time::Instant::now() + timeout);
        }
    }
}
