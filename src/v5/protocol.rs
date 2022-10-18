use log::{error, info, trace};
use mio::net::TcpStream;

use std::{io, mem, net, thread, time};

use crate::v5::{self, Config};
use crate::{Blob, ClientID, PacketID, Packetize, QPacket, QoS, QueueStatus, SLEEP_10MS};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub type QueuePkt = QueueStatus<QPacket>;

/// Type implement Protocol bridge between MQTT-v5 and broker.
#[derive(Clone, Eq, PartialEq)]
pub struct Protocol {
    client_id: ClientID,
    shard_id: u32,
    raddr: net::SocketAddr,
    config: Config,
    connect: v5::Connect,
}

impl From<Config> for Protocol {
    fn from(config: Config) -> Protocol {
        Protocol {
            client_id: ClientID::default(),
            shard_id: u32::default(),
            raddr: "0.0.0.0:0".parse().unwrap(),
            config,
            connect: v5::Connect::default(),
        }
    }
}

impl Protocol {
    pub fn is_listen(&self) -> bool {
        self.config.mqtt_listener
    }

    pub fn to_listen_address(&self) -> net::SocketAddr {
        let port = self.config.mqtt_port;
        net::SocketAddr::new(net::IpAddr::V4(net::Ipv4Addr::new(0, 0, 0, 0)), port)
    }

    pub fn to_listen_port(&self) -> u16 {
        self.config.mqtt_port
    }

    #[inline]
    pub fn maximum_qos(&self) -> QoS {
        QoS::try_from(self.config.mqtt_maximum_qos).unwrap()
    }

    #[inline]
    pub fn retain_available(&self) -> bool {
        self.config.mqtt_retain_available
    }

    #[inline]
    pub fn max_packet_size(&self) -> u32 {
        self.config.mqtt_max_packet_size
    }

    #[inline]
    pub fn keep_alive(&self) -> Option<u16> {
        self.config.keep_alive()
    }

    #[inline]
    pub fn keep_alive_factor(&self) -> f32 {
        self.config.keep_alive_factor()
    }

    #[inline]
    pub fn topic_alias_max(&self) -> Option<u16> {
        self.config.topic_alias_max()
    }
}

impl Protocol {
    pub fn handshake(&self, prefix: &str, mut conn: TcpStream) -> Result<Socket> {
        use crate::v5::MQTTRead;

        let (raddr, laddr) = (conn.peer_addr().unwrap(), conn.local_addr().unwrap());
        info!("{} raddr:{} laddr:{} new connection", prefix, raddr, laddr);

        let deadline = {
            let timeout = u64::from(self.config.mqtt_connect_timeout / 2);
            time::Instant::now() + time::Duration::from_secs(timeout)
        };

        let mut packetr = MQTTRead::new(self.config.mqtt_max_packet_size);
        loop {
            packetr = match packetr.read(&mut conn) {
                Ok((packetr, _would_block)) => packetr,
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, err:{}", prefix, err);
                    self.send_connack(prefix, err.code(), conn)?;
                    break Err(err);
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, err:{}", prefix, err);
                    self.send_connack(prefix, err.code(), conn)?;
                    break Err(err);
                }
                Err(err) => unreachable!("unexpected error {}", err),
            };

            match &packetr {
                MQTTRead::Init { .. } if time::Instant::now() < deadline => {
                    thread::sleep(SLEEP_10MS);
                    continue;
                }
                MQTTRead::Header { .. } if time::Instant::now() < deadline => {
                    thread::sleep(SLEEP_10MS);
                    continue;
                }
                MQTTRead::Remain { .. } if time::Instant::now() < deadline => {
                    thread::sleep(SLEEP_10MS);
                    continue;
                }
                MQTTRead::Init { .. }
                | MQTTRead::Header { .. }
                | MQTTRead::Remain { .. } => {
                    let code = ReasonCode::UnspecifiedError;
                    self.send_connack(prefix, code, conn)?;

                    break err!(
                        InvalidInput,
                        code: UnspecifiedError,
                        "{} deadline:{:?} fail handshake connect rx",
                        prefix,
                        deadline
                    );
                }
                MQTTRead::Fin { .. } => (),
                MQTTRead::None => unreachable!(),
            }

            match packetr.parse() {
                Ok(v5::Packet::Connect(connect)) => {
                    if let Err(err) = connect.validate() {
                        error!("{}, invalid connect-packet err:{}", prefix, err);
                        self.send_connack(prefix, err.code(), conn)?;
                        break Err(err);
                    }

                    if let Some(publ) = connect.to_will_publish() {
                        if let Err(err) = validate_wpublish(prefix, &self.config, &publ) {
                            self.send_connack(prefix, err.code(), conn)?;
                            break Err(err);
                        }
                    }

                    break self.new_socket(conn, connect);
                }
                Ok(pkt) => {
                    let code = ReasonCode::ProtocolError;
                    self.send_connack(prefix, code, conn)?;

                    break err!(
                        ProtocolError,
                        code: ProtocolError,
                        "{} packet:{} unexpected in connection",
                        prefix,
                        pkt
                    );
                }
                Err(err) => {
                    error!("{}, invalid packet parse err:{}", prefix, err);
                    self.send_connack(prefix, err.code(), conn)?;
                    break Err(err);
                }
            }
        }
    }

    fn send_connack(&self, pr: &str, rc: ReasonCode, mut conn: TcpStream) -> Result<()> {
        use crate::v5::ConnAckReasonCode;

        let raddr = conn.peer_addr().unwrap();
        let max_size = self.config.mqtt_max_packet_size;

        let deadline = {
            let timeout = u64::from(self.config.mqtt_connect_timeout / 2);
            time::Instant::now() + time::Duration::from_secs(timeout)
        };

        let mut packetw = {
            let code = ConnAckReasonCode::try_from(rc).unwrap();
            let cack = v5::ConnAck::from_reason_code(code);
            v5::MQTTWrite::new(cack.encode().unwrap().as_ref(), max_size)
        };

        loop {
            let (val, would_block) = match packetw.write(&mut conn) {
                Ok((packetw, would_block)) => (packetw, would_block),
                Err(err) => {
                    error!("{} problem writing connack packet err:{}", pr, err);
                    break Err(err);
                }
            };
            packetw = val;

            if would_block && time::Instant::now() < deadline {
                thread::sleep(SLEEP_10MS);
            } else if would_block {
                break err!(
                    Disconnected,
                    desc: "{} deadline:{:?} failed handshake connack tx",
                    pr, deadline
                );
            } else {
                info!("{} raddr:{} CONNACK", pr, raddr);
                break Ok(());
            }
        }
    }

    fn new_socket(&self, conn: mio::net::TcpStream, cpkt: v5::Connect) -> Result<Socket> {
        let socket = Socket {
            client_id: ClientID::from(&cpkt),
            shard_id: 0,
            config: self.config.clone(),
            conn,
            connect: cpkt,
            token: mio::Token(0),
            rd: Source::default(),
            wt: Sink::default(),
        };

        Ok(socket)
    }
}

impl Protocol {
    pub fn new_ping_resp(&self, _ping_req: v5::Packet) -> QPacket {
        QPacket::V5(v5::Packet::PingResp)
    }

    pub fn new_pub_ack(&self, packet_id: PacketID) -> QPacket {
        QPacket::V5(v5::Packet::PubAck(v5::Pub::new_pub_ack(packet_id)))
    }

    pub fn new_sub_ack(&self, sub: &v5::Packet, rcodes: Vec<ReasonCode>) -> QPacket {
        match sub {
            v5::Packet::Subscribe(sub) => {
                let suback = v5::SubAck::from_sub(sub, rcodes);
                QPacket::V5(v5::Packet::SubAck(suback))
            }
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn new_unsub_ack(&self, unsub: &v5::Packet, rcodes: Vec<ReasonCode>) -> QPacket {
        match unsub {
            v5::Packet::UnSubscribe(unsub) => {
                let unsuback = v5::UnsubAck::from_unsub(&unsub, rcodes);
                QPacket::V5(v5::Packet::UnsubAck(unsuback))
            }
            pkt => unreachable!("{}", pkt),
        }
    }
}

/// Type implement the socket connection for MQTT-v5 and broker.
pub struct Socket {
    client_id: ClientID,
    shard_id: u32,
    config: Config,
    conn: mio::net::TcpStream,
    connect: v5::Connect,
    token: mio::Token,
    rd: Source,
    wt: Sink,
}

#[derive(Default)]
struct Source {
    pr: v5::MQTTRead,
    timeout: time::Duration,
    deadline: Option<time::SystemTime>,
}

#[derive(Default)]
struct Sink {
    pw: v5::MQTTWrite,
    timeout: time::Duration,
    deadline: Option<time::SystemTime>,
}

impl mio::event::Source for Socket {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.conn.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.conn.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.conn.deregister(registry)
    }
}

impl Socket {
    #[inline]
    pub fn set_mio_token(&mut self, token: mio::Token) {
        self.token = token;
    }

    #[inline]
    pub fn set_shard_id(&mut self, shard_id: u32) {
        self.shard_id = shard_id;
    }
}

impl Socket {
    #[inline]
    pub fn peer_addr(&self) -> net::SocketAddr {
        self.conn.peer_addr().unwrap()
    }

    #[inline]
    pub fn as_client_id(&self) -> &ClientID {
        &self.client_id
    }

    #[inline]
    pub fn to_mio_token(&self) -> mio::Token {
        self.token
    }

    #[inline]
    pub fn to_protocol(&self) -> Protocol {
        Protocol {
            client_id: self.client_id.clone(),
            shard_id: self.shard_id,
            raddr: self.conn.peer_addr().unwrap(),
            config: self.config.clone(),
            connect: self.connect.clone(),
        }
    }

    #[inline]
    pub fn client_keep_alive(&self) -> u16 {
        self.connect.keep_alive
    }

    #[inline]
    pub fn client_receive_maximum(&self) -> u16 {
        self.connect.receive_maximum()
    }

    #[inline]
    pub fn client_session_expiry_interval(&self) -> Option<u32> {
        self.connect.session_expiry_interval()
    }

    #[inline]
    pub fn is_clean_start(&self) -> bool {
        self.connect.flags.is_clean_start()
    }
}

impl Socket {
    pub fn send_disconnect(&mut self, prefix: &str, rcode: ReasonCode) {
        let blob = {
            let disconn = v5::Disconnect { code: rcode, properties: None };
            disconn.encode().ok()
        };
        self.write_packet(prefix, blob);
    }

    pub fn new_conn_ack(&self, rcode: ReasonCode) -> QPacket {
        let val = self.connect.session_expiry_interval();
        let sei = match (self.config.mqtt_session_expiry_interval, val) {
            (Some(_one), Some(two)) => Some(two),
            (Some(one), None) => Some(one),
            (None, Some(two)) => Some(two),
            (None, None) => None,
        };

        let mut props = v5::ConnAckProperties {
            session_expiry_interval: sei,
            receive_maximum: Some(self.config.mqtt_receive_maximum),
            maximum_qos: Some(self.config.mqtt_maximum_qos.try_into().unwrap()),
            retain_available: Some(self.config.mqtt_retain_available),
            max_packet_size: Some(self.config.mqtt_max_packet_size),
            assigned_client_identifier: None,
            wildcard_subscription_available: Some(true),
            subscription_identifiers_available: Some(true),
            shared_subscription_available: None,
            topic_alias_max: self.config.topic_alias_max(),
            ..v5::ConnAckProperties::default()
        };

        if self.connect.payload.client_id.len() == 0 {
            props.assigned_client_identifier = Some((*self.client_id).clone());
        }

        if let Some(keep_alive) = self.config.keep_alive() {
            props.server_keep_alive = Some(keep_alive)
        }

        let connack = match rcode {
            ReasonCode::Success => v5::ConnAck::new_success(props),
            _ => unreachable!(),
        };

        QPacket::V5(v5::Packet::ConnAck(connack))
    }

    pub fn to_will_publish(&self) -> Option<QPacket> {
        self.connect.to_will_publish().map(|pkt| QPacket::V5(v5::Packet::Publish(pkt)))
    }

    pub fn will_delay_interval(&self) -> u32 {
        self.connect.will_delay_interval()
    }
}

impl Socket {
    // return a single packet, if fully received.
    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    pub fn read_packet(&mut self, prefix: &str) -> Result<QueuePkt> {
        use crate::v5::MQTTRead::{Fin, Header, Init, Remain};

        let disconnected = QueueStatus::<QPacket>::Disconnected(Vec::new());

        let pr = mem::replace(&mut self.rd.pr, v5::MQTTRead::default());
        let mut pr = match pr.read(&mut self.conn) {
            Ok((pr, _would_block)) => pr,
            Err(err) if err.kind() == ErrorKind::Disconnected => return Ok(disconnected),
            Err(err) => return Err(err),
        };

        let status = match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !self.read_elapsed() => {
                trace!("{} read retrying", prefix);
                self.set_read_timeout(true, self.config.mqtt_sock_read_timeout);
                QueueStatus::Block(Vec::new())
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                error!("{} rd_timeout:{:?} disconnecting", prefix, self.rd.timeout);
                self.set_read_timeout(false, self.config.mqtt_sock_read_timeout);
                QueueStatus::Disconnected(Vec::new())
            }
            Fin { .. } => {
                self.set_read_timeout(false, self.config.mqtt_sock_read_timeout);
                let pkt = pr.parse()?;
                pr = pr.reset();
                QueueStatus::Ok(vec![pkt.into()])
            }
            v5::MQTTRead::None => unreachable!(),
        };

        let _none = mem::replace(&mut self.rd.pr, pr);
        Ok(status)
    }
}

impl Socket {
    // QueueStatus shall not carry any packets
    pub fn write_packet(&mut self, prefix: &str, blob: Option<Blob>) -> QueuePkt {
        use crate::v5::MQTTWrite::{Fin, Init, Remain};
        use std::io::Write;

        let mut pw = match (blob, &self.wt.pw) {
            (Some(blob), Fin { .. }) => {
                if let Err(err) = self.conn.flush() {
                    error!("{} fail conn.flush() err:{}", prefix, err);
                    return QueueStatus::Disconnected(Vec::new());
                }

                let pw = mem::replace(&mut self.wt.pw, v5::MQTTWrite::default());
                pw.reset(blob.as_ref())
            }
            (Some(_blob), _) => unreachable!(),
            _ => mem::replace(&mut self.wt.pw, v5::MQTTWrite::default()),
        };

        let write_timeout = self.config.mqtt_sock_write_timeout;
        let timeout = self.wt.timeout;

        let (res, pw) = loop {
            pw = match pw.write(&mut self.conn) {
                Ok((pw, _would_block)) => match &pw {
                    Init { .. } | Remain { .. } if !self.write_elapsed() => {
                        trace!("{} write retrying", prefix);
                        self.set_write_timeout(true, write_timeout);
                        pw
                        // TODO: thread yield ?
                    }
                    Init { .. } | Remain { .. } => {
                        self.set_write_timeout(false, write_timeout);
                        error!("{} wt_timeout:{:?} disconnecting..", prefix, timeout);
                        break (QueueStatus::Disconnected(Vec::new()), pw);
                    }
                    Fin { .. } => {
                        self.set_write_timeout(false, write_timeout);
                        break (QueueStatus::Ok(Vec::new()), pw);
                    }
                    v5::MQTTWrite::None => unreachable!(),
                },
                Err(err) if err.kind() == ErrorKind::Disconnected => {
                    let val = v5::MQTTWrite::default();
                    break (QueueStatus::Disconnected(Vec::new()), val);
                }
                Err(err) => unreachable!("unexpected error: {}", err),
            }
        };

        let _none = mem::replace(&mut self.wt.pw, pw);
        res
    }
}

impl Socket {
    fn read_elapsed(&self) -> bool {
        let now = time::SystemTime::now();
        match &self.rd.deadline {
            Some(deadline) if &now > deadline => true,
            Some(_) | None => false,
        }
    }

    fn set_read_timeout(&mut self, retry: bool, timeout: Option<u32>) {
        if let Some(timeout) = timeout {
            if retry && self.rd.deadline.is_none() {
                let now = time::SystemTime::now();
                self.rd.deadline = Some(now + time::Duration::from_secs(timeout as u64));
            } else if retry == false {
                self.rd.deadline = None;
            }
        }
    }

    fn write_elapsed(&self) -> bool {
        let now = time::SystemTime::now();
        match &self.wt.deadline {
            Some(deadline) if &now > deadline => true,
            Some(_) | None => false,
        }
    }

    fn set_write_timeout(&mut self, retry: bool, timeout: Option<u32>) {
        if let Some(timeout) = timeout {
            if retry && self.wt.deadline.is_none() {
                let now = time::SystemTime::now();
                self.wt.deadline = Some(now + time::Duration::from_secs(timeout as u64));
            } else if retry == false {
                self.wt.deadline = None;
            }
        }
    }
}

fn validate_wpublish(prefix: &str, config: &Config, publish: &v5::Publish) -> Result<()> {
    let server_qos = QoS::try_from(config.mqtt_maximum_qos)?;

    if publish.qos > server_qos {
        err_unsup_qos(prefix, publish.qos)
    } else if publish.retain && !config.mqtt_retain_available {
        err_unsup_retain(prefix)
    } else {
        Ok(())
    }
}

fn err_unsup_qos(prefix: &str, qos: QoS) -> Result<()> {
    err!(
        ProtocolError,
        code: QoSNotSupported,
        "{} publish-qos exceeds server-qos {:?}",
        prefix,
        qos
    )
}

fn err_unsup_retain(prefix: &str) -> Result<()> {
    err!(ProtocolError, code: RetainNotSupported, "{} retain unavailable", prefix)
}
