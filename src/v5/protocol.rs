use log::{error, info, trace};

use std::{io, mem, net, thread, time};

use crate::v5::{self, Config};
use crate::{Blob, ClientID, Packetize, QPacket, QueueStatus, SLEEP_10MS};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub type QueuePkt = QueueStatus<QPacket>;

/// Type implement Protocol bridge between MQTT-v5 and broker.
#[derive(Clone)]
pub struct Protocol {
    config: Config,
}

impl Protocol {
    pub fn new(config: toml::Value) -> Result<Protocol> {
        let config = Config::try_from(config)?;
        Ok(Protocol { config })
    }
}

impl Protocol {
    pub fn handshake(
        &self,
        prefix: &str,
        mut conn: mio::net::TcpStream,
    ) -> Result<Socket> {
        use crate::v5::ConnAckReasonCode;

        let raddr = conn.peer_addr().unwrap();
        let laddr = conn.local_addr().unwrap();

        let mut packetr = v5::MQTTRead::new(self.config.mqtt_max_packet_size);
        let timeout = {
            let now = time::Instant::now();
            now + time::Duration::from_secs(self.config.mqtt_connect_timeout as u64)
        };

        info!("{} raddr:{} laddr:{} new connection", prefix, raddr, laddr);

        loop {
            packetr = match packetr.read(&mut conn) {
                Ok((val, _would_block)) => val,
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, err:{}", prefix, err);
                    let code = ConnAckReasonCode::try_from(err.code() as u8).unwrap();
                    self.send_connack(prefix, raddr, code, &mut conn)?;
                    break Err(err);
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, err:{}", prefix, err);
                    let code = ConnAckReasonCode::try_from(err.code() as u8).unwrap();
                    self.send_connack(prefix, raddr, code, &mut conn)?;
                    break Err(err);
                }
                Err(err) => unreachable!("unexpected error {}", err),
            };
            match &packetr {
                v5::MQTTRead::Init { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                v5::MQTTRead::Header { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                v5::MQTTRead::Remain { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                v5::MQTTRead::Fin { .. } => match packetr.parse() {
                    Ok(v5::Packet::Connect(connect)) => match connect.validate() {
                        Ok(()) => break self.new_socket(conn, connect),
                        Err(err) => {
                            error!("{}, invalid connect err:{}", prefix, err);
                            let code = {
                                let code = err.code();
                                ConnAckReasonCode::try_from(code as u8).unwrap()
                            };
                            self.send_connack(prefix, raddr, code, &mut conn)?;
                            break Err(err);
                        }
                    },
                    Ok(pkt) => {
                        let pt = pkt.to_packet_type();
                        let err: Result<Socket> = err!(
                            ProtocolError,
                            code: ProtocolError,
                            "{} packet:{:?} unexpect in connection",
                            prefix,
                            pt
                        );
                        let code = {
                            let code = ReasonCode::ProtocolError;
                            ConnAckReasonCode::try_from(code as u8).unwrap()
                        };
                        self.send_connack(prefix, raddr, code, &mut conn)?;
                        break err;
                    }
                    Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                        error!("{} fail parse, err:{}", prefix, err);
                        let code = ConnAckReasonCode::try_from(err.code() as u8).unwrap();
                        self.send_connack(prefix, raddr, code, &mut conn)?;
                        break Err(err);
                    }
                    Err(err) if err.kind() == ErrorKind::ProtocolError => {
                        error!("{} fail parse, err:{}", prefix, err);
                        let code = ConnAckReasonCode::try_from(err.code() as u8).unwrap();
                        self.send_connack(prefix, raddr, code, &mut conn)?;
                        break Err(err);
                    }
                    Err(err) => unreachable!("unexpected error {}", err),
                },
                _ => {
                    let err: Result<Socket> = err!(
                        InvalidInput,
                        code: UnspecifiedError,
                        "{} timeout:{:?} fail handshake",
                        prefix,
                        time::Instant::now()
                    );
                    let code = {
                        let code = ReasonCode::UnspecifiedError;
                        ConnAckReasonCode::try_from(code as u8).unwrap()
                    };
                    self.send_connack(prefix, raddr, code, &mut conn)?;
                    break err;
                }
            };
        }
    }

    fn send_connack<W>(
        &self,
        prefix: &str,
        raddr: net::SocketAddr,
        code: v5::ConnAckReasonCode,
        conn: &mut W,
    ) -> Result<()>
    where
        W: io::Write,
    {
        let max_size = self.config.mqtt_max_packet_size;
        let timeout = {
            let now = time::Instant::now();
            let connect_timeout = self.config.mqtt_connect_timeout;
            now + time::Duration::from_secs(connect_timeout as u64)
        };

        let cack = v5::ConnAck::from_reason_code(code);
        let mut packetw = v5::MQTTWrite::new(cack.encode().unwrap().as_ref(), max_size);
        loop {
            let (val, would_block) = match packetw.write(conn) {
                Ok(args) => args,
                Err(err) => {
                    error!("{} problem writing connack packet err:{}", prefix, err);
                    break Err(err);
                }
            };
            packetw = val;

            if would_block && timeout < time::Instant::now() {
                thread::sleep(SLEEP_10MS);
            } else if would_block {
                break err!(
                    Disconnected,
                    desc: "{} failed writing connack after {:?}",
                    prefix, time::Instant::now()
                );
            } else {
                info!("{} raddr:{} connection NACK", prefix, raddr);
                break Ok(());
            }
        }
    }

    fn new_socket(&self, conn: mio::net::TcpStream, cpkt: v5::Connect) -> Result<Socket> {
        let socket = Socket {
            client_id: ClientID::from(&cpkt),
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
    pub fn to_listen_address(&self) -> net::SocketAddr {
        let port = self.config.mqtt_port;
        net::SocketAddr::new(net::IpAddr::V4(net::Ipv4Addr::new(0, 0, 0, 0)), port)
    }

    pub fn to_listen_port(&self) -> u16 {
        self.config.mqtt_port
    }

    pub fn max_packet_size(&self) -> u32 {
        self.config.mqtt_max_packet_size
    }
}

/// Type implement the socket connection for MQTT-v5 and broker.
pub struct Socket {
    client_id: ClientID,
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
        self.deregister(registry)
    }
}

impl Socket {
    pub fn peer_addr(&self) -> net::SocketAddr {
        self.conn.peer_addr().unwrap()
    }

    pub fn to_client_id(&self) -> ClientID {
        self.client_id.clone()
    }

    pub fn to_mio_token(&self) -> mio::Token {
        self.token
    }

    pub fn set_mio_token(&mut self, token: mio::Token) {
        self.token = token;
    }

    pub fn to_protocol(&self) -> Protocol {
        Protocol { config: self.config.clone() }
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
