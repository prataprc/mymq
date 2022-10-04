use log::{error, trace};

use std::{collections::VecDeque, io, mem, net, time};

use crate::v5::{self, Config};
use crate::{ClientID, PacketRx, PacketTx, Packetize, QPacket, QueueStatus};
use crate::{ErrorKind, Result};

pub type QueuePkt = QueueStatus<QPacket>;

/// Type implement Protocol bridge between MQTT-v5 and broker.
pub struct Protocol {
    config: Config,
}

impl Protocol {
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

struct Source {
    pr: v5::MQTTRead,
    timeout: time::Duration,
    deadline: Option<time::SystemTime>,
    session_tx: PacketTx,
    // All incoming MQTT packets on this socket first land here.
    packets: VecDeque<v5::Packet>,
}

struct Sink {
    pw: v5::MQTTWrite,
    timeout: time::Duration,
    deadline: Option<time::SystemTime>,
    miot_rx: PacketRx,
    // All out-going MQTT packets on this socket first land here.
    packets: VecDeque<v5::Packet>,
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
}

impl Socket {
    // TODO
    //let rd = socket::Source {
    //    pr: v5::MQTTRead::new(max_packet_size),
    //    timeout: time::Duration::default(),
    //    deadline: None,
    //    session_tx,
    //    packets: VecDeque::default(),
    //};
    //let wt = socket::Sink {
    //    pw: v5::MQTTWrite::new(&[], args.max_packet_size),
    //    timeout: time::Duration::default(),
    //    deadline: None,
    //    miot_rx,
    //    packets: VecDeque::default(),
    //};

    // returned QueueStatus shall not carry any packets, packets are booked in Socket
    // Error shall be MalformedPacket or ProtocolError.
    pub fn read_packets(&mut self, prefix: &str) -> Result<QueuePkt> {
        let pkt_batch_size = self.config.mqtt_pkt_batch_size as usize;

        // before reading from socket, send remaining packets to shard.
        loop {
            match self.send_upstream(prefix) {
                QueueStatus::Ok(_) => (),
                status @ QueueStatus::Block(_) => break Ok(status),
                status @ QueueStatus::Disconnected(_) => break Ok(status),
            }

            let mut status = self.read_packet(prefix)?;
            self.rd.packets.extend(status.take_values().into_iter());

            match status {
                QueueStatus::Ok(_) if self.rd.packets.len() < pkt_batch_size => (),
                QueueStatus::Ok(_) => break Ok(self.send_upstream(prefix)),
                QueueStatus::Block(_) => break Ok(self.send_upstream(prefix)),
                status @ QueueStatus::Disconnected(_) if self.rd.packets.len() == 0 => {
                    break Ok(status.replace(Vec::new()));
                }
                QueueStatus::Disconnected(_) => break Ok(self.send_upstream(prefix)),
            };
        }
    }

    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    fn read_packet(&mut self, prefix: &str) -> Result<QueueStatus<v5::Packet>> {
        use crate::v5::MQTTRead::{Fin, Header, Init, Remain};

        let disconnected = QueueStatus::<v5::Packet>::Disconnected(Vec::new());

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
                QueueStatus::Ok(vec![pkt])
            }
            v5::MQTTRead::None => unreachable!(),
        };

        let _none = mem::replace(&mut self.rd.pr, pr);
        Ok(status)
    }

    // QueueStatus shall not carry any packets
    fn send_upstream(&mut self, prefix: &str) -> QueueStatus<QPacket> {
        let session_tx = self.rd.session_tx.clone(); // shard woken when dropped

        let pkts: Vec<QPacket> = {
            let pkts = mem::replace(&mut self.rd.packets, VecDeque::default());
            pkts.into_iter().map(QPacket::from).collect()
        };
        let mut status = session_tx.try_sends(prefix, pkts);
        self.rd.packets =
            // left over packets
            status.take_values().into_iter().map(v5::Packet::from).collect();

        status
    }
}

impl Socket {
    // Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    pub fn write_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        // before reading from socket, send remaining packets to connection.
        let (mut items, mut bytes) = (0_usize, 0_usize);
        loop {
            match self.flush_packets(prefix) {
                (QueueStatus::Ok(_), a, b) => {
                    items += a;
                    bytes += b;
                }
                (status @ QueueStatus::Block(_), a, b) => {
                    items += a;
                    bytes += b;
                    break (status, items, bytes);
                }
                (status @ QueueStatus::Disconnected(_), a, b) => {
                    items += a;
                    bytes += b;
                    break (status, items, bytes);
                }
            }

            let mut status = self.wt.miot_rx.try_recvs(prefix);
            self.wt
                .packets
                .extend(status.take_values().into_iter().map(|p| v5::Packet::from(p)));

            match status {
                QueueStatus::Ok(_) => (),
                QueueStatus::Block(_) => {
                    let (status, a, b) = self.flush_packets(prefix);
                    items += a;
                    bytes += b;
                    break (status, items, bytes);
                }
                status @ QueueStatus::Disconnected(_) => break (status, items, bytes),
            }
        }
    }

    // QueueStatus shall not carry any packets, (queue, items, bytes)
    pub fn flush_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        use std::io::Write;

        let mut iter =
            mem::replace(&mut self.wt.packets, VecDeque::default()).into_iter();

        let (mut items, mut bytes) = (0_usize, 0_usize);

        let status = loop {
            match self.write_packet(prefix) {
                QueueStatus::Ok(_) => (),
                s @ QueueStatus::Block(_) => break s.replace(Vec::new()),
                s @ QueueStatus::Disconnected(_) => break s.replace(Vec::new()),
            }
            if let Some(packet) = iter.next() {
                let blob = match packet.encode() {
                    Ok(blob) => blob,
                    Err(err) => {
                        let pt = packet.to_packet_type();
                        error!("{} packet:{:?} skipping err:{}", prefix, pt, err);
                        continue;
                    }
                };
                bytes += blob.as_ref().len();
                match self.conn.flush() {
                    Ok(()) => {
                        let mut pw = {
                            let dflt = v5::MQTTWrite::default();
                            mem::replace(&mut self.wt.pw, dflt)
                        };
                        items += 1;
                        pw = pw.reset(blob.as_ref());
                        let _none = mem::replace(&mut self.wt.pw, pw);
                    }
                    Err(_) => break QueueStatus::Disconnected(Vec::new()),
                };
            } else {
                break QueueStatus::Ok(Vec::new());
            }
        };

        self.wt.packets.extend(iter);

        (status, items, bytes)
    }

    // QueueStatus shall not carry any packets
    fn write_packet(&mut self, prefix: &str) -> QueueStatus<v5::Packet> {
        use crate::v5::MQTTWrite::{Fin, Init, Remain};

        let pw = mem::replace(&mut self.wt.pw, v5::MQTTWrite::default());
        let (res, pw) = match pw.write(&mut self.conn) {
            Ok((pw, _would_block)) => match &pw {
                Init { .. } | Remain { .. } if !self.write_elapsed() => {
                    trace!("{} write retrying", prefix);
                    self.set_write_timeout(true, self.config.mqtt_sock_write_timeout);
                    (QueueStatus::Block(Vec::new()), pw)
                }
                Init { .. } | Remain { .. } => {
                    self.set_write_timeout(false, self.config.mqtt_sock_write_timeout);
                    error!("{} wt_timeout:{:?} disconnecting..", prefix, self.wt.timeout);
                    (QueueStatus::Disconnected(Vec::new()), pw)
                }
                Fin { .. } => {
                    self.set_write_timeout(false, self.config.mqtt_sock_write_timeout);
                    (QueueStatus::Ok(Vec::new()), pw)
                }
                v5::MQTTWrite::None => unreachable!(),
            },
            Err(err) if err.kind() == ErrorKind::Disconnected => {
                (QueueStatus::Disconnected(Vec::new()), v5::MQTTWrite::default())
            }
            Err(err) => unreachable!("unexpected error: {}", err),
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
