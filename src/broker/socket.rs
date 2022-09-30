use log::{error, trace, warn};

use std::sync::{mpsc, Arc, Mutex};
use std::{collections::VecDeque, mem, time};

use crate::broker::{Config, QueueStatus};

use crate::{v5, ClientID, Packetize};
use crate::{ErrorKind, Result};

pub type QueuePkt = QueueStatus<v5::Packet>;

/// Type implement the tx-handle for a packet-queue.
#[derive(Clone)]
pub struct PktTx {
    miot_id: u32, // packet queue for shard/miot is same for both.
    tx: mpsc::SyncSender<v5::Packet>, // shard/miot incoming packet queue.
    waker: Arc<mio::Waker>, // shard/miot waker
}

impl Drop for PktTx {
    fn drop(&mut self) {
        match self.waker.wake() {
            Ok(()) => (),
            Err(err) => error!("waking shard/miot-{} err:{}", self.miot_id, err),
        }
    }
}

impl PktTx {
    pub fn try_sends(&self, prefix: &str, pkts: Vec<v5::Packet>) -> QueuePkt {
        let mut iter = pkts.into_iter();
        loop {
            match iter.next() {
                Some(pkt) => match self.tx.try_send(pkt) {
                    Ok(()) => (),
                    Err(mpsc::TrySendError::Full(pkt)) => {
                        let mut pkts: Vec<v5::Packet> = Vec::from_iter(iter);
                        pkts.insert(0, pkt);
                        break QueueStatus::Block(pkts);
                    }
                    Err(mpsc::TrySendError::Disconnected(pkt)) => {
                        warn!("{} receiver disconnected ...", prefix);
                        let mut pkts: Vec<v5::Packet> = Vec::from_iter(iter);
                        pkts.insert(0, pkt);
                        break QueueStatus::Disconnected(pkts);
                    }
                },
                None => break QueueStatus::Ok(Vec::new()),
            }
        }
    }
}

/// Type implement the rx-handle for a packet-queue.
pub struct PktRx {
    pkt_batch_size: usize,
    rx: Mutex<mpsc::Receiver<v5::Packet>>,
}

impl PktRx {
    pub fn try_recvs(&self, _prefix: &str) -> QueueStatus<v5::Packet> {
        let mut pkts = Vec::with_capacity(self.pkt_batch_size);
        loop {
            match self.rx.lock().unwrap().try_recv() {
                Ok(pkt) if pkts.len() < self.pkt_batch_size => pkts.push(pkt),
                Ok(pkt) => {
                    pkts.push(pkt);
                    break QueueStatus::Ok(pkts);
                }
                Err(mpsc::TryRecvError::Empty) => break QueueStatus::Block(pkts),
                Err(mpsc::TryRecvError::Disconnected) => {
                    break QueueStatus::Disconnected(pkts);
                }
            }
        }
    }
}

#[derive(Default)]
pub struct Stats {
    pub items: usize,
    pub bytes: usize,
}

impl Stats {
    pub fn update(&mut self, other: &Stats) {
        self.items = other.items;
        self.bytes = other.bytes;
    }

    pub fn to_json(&self) -> String {
        format!("{{ {:?}: {}, {:?}: {} }}", "items", self.items, "bytes", self.bytes)
    }
}

/// Type encapsulates the socket connection and associated data-structures.
pub struct Socket {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub token: mio::Token,
    pub rd: Source,
    pub wt: Sink,
}

pub struct Source {
    pub pr: v5::MQTTRead,
    pub timeout: time::Duration,
    pub deadline: Option<time::SystemTime>,
    pub session_tx: PktTx,
    // All incoming MQTT packets on this socket first land here.
    pub packets: VecDeque<v5::Packet>,
}

pub struct Sink {
    pub pw: v5::MQTTWrite,
    pub timeout: time::Duration,
    pub deadline: Option<time::SystemTime>,
    pub miot_rx: PktRx,
    // All out-going MQTT packets on this socket first land here.
    pub packets: VecDeque<v5::Packet>,
}

impl Socket {
    pub fn read_elapsed(&self) -> bool {
        let now = time::SystemTime::now();
        match &self.rd.deadline {
            Some(deadline) if &now > deadline => true,
            Some(_) | None => false,
        }
    }

    pub fn set_read_timeout(&mut self, retry: bool, timeout: Option<u32>) {
        if let Some(timeout) = timeout {
            if retry && self.rd.deadline.is_none() {
                let now = time::SystemTime::now();
                self.rd.deadline = Some(now + time::Duration::from_secs(timeout as u64));
            } else if retry == false {
                self.rd.deadline = None;
            }
        }
    }

    pub fn write_elapsed(&self) -> bool {
        let now = time::SystemTime::now();
        match &self.wt.deadline {
            Some(deadline) if &now > deadline => true,
            Some(_) | None => false,
        }
    }

    pub fn set_write_timeout(&mut self, retry: bool, timeout: Option<u32>) {
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

impl Socket {
    // returned QueueStatus shall not carry any packets, packets are booked in Socket
    // MalformedPacket, ProtocolError
    pub fn read_packets(&mut self, prefix: &str, config: &Config) -> Result<QueuePkt> {
        let pkt_batch_size = config.mqtt_pkt_batch_size as usize;

        // before reading from socket, send remaining packets to shard.
        loop {
            match self.send_upstream(prefix) {
                QueueStatus::Ok(_) => (),
                status @ QueueStatus::Block(_) => break Ok(status),
                status @ QueueStatus::Disconnected(_) => break Ok(status),
            }

            let mut status = self.read_packet(prefix, config)?;
            self.rd.packets.extend(status.take_values().into_iter());

            match status {
                QueueStatus::Ok(_) if self.rd.packets.len() < pkt_batch_size => (),
                QueueStatus::Ok(_) => break Ok(self.send_upstream(prefix)),
                QueueStatus::Block(_) => break Ok(self.send_upstream(prefix)),
                status @ QueueStatus::Disconnected(_) if self.rd.packets.len() == 0 => {
                    break Ok(status)
                }
                QueueStatus::Disconnected(_) => break Ok(self.send_upstream(prefix)),
            };
        }
    }

    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    fn read_packet(&mut self, prefix: &str, config: &Config) -> Result<QueuePkt> {
        use crate::v5::MQTTRead::{Fin, Header, Init, Remain};

        let disconnected = QueuePkt::Disconnected(Vec::new());

        let pr = mem::replace(&mut self.rd.pr, v5::MQTTRead::default());
        let mut pr = match pr.read(&mut self.conn) {
            Ok((pr, _would_block)) => pr,
            Err(err) if err.kind() == ErrorKind::Disconnected => return Ok(disconnected),
            Err(err) => return Err(err),
        };

        let status = match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !self.read_elapsed() => {
                trace!("{} read retrying", prefix);
                self.set_read_timeout(true, config.sock_mqtt_read_timeout);
                QueueStatus::Block(Vec::new())
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                error!("{} rd_timeout:{:?} disconnecting", prefix, self.rd.timeout);
                self.set_read_timeout(false, config.sock_mqtt_read_timeout);
                QueueStatus::Disconnected(Vec::new())
            }
            Fin { .. } => {
                self.set_read_timeout(false, config.sock_mqtt_read_timeout);
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
    pub fn send_upstream(&mut self, prefix: &str) -> QueueStatus<v5::Packet> {
        let session_tx = self.rd.session_tx.clone(); // shard woken when dropped

        let pkts: Vec<v5::Packet> =
            mem::replace(&mut self.rd.packets, VecDeque::default()).into();
        let mut status = session_tx.try_sends(prefix, pkts);
        self.rd.packets = status.take_values().into(); // left over packets

        status
    }
}

impl Socket {
    pub fn write_packets(&mut self, prefix: &str, config: &Config) -> (QueuePkt, Stats) {
        // before reading from socket, send remaining packets to connection.
        let mut stats = Stats::default();
        loop {
            match self.flush_packets(prefix, config) {
                (QueueStatus::Ok(_), flush_stats) => stats.update(&flush_stats),
                (status @ QueueStatus::Block(_), flush_stats) => {
                    stats.update(&flush_stats);
                    break (status, stats);
                }
                (status @ QueueStatus::Disconnected(_), flush_stats) => {
                    stats.update(&flush_stats);
                    break (status, stats);
                }
            }

            let mut status = self.wt.miot_rx.try_recvs(prefix);
            self.wt.packets.extend(status.take_values().into_iter());

            match status {
                QueueStatus::Ok(_) => (),
                QueueStatus::Block(_) => {
                    let (status, flush_stats) = self.flush_packets(prefix, config);
                    stats.update(&flush_stats);
                    break (status, stats);
                }
                status @ QueueStatus::Disconnected(_) => break (status, stats),
            }
        }
    }

    // QueueStatus shall not carry any packets
    pub fn flush_packets(&mut self, prefix: &str, config: &Config) -> (QueuePkt, Stats) {
        use std::io::Write;

        let mut iter =
            mem::replace(&mut self.wt.packets, VecDeque::default()).into_iter();

        let mut stats = Stats::default();

        let res = loop {
            match self.write_packet(prefix, config) {
                QueueStatus::Ok(_) => (),
                res @ QueueStatus::Block(_) => break res,
                res @ QueueStatus::Disconnected(_) => break res,
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
                stats.bytes += blob.as_ref().len();
                match self.conn.flush() {
                    Ok(()) => {
                        let mut pw = {
                            let dflt = v5::MQTTWrite::default();
                            mem::replace(&mut self.wt.pw, dflt)
                        };
                        stats.items += 1;
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

        (res, stats)
    }

    // QueueStatus shall not carry any packets
    fn write_packet(&mut self, prefix: &str, config: &Config) -> QueuePkt {
        use crate::v5::MQTTWrite::{Fin, Init, Remain};

        let pw = mem::replace(&mut self.wt.pw, v5::MQTTWrite::default());
        let (res, pw) = match pw.write(&mut self.conn) {
            Ok((pw, _would_block)) => match &pw {
                Init { .. } | Remain { .. } if !self.write_elapsed() => {
                    trace!("{} write retrying", prefix);
                    self.set_write_timeout(true, config.sock_mqtt_write_timeout);
                    (QueueStatus::Block(Vec::new()), pw)
                }
                Init { .. } | Remain { .. } => {
                    self.set_write_timeout(false, config.sock_mqtt_write_timeout);
                    error!("{} wt_timeout:{:?} disconnecting..", prefix, self.wt.timeout);
                    (QueueStatus::Disconnected(Vec::new()), pw)
                }
                Fin { .. } => {
                    self.set_write_timeout(false, config.sock_mqtt_write_timeout);
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

/// Create a packet-queue for shard/miot that can hold upto `size` packets.
///
/// `waker` is attached to the thread receiving this messages from the queue.
/// When PktTx is dropped, thread will be woken up using `waker`.
pub fn pkt_channel(miot_id: u32, size: usize, waker: Arc<mio::Waker>) -> (PktTx, PktRx) {
    let (tx, rx) = mpsc::sync_channel(size);
    let pkt_tx = PktTx { miot_id, tx, waker };
    let pkt_rx = PktRx { pkt_batch_size: size, rx: Mutex::new(rx) };

    (pkt_tx, pkt_rx)
}
