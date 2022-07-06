use log::{error, trace, warn};

use std::sync::{mpsc, Arc};
use std::{collections::VecDeque, mem, net, time};

use crate::packet::{MQTTRead, MQTTWrite};
use crate::{v5, ClientID, Config, Packetize, QueueStatus};
use crate::{ErrorKind, Result};

pub type QueuePkt = QueueStatus<v5::Packet>;

#[derive(Clone)]
pub struct PktTx {
    miot_id: u32, // packet queue for shard/miot is same for both.
    tx: mpsc::SyncSender<v5::Packet>, // shard/miot incoming packet queue.
    waker: Arc<mio::Waker>, // shard/miot waker
    count: usize,
}

impl Drop for PktTx {
    fn drop(&mut self) {
        if self.count > 0 {
            match self.waker.wake() {
                Ok(()) => (),
                Err(err) => {
                    error!("shard/miot-{} waking the miot: {}", self.miot_id, err)
                }
            }
        }
    }
}

impl PktTx {
    pub fn try_sends(&mut self, pkts: Vec<v5::Packet>) -> QueueStatus<v5::Packet> {
        let mut iter = pkts.into_iter();
        loop {
            match iter.next() {
                Some(pkt) => match self.tx.try_send(pkt) {
                    Ok(()) => self.count += 1,
                    Err(mpsc::TrySendError::Full(pkt)) => {
                        let mut pkts: Vec<v5::Packet> = Vec::from_iter(iter);
                        pkts.insert(0, pkt);
                        break QueueStatus::Block(pkts);
                    }
                    Err(mpsc::TrySendError::Disconnected(pkt)) => {
                        warn!("shard-{} shard disconnected ...", self.miot_id);
                        let mut pkts: Vec<v5::Packet> = Vec::from_iter(iter);
                        pkts.insert(0, pkt);
                        break QueueStatus::Disconnected(pkts);
                    }
                },
                None => break QueueStatus::Ok(vec![]),
            }
        }
    }
}

pub struct PktRx {
    miot_id: u32, // packet queue for shard/miot is same for both.
    msg_batch_size: usize,
    rx: mpsc::Receiver<v5::Packet>,
}

impl PktRx {
    pub fn try_recvs(&self) -> QueueStatus<v5::Packet> {
        let mut pkts = vec![];
        loop {
            match self.rx.try_recv() {
                Ok(pkt) if pkts.len() < self.msg_batch_size => pkts.push(pkt),
                Ok(pkt) => {
                    pkts.push(pkt);
                    break QueueStatus::Ok(pkts);
                }
                Err(mpsc::TryRecvError::Empty) => break QueueStatus::Block(pkts),
                Err(mpsc::TryRecvError::Disconnected) => {
                    warn!("shard-{} shard disconnected ...", self.miot_id);
                    break QueueStatus::Disconnected(pkts);
                }
            }
        }
    }
}

pub struct Socket {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub token: mio::Token,
    pub rd: Source,
    pub wt: Sink,
}

pub struct Source {
    pub pr: MQTTRead,
    pub timeout: Option<time::Instant>,
    pub session_tx: PktTx,
    pub packets: VecDeque<v5::Packet>,
}

pub struct Sink {
    pub pw: MQTTWrite,
    pub timeout: Option<time::Instant>,
    pub miot_rx: PktRx,
    pub packets: VecDeque<v5::Packet>,
}

impl Socket {
    pub fn read_elapsed(&self) -> bool {
        match &self.rd.timeout {
            Some(timeout) if timeout > &time::Instant::now() => true,
            Some(_) | None => false,
        }
    }

    pub fn write_elapsed(&self) -> bool {
        match &self.wt.timeout {
            Some(timeout) if timeout > &time::Instant::now() => true,
            Some(_) | None => false,
        }
    }

    pub fn set_read_timeout(&mut self, retry: bool, timeout: u32) {
        if retry && self.rd.timeout.is_none() {
            self.rd.timeout =
                Some(time::Instant::now() + time::Duration::from_secs(timeout as u64));
        } else if retry == false {
            self.rd.timeout = None;
        }
    }

    pub fn set_write_timeout(&mut self, retry: bool, timeout: u32) {
        if retry && self.wt.timeout.is_none() {
            self.wt.timeout =
                Some(time::Instant::now() + time::Duration::from_secs(timeout as u64));
        } else if retry == false {
            self.wt.timeout = None;
        }
    }
}

impl Socket {
    // returned QueueStatus shall not carry any packets
    // MalformedPacket, ProtocolError
    pub fn read_packets(&mut self, prefix: &str, config: &Config) -> Result<QueuePkt> {
        let msg_batch_size = config.mqtt_msg_batch_size() as usize;

        // before reading from socket, send remaining packets to shard.
        loop {
            match self.send_upstream(prefix) {
                QueueStatus::Ok(_) => (),
                QueueStatus::Block(_) => break,
                res @ QueueStatus::Disconnected(_) => return Ok(res),
            }

            match self.read_packet(prefix, config)? {
                QueueStatus::Ok(pkts) if self.rd.packets.len() < msg_batch_size => {
                    self.rd.packets.extend(pkts.into_iter());
                }
                QueueStatus::Block(pkts) => {
                    self.rd.packets.extend(pkts.into_iter());
                    break;
                }
                QueueStatus::Disconnected(pkts) => {
                    self.rd.packets.extend(pkts.into_iter());
                    return Ok(QueueStatus::Disconnected(vec![]));
                }
            }
        }

        Ok(self.send_upstream(prefix))
    }

    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    fn read_packet(&mut self, prefix: &str, config: &Config) -> Result<QueuePkt> {
        use crate::packet::MQTTRead::{Fin, Header, Init, Remain};

        let timeout = config.mqtt_read_timeout();
        let pr = mem::replace(&mut self.rd.pr, MQTTRead::default());
        let mut pr = match pr.read(&self.conn) {
            Ok((pr, _would_block)) => pr,
            Err(err) if err.kind() == ErrorKind::Disconnected => {
                return Ok(QueueStatus::Disconnected(vec![]));
            }
            Err(err) => {
                return Err(err);
            }
        };

        let res = match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !self.read_elapsed() => {
                trace!("{} read retrying", prefix);
                self.set_read_timeout(true, timeout);
                QueueStatus::Block(vec![])
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                self.set_read_timeout(false, timeout);
                error!("{} packet read fail after {:?}", prefix, self.wt.timeout);
                QueueStatus::Disconnected(vec![])
            }
            Fin { .. } => {
                self.set_read_timeout(false, timeout);
                let pkt = pr.parse()?;
                pr = pr.reset();
                QueueStatus::Ok(vec![pkt])
            }
            MQTTRead::None => unreachable!(),
        };

        let _pr_none = mem::replace(&mut self.rd.pr, pr);
        Ok(res)
    }

    // QueueStatus shall not carry any packets
    pub fn send_upstream(&mut self, prefix: &str) -> QueuePkt {
        let session_tx = &self.rd.session_tx.clone(); // shard woken when dropped
        match session_tx.try_sends(self.rd.packets.drain(..).collect()) {
            res @ QueueStatus::Ok(_pkts) => res,
            QueueStatus::Block(pkts) => {
                self.rd.packets = pkts.into();
                QueueStatus::Block(vec![])
            }
            QueueStatus::Disconnected(pkts) => {
                self.rd.packets = pkts.into();
                QueueStatus::Disconnected(vec![])
            }
        }
    }
}

impl Socket {
    pub fn write_packets(&mut self, prefix: &str, config: &Config) -> QueuePkt {
        // before reading from socket, send remaining packets to connection.
        loop {
            match self.flush_packets(prefix, config) {
                QueueStatus::Ok(_) => (),
                QueueStatus::Block(_) => break,
                res @ QueueStatus::Disconnected(_) => return res,
            }

            match self.wt.miot_rx.try_recvs() {
                QueueStatus::Ok(pkts) => {
                    self.wt.packets.extend(pkts.into_iter());
                }
                QueueStatus::Block(pkts) => {
                    self.wt.packets.extend(pkts.into_iter());
                    break;
                }
                QueueStatus::Disconnected(pkts) if pkts.len() == 0 => {
                    return QueueStatus::Disconnected(pkts)
                }
                QueueStatus::Disconnected(pkts) => {
                    self.wt.packets.extend(pkts.into_iter());
                    break;
                }
            }
        }

        self.flush_packets(prefix, config)
    }

    // QueueStatus shall not carry any packets
    pub fn flush_packets(&mut self, prefix: &str, config: &Config) -> QueuePkt {
        let mut pw = mem::replace(&mut self.wt.pw, MQTTWrite::default());
        let mut iter = self.wt.packets.drain(..);
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
                        error!("{} skipping packet {:?} : {}", prefix, pt, err);
                        continue;
                    }
                };
                pw = pw.reset(blob.as_ref());
            } else {
                break QueueStatus::Ok(vec![]);
            }
        };

        self.wt.packets.extend(iter);
        let _pw_none = mem::replace(&mut self.wt.pw, pw);

        res
    }

    // QueueStatus shall not carry any packets
    fn write_packet(&mut self, prefix: &str, config: &Config) -> QueuePkt {
        use crate::packet::MQTTWrite::{Fin, Init, Remain};

        let timeout = config.mqtt_write_timeout();
        let pw = mem::replace(&mut self.wt.pw, MQTTWrite::default());
        let pw = match pw.write(&self.conn) {
            Ok((pw, _would_block)) => pw,
            Err(err) if err.kind() == ErrorKind::Disconnected => {
                return QueueStatus::Disconnected(vec![]);
            }
            Err(err) => unreachable!(),
        };

        let res = match &pw {
            Init { .. } | Remain { .. } if !self.write_elapsed() => {
                trace!("{} write retrying", prefix);
                self.set_write_timeout(true, timeout);
                QueueStatus::Block(vec![])
            }
            Init { .. } | Remain { .. } => {
                self.set_write_timeout(false, timeout);
                error!("{} packet write fail after {:?}", prefix, self.wt.timeout);
                QueueStatus::Disconnected(vec![])
            }
            Fin { .. } => {
                self.set_write_timeout(false, timeout);
                QueueStatus::Ok(vec![])
            }
            MQTTWrite::None => unreachable!(),
        };

        let _pw_none = mem::replace(&mut self.wt.pw, pw);
        res
    }
}

pub fn pkt_channel(miot_id: u32, size: usize, waker: Arc<mio::Waker>) -> (PktTx, PktRx) {
    let (tx, rx) = mpsc::sync_channel(size);
    let pkt_tx = PktTx { miot_id, tx, waker, count: usize::default() };
    let pkt_rx = PktRx { miot_id, msg_batch_size: size, rx };

    (pkt_tx, pkt_rx)
}
