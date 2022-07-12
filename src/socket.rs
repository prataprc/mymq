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
    pub fn try_sends(&mut self, prefix: &str, pkts: Vec<v5::Packet>) -> QueuePkt {
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

pub struct PktRx {
    pkt_batch_size: usize,
    rx: mpsc::Receiver<v5::Packet>,
}

impl PktRx {
    pub fn try_recvs(&self, prefix: &str) -> QueueStatus<v5::Packet> {
        let mut pkts = Vec::with_capacity(self.pkt_batch_size);
        loop {
            match self.rx.try_recv() {
                Ok(pkt) if pkts.len() < self.pkt_batch_size => pkts.push(pkt),
                Ok(pkt) => {
                    pkts.push(pkt);
                    break QueueStatus::Ok(pkts);
                }
                Err(mpsc::TryRecvError::Empty) => break QueueStatus::Block(pkts),
                Err(mpsc::TryRecvError::Disconnected) => {
                    warn!("{} senders disconnected ...", prefix);
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
    // All incoming MQTT packets on this socket first land here.
    pub packets: VecDeque<v5::Packet>,
}

pub struct Sink {
    pub pw: MQTTWrite,
    pub timeout: Option<time::Instant>,
    pub miot_rx: PktRx,
    // All out-going MQTT packets on this socket first land here.
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
        let pkt_batch_size = config.mqtt_pkt_batch_size() as usize;

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
        use crate::packet::MQTTRead::{Fin, Header, Init, Remain};

        let disconnected = QueuePkt::Disconnected(Vec::new());

        let timeout = config.mqtt_read_timeout();
        let pr = mem::replace(&mut self.rd.pr, MQTTRead::default());
        let mut pr = match pr.read(&self.conn) {
            Ok((pr, _would_block)) => pr,
            Err(err) if err.kind() == ErrorKind::Disconnected => return Ok(disconnected),
            Err(err) => return Err(err),
        };

        let status = match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !self.read_elapsed() => {
                trace!("{} read retrying", prefix);
                self.set_read_timeout(true, timeout);
                QueueStatus::Block(Vec::new())
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                self.set_read_timeout(false, timeout);
                error!("{} disconnect, pkt-read timesout {:?}", prefix, self.wt.timeout);
                QueueStatus::Disconnected(Vec::new())
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
        Ok(status)
    }

    // QueueStatus shall not carry any packets
    pub fn send_upstream(&mut self, prefix: &str) -> QueueStatus<v5::Packet> {
        let mut session_tx = self.rd.session_tx.clone(); // shard woken when dropped

        let pkts = self.rd.packets.drain(..).collect();

        let mut status = session_tx.try_sends(prefix, pkts);

        self.rd.packets = status.take_values().into(); // left over packets

        status
    }
}

impl Socket {
    pub fn write_packets(&mut self, prefix: &str, config: &Config) -> QueuePkt {
        // before reading from socket, send remaining packets to connection.
        loop {
            match self.flush_packets(prefix, config) {
                QueueStatus::Ok(_) => (),
                status @ QueueStatus::Block(_) => break status,
                status @ QueueStatus::Disconnected(_) => break status,
            }

            let mut status = self.wt.miot_rx.try_recvs(prefix);
            self.wt.packets.extend(status.take_values().into_iter());

            match status {
                QueueStatus::Ok(_) => (),
                QueueStatus::Block(_) => break self.flush_packets(prefix, config),
                status @ QueueStatus::Disconnected(_) => break status,
            }
        }
    }

    // QueueStatus shall not carry any packets
    pub fn flush_packets(&mut self, prefix: &str, config: &Config) -> QueuePkt {
        let mut pw = mem::replace(&mut self.wt.pw, MQTTWrite::default());
        let mut iter = {
            let packets = self.wt.packets.drain(..).collect::<Vec<v5::Packet>>();
            packets.into_iter()
        };
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
                break QueueStatus::Ok(Vec::new());
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
                return QueueStatus::Disconnected(Vec::new());
            }
            Err(err) => unreachable!("unexpected error: {}", err),
        };

        let res = match &pw {
            Init { .. } | Remain { .. } if !self.write_elapsed() => {
                trace!("{} write retrying", prefix);
                self.set_write_timeout(true, timeout);
                QueueStatus::Block(Vec::new())
            }
            Init { .. } | Remain { .. } => {
                self.set_write_timeout(false, timeout);
                error!("{} packet write fail after {:?}", prefix, self.wt.timeout);
                QueueStatus::Disconnected(Vec::new())
            }
            Fin { .. } => {
                self.set_write_timeout(false, timeout);
                QueueStatus::Ok(Vec::new())
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
    let pkt_rx = PktRx { pkt_batch_size: size, rx };

    (pkt_tx, pkt_rx)
}
