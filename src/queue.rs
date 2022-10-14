use log::{error, warn};

use std::mem;
use std::sync::{mpsc, Arc, Mutex};

use crate::QPacket;

/// Return type from methods used to send or receive messages/packets/commands.
///
/// This type is associated with methods that communicates with:
/// * Message-Queue, communication between shards
/// * Packet-Queue, communication between shard and socket
/// * Command-Queue, communication with thread.
#[derive(Clone)]
pub enum QueueStatus<T> {
    Ok(Vec<T>),           // holds remaining (for tx) or received (for rx) values
    Block(Vec<T>),        // holds remaining (for tx) or received (for rx) values
    Disconnected(Vec<T>), // holds remaining (for tx) or received (for rx) values
}

impl<T> QueueStatus<T> {
    pub fn take_values(&mut self) -> Vec<T> {
        let val = match self {
            QueueStatus::Ok(val) => val,
            QueueStatus::Block(val) => val,
            QueueStatus::Disconnected(val) => val,
        };
        mem::replace(val, Vec::new())
    }

    pub fn set_values(&mut self, values: Vec<T>) {
        let old_values = match self {
            QueueStatus::Ok(old_values) => old_values,
            QueueStatus::Block(old_values) => old_values,
            QueueStatus::Disconnected(old_values) => old_values,
        };
        assert!(old_values.len() == 0);
        let _empty = mem::replace(old_values, values);
    }

    pub fn replace<U>(self, values: Vec<U>) -> QueueStatus<U> {
        use QueueStatus::*;

        let (old_values, val) = match self {
            Ok(old_values) => (old_values, Ok(values)),
            Block(old_values) => (old_values, Block(values)),
            Disconnected(old_values) => (old_values, Disconnected(values)),
        };
        assert!(old_values.len() == 0);
        val
    }

    pub fn map<F, U>(self, callb: F) -> QueueStatus<U>
    where
        F: Fn(T) -> U,
    {
        match self {
            QueueStatus::Ok(vals) => {
                QueueStatus::Ok(vals.into_iter().map(callb).collect::<Vec<U>>())
            }
            QueueStatus::Block(vals) => {
                QueueStatus::Block(vals.into_iter().map(callb).collect::<Vec<U>>())
            }
            QueueStatus::Disconnected(vals) => {
                QueueStatus::Disconnected(vals.into_iter().map(callb).collect::<Vec<U>>())
            }
        }
    }

    pub fn is_disconnected(&self) -> bool {
        match self {
            QueueStatus::Disconnected(_) => true,
            _ => false,
        }
    }
}

/// Type implement the tx-handle for a packet-queue.
#[derive(Clone)]
pub struct PacketTx {
    id: u32,                       // Id of owner, owning this packet queue
    tx: mpsc::SyncSender<QPacket>, // shard/miot incoming packets to queue.
    waker: Arc<mio::Waker>,        // shard/miot waker
}

impl Drop for PacketTx {
    fn drop(&mut self) {
        match self.waker.wake() {
            Ok(()) => (),
            Err(err) => error!("waking shard/miot-{} err:{}", self.id, err),
        }
    }
}

impl PacketTx {
    pub fn try_sends(&self, prefix: &str, pkts: Vec<QPacket>) -> QueueStatus<QPacket> {
        let mut iter = pkts.into_iter();
        loop {
            match iter.next() {
                Some(pkt) => match self.tx.try_send(pkt) {
                    Ok(()) => (),
                    Err(mpsc::TrySendError::Full(pkt)) => {
                        let mut pkts: Vec<QPacket> = vec![pkt];
                        pkts.extend(iter);
                        break QueueStatus::Block(pkts);
                    }
                    Err(mpsc::TrySendError::Disconnected(pkt)) => {
                        warn!("{} receiver disconnected ...", prefix);
                        let mut pkts: Vec<QPacket> = vec![pkt];
                        pkts.extend(iter);
                        break QueueStatus::Disconnected(pkts);
                    }
                },
                None => break QueueStatus::Ok(Vec::new()),
            }
        }
    }
}

/// Type implement the rx-handle for a packet-queue.
pub struct PacketRx {
    pkt_batch_size: usize,
    rx: Mutex<mpsc::Receiver<QPacket>>,
}

impl PacketRx {
    pub fn try_recvs(&self, _prefix: &str) -> QueueStatus<QPacket> {
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

/// Create a packet-queue for shard/miot that can hold upto `size` packets.
///
/// `waker` is attached to the thread receiving this messages from the queue.
/// When PacketTx is dropped, thread will be woken up using `waker`.
pub fn new_packet_queue(id: u32, sz: usize, w: Arc<mio::Waker>) -> (PacketTx, PacketRx) {
    let (tx, rx) = mpsc::sync_channel(sz);
    let pkt_tx = PacketTx { id, tx, waker: w };
    let pkt_rx = PacketRx { pkt_batch_size: sz, rx: Mutex::new(rx) };

    (pkt_tx, pkt_rx)
}
