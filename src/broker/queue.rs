use log::error;

use std::{collections::VecDeque, mem, net, time};

use crate::broker::Config;
use crate::Result;
use crate::{Blob, ClientID, QueueStatus};
use crate::{PacketRx, PacketTx, Packetize, QPacket, Socket};

type QueuePkt = QueueStatus<QPacket>;

pub struct PQueue {
    config: Config,
    socket: Socket,
    session_tx: PacketTx, // Outbound channel to session thread
    miot_rx: PacketRx,    // Inbound channel from session thread
    // All incoming MQTT packets on this socket first land here.
    inc_packets: VecDeque<QPacket>,
    // All out-going MQTT packets on this socket first land here.
    oug_packets: VecDeque<QPacket>,
}

pub struct PQueueArgs {
    pub config: Config,
    pub socket: Socket,
    pub session_tx: PacketTx,
    pub miot_rx: PacketRx,
}

impl PQueue {
    pub fn new(args: PQueueArgs) -> PQueue {
        PQueue {
            config: args.config,
            socket: args.socket,
            session_tx: args.session_tx,
            miot_rx: args.miot_rx,
            inc_packets: VecDeque::default(),
            oug_packets: VecDeque::default(),
        }
    }

    #[inline]
    pub fn peer_addr(&self) -> net::SocketAddr {
        self.socket.peer_addr()
    }

    #[inline]
    pub fn to_client_id(&self) -> ClientID {
        self.socket.to_client_id()
    }

    #[inline]
    pub fn as_mut_socket(&mut self) -> &mut Socket {
        &mut self.socket
    }
}

impl PQueue {
    // returned QueueStatus shall not carry any packets, packets are booked in Socket
    // Error shall be MalformedPacket or ProtocolError.
    pub fn read_packets(&mut self, prefix: &str) -> Result<QueuePkt> {
        let batch_size = self.config.pkt_batch_size as usize;

        // before reading from socket, send remaining packets to shard.
        match self.send_upstream(prefix) {
            QueueStatus::Ok(_) => {
                let status = loop {
                    let mut status = self.socket.read_packet(prefix)?;
                    self.inc_packets.extend(status.take_values().into_iter());
                    match status {
                        QueueStatus::Ok(_) if self.inc_packets.len() < batch_size => (),
                        status => break status,
                    }
                };

                match status {
                    s @ QueueStatus::Disconnected(_) if self.inc_packets.is_empty() => {
                        Ok(s.replace(Vec::new()))
                    }
                    _status => Ok(self.send_upstream(prefix)),
                }
            }
            status => Ok(status),
        }
    }

    // QueueStatus shall not carry any packets
    fn send_upstream(&mut self, prefix: &str) -> QueueStatus<QPacket> {
        let session_tx = self.session_tx.clone(); // shard woken when dropped
        let mut status = {
            let pkts = mem::replace(&mut self.inc_packets, VecDeque::default());
            session_tx.try_sends(prefix, pkts.into())
        };
        self.inc_packets = status.take_values().into(); // left over packets
        status
    }
}

impl PQueue {
    // Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    pub fn write_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        // before reading from socket, send remaining packets to connection.
        let (mut items, mut bytes) = (0_usize, 0_usize);
        let deadline = {
            let dur = time::Duration::from_secs(u64::from(self.config.flush_timeout));
            time::Instant::now() + dur
        };

        loop {
            if time::Instant::now() > deadline {
                break (QueueStatus::Block(Vec::new()), items, bytes);
            }

            match self.flush_packets(prefix) {
                (QueueStatus::Ok(_), a, b) => {
                    items += a;
                    bytes += b;
                }
                (status @ QueueStatus::Disconnected(_), a, b) => {
                    items += a;
                    bytes += b;
                    break (status, items, bytes);
                }
                (QueueStatus::Block(_), _a, _b) => unreachable!(),
            }

            let mut status = self.miot_rx.try_recvs(prefix);
            self.oug_packets.extend(status.take_values().into_iter());

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
        let mut iter = {
            let val = mem::replace(&mut self.oug_packets, VecDeque::default());
            val.into_iter()
        };

        let (mut items, mut bytes) = (0_usize, 0_usize);
        let mut blob: Option<Blob> = None;

        let status = loop {
            match self.socket.write_packet(prefix, blob.take()) {
                QueueStatus::Ok(_) => match iter.next() {
                    Some(packet) => match packet.encode() {
                        Ok(blob0) => {
                            items += 1;
                            bytes += blob0.as_ref().len();
                            blob = Some(blob0);
                        }
                        Err(err) => {
                            error!("{} packet:{} skipping err:{}", prefix, packet, err);
                        }
                    },
                    None => break QueueStatus::Ok(Vec::new()),
                },
                status @ QueueStatus::Disconnected(_) => break status,
                QueueStatus::Block(_) => unreachable!(),
            }
        };

        self.oug_packets.extend(iter);

        (status, items, bytes)
    }
}
