use log::{error, info};

use std::{net, time};

use crate::thread::{Rx, Threadable};
use crate::{v5, Cluster};
use crate::{ErrorKind, ReasonCode as RC};

pub struct Handshake {
    pub prefix: String,
    pub conn: Option<mio::net::TcpStream>,
    pub addr: net::SocketAddr,
    pub cluster: Cluster,
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        use crate::packet::{send_connack, PacketRead};
        use crate::{MAX_CONNECT_TIMEOUT, MAX_PACKET_SIZE, MAX_SOCKET_RETRY};
        use std::thread;

        info!("{} new connection", self.prefix);

        let mut packetr = PacketRead::new(MAX_PACKET_SIZE);
        let (conn, addr) = (self.conn.take().unwrap(), self.addr);
        let dur = MAX_CONNECT_TIMEOUT / u64::try_from(MAX_SOCKET_RETRY).unwrap();
        let (mut retries, prefix) = (0, self.prefix.clone());

        let pkt_connect = loop {
            packetr = match packetr.read(&conn) {
                Ok((pr, _retry, true)) => {
                    thread::sleep(time::Duration::from_millis(dur));
                    pr
                }
                Ok((pr, true, _would_block)) if retries < MAX_SOCKET_RETRY => {
                    retries += 1;
                    pr
                }
                Ok((_pr, true, _would_block)) => {
                    error!("{}, fail after {} retries", prefix, retries);
                    break None;
                }
                Ok((pr, false, _would_block)) => match pr.parse() {
                    Ok(v5::Packet::Connect(pkt_connect)) => {
                        break Some(pkt_connect);
                    }
                    Ok(pkt) => {
                        let pt = pkt.to_packet_type();
                        error!("{}, unexpect {:?} on new connection", prefix, pt);
                        send_connack(&prefix, RC::ProtocolError, &conn).ok();
                        break None;
                    }
                    Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                        error!("{}, fail parse, error {}", prefix, err);
                        send_connack(&prefix, RC::MalformedPacket, &conn).ok();
                        break None;
                    }
                    Err(err) if err.kind() == ErrorKind::ProtocolError => {
                        error!("{}, fail parse, error {}", prefix, err);
                        send_connack(&prefix, RC::ProtocolError, &conn).ok();
                        break None;
                    }
                    Err(_err) => unreachable!(),
                },
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, error {}", prefix, err);
                    send_connack(&prefix, RC::MalformedPacket, &conn).ok();
                    break None;
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, error {}", prefix, err);
                    send_connack(&prefix, RC::ProtocolError, &conn).ok();
                    break None;
                }
                Err(_err) => unreachable!(),
            };
        };

        match pkt_connect {
            Some(pkt_connect) => {
                error!("{} cluster.add_connection", prefix);
                self.cluster.add_connection(conn, addr, pkt_connect).ok();
            }
            None => (),
        }

        self
    }
}
