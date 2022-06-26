use log::{error, info};

use std::{net, time};

use crate::thread::{Rx, Threadable};
use crate::{v5, Cluster, Packetize};
use crate::{ErrorKind, ReasonCode};

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
        use crate::packet::{PacketRead, PacketWrite};
        use crate::{MAX_CONNECT_TIMEOUT, MAX_PACKET_SIZE, MAX_SOCKET_RETRY};
        use std::{mem, thread};

        info!("{} new connection", self.prefix);

        let mut packetr = PacketRead::new(MAX_PACKET_SIZE);
        let mut packetw = PacketWrite::new(&[], MAX_PACKET_SIZE);
        let (conn, addr) = (self.conn.take().unwrap(), self.addr);
        let dur = MAX_CONNECT_TIMEOUT / u64::try_from(MAX_SOCKET_RETRY).unwrap();
        let (mut retries, prefix) = (0, self.prefix.clone());

        let (pkt_connect, pkt_disconnect) = loop {
            packetr = match packetr.read(&conn) {
                Ok((pr, true, _)) if retries < MAX_SOCKET_RETRY => {
                    retries += 1;
                    pr
                }
                Ok((_pr, true, _)) => {
                    error!("{}, fail after {} retries", prefix, retries);
                    break (None, None);
                }
                Ok((pr, false, _)) => match pr.parse() {
                    Ok(v5::Packet::Connect(pkt_connect)) => {
                        break (Some(pkt_connect), None)
                    }
                    Ok(pkt) => {
                        error!(
                            "{}, unexpect {:?} on new connection",
                            prefix,
                            pkt.to_packet_type()
                        );
                        let code = ReasonCode::ProtocolError;
                        break (None, Some(v5::Disconnect::from_reason_code(code)));
                    }
                    Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                        error!("{}, fail parse, error {}", prefix, err);
                        break (None, Some(v5::Disconnect::from_reason_code(err.code())));
                    }
                    Err(err) if err.kind() == ErrorKind::ProtocolError => {
                        error!("{}, fail parse, error {}", prefix, err);
                        break (None, Some(v5::Disconnect::from_reason_code(err.code())));
                    }
                    Err(_err) => unreachable!(),
                },
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, error {}", prefix, err);
                    break (None, Some(v5::Disconnect::from_reason_code(err.code())));
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, error {}", prefix, err);
                    break (None, Some(v5::Disconnect::from_reason_code(err.code())));
                }
                Err(_err) => unreachable!(),
            };
            thread::sleep(time::Duration::from_millis(dur));
        };

        match pkt_disconnect {
            Some(dc) => {
                packetw = packetw.reset(dc.encode().unwrap().as_ref());
                retries = 0;
                loop {
                    let (val, retry, would_block) = match packetw.write(&conn) {
                        Ok((val, retry, would_block)) => (val, retry, would_block),
                        Err(err) => {
                            error!("{} writing disconnect {}", prefix, err);
                            break;
                        }
                    };
                    packetw = val;

                    if would_block {
                        thread::sleep(time::Duration::from_millis(dur));
                    } else if retry && retries < MAX_SOCKET_RETRY {
                        retries += 1;
                    } else if retry {
                        error!("{} writing disconnect failed after retries", prefix);
                        break;
                    } else {
                        info!("{} DISCONNECT Sent closing the socket", prefix);
                        mem::drop(conn);
                        break;
                    }
                }
            }
            None => match pkt_connect {
                Some(pkt_connect) => ignore_error!(
                    self.prefix,
                    "cluster.add_connection",
                    self.cluster.add_connection(conn, addr, pkt_connect)
                ),
                None => (),
            },
        };

        self
    }
}
