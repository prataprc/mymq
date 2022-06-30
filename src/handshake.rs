use log::{error, info};

use std::{net, thread, time};

use crate::packet::{send_connack, PacketRead};
use crate::thread::{Rx, Threadable};
use crate::{v5, Cluster, Config, SLEEP_10MS};
use crate::{Error, ErrorKind, ReasonCode as RC};

pub struct Handshake {
    pub prefix: String,
    pub conn: Option<mio::net::TcpStream>,
    pub addr: net::SocketAddr,
    pub config: Config,
    pub cluster: Cluster,
    pub connect_timeout: u32,
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        let now = time::Instant::now();
        info!("{} new connection at {:?}", self.prefix, now);

        let max_size = self.config.mqtt_max_packet_size();
        let mut packetr = PacketRead::new(max_size);
        let (conn, addr) = (self.conn.take().unwrap(), self.addr);
        let timeout = now + time::Duration::from_secs(self.connect_timeout as u64);
        let prefix = self.prefix.clone();

        loop {
            packetr = match packetr.read(&conn) {
                Ok((val, _would_block)) => val,
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, error {}", prefix, err);
                    send_connack(&prefix, timeout, max_size, RC::MalformedPacket, &conn)
                        .ok();
                    break;
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, error {}", prefix, err);
                    send_connack(&prefix, timeout, max_size, RC::ProtocolError, &conn)
                        .ok();
                    break;
                }
                Err(_err) => unreachable!(),
            };
            match &packetr {
                PacketRead::Init { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                PacketRead::Header { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                PacketRead::Remain { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                PacketRead::Fin { .. } => match packetr.parse() {
                    Ok(v5::Packet::Connect(pkt_connect)) => {
                        err!(
                            IPCFail,
                            try: self.cluster.add_connection(conn, addr, pkt_connect),
                            "cluster.add_connection"
                        )
                        .ok();
                        break;
                    }
                    Ok(pkt) => {
                        let pt = pkt.to_packet_type();
                        error!("{}, unexpect {:?} on new connection", prefix, pt);
                        send_connack(
                            &prefix,
                            timeout,
                            max_size,
                            RC::ProtocolError,
                            &conn,
                        )
                        .ok();
                        break;
                    }
                    Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                        error!("{}, fail parse, error {}", prefix, err);
                        send_connack(
                            &prefix,
                            timeout,
                            max_size,
                            RC::MalformedPacket,
                            &conn,
                        )
                        .ok();
                        break;
                    }
                    Err(err) if err.kind() == ErrorKind::ProtocolError => {
                        error!("{}, fail parse, error {}", prefix, err);
                        send_connack(
                            &prefix,
                            timeout,
                            max_size,
                            RC::ProtocolError,
                            &conn,
                        )
                        .ok();
                        break;
                    }
                    Err(_err) => unreachable!(),
                },
                _ => {
                    error!("{}, fail after {:?}", prefix, time::Instant::now());
                    break;
                }
            };
        }

        self
    }
}
