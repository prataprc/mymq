use log::{error, info};

use std::{net, thread, time};

use crate::packet::{send_connack, MQTTRead};
use crate::thread::{Rx, Threadable};
use crate::{v5, Cluster, Config, SLEEP_10MS};
use crate::{Error, ErrorKind, ReasonCode};

pub struct Handshake {
    pub prefix: String,
    pub conn: Option<mio::net::TcpStream>,
    pub addr: net::SocketAddr,
    pub config: Config,
    pub cluster: Cluster,
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        use crate::cluster::AddConnectionArgs;

        let now = time::Instant::now();
        info!("{} new connection {:?} at {:?}", self.prefix, self.addr, now);

        let max_size = self.config.mqtt_max_packet_size();
        let connect_timeout = self.config.connect_timeout();

        let mut packetr = MQTTRead::new(max_size);
        let (conn, addr) = (self.conn.take().unwrap(), self.addr);
        let timeout = now + time::Duration::from_secs(connect_timeout as u64);
        let prefix = self.prefix.clone();

        let (code, connack, pkt_connect) = loop {
            packetr = match packetr.read(&conn) {
                Ok((val, _would_block)) => val,
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, error {}", prefix, err);
                    break (err.code(), true, None);
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, error {}", prefix, err);
                    break (err.code(), true, None);
                }
                Err(err) => unreachable!("unexpected error {}", err),
            };
            match &packetr {
                MQTTRead::Init { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                MQTTRead::Header { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                MQTTRead::Remain { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                MQTTRead::Fin { .. } => match packetr.parse() {
                    Ok(v5::Packet::Connect(val)) => {
                        break (ReasonCode::Success, false, Some(val))
                    }
                    Ok(pkt) => {
                        let pt = pkt.to_packet_type();
                        error!("{}, unexpect {:?} on new connection", prefix, pt);
                        break (ReasonCode::ProtocolError, true, None);
                    }
                    Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                        error!("{}, fail parse, error {}", prefix, err);
                        break (err.code(), true, None);
                    }
                    Err(err) if err.kind() == ErrorKind::ProtocolError => {
                        error!("{}, fail parse, error {}", prefix, err);
                        break (err.code(), true, None);
                    }
                    Err(err) => unreachable!("unexpected error {}", err),
                },
                _ => {
                    error!("{}, fail after {:?}", prefix, time::Instant::now());
                    break (ReasonCode::UnspecifiedError, true, None);
                }
            };
        };

        if connack {
            // if error, connect-ack shall be sent right here and ignored.
            let code = v5::ConnackReasonCode::try_from(code as u8).unwrap();
            send_connack(&prefix, code, &conn, timeout, max_size).ok();
        } else if let Some(pkt_connect) = pkt_connect {
            let args = AddConnectionArgs { conn, addr, pkt: pkt_connect };
            err!(
                IPCFail,
                try: self.cluster.add_connection(args),
                "cluster.add_connection"
            )
            .ok();
        } else {
            unreachable!()
        }

        self
    }
}
