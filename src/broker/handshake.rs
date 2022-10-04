use log::{error, info};

use std::{io, net, thread, time};

use crate::broker::thread::{Rx, Threadable};
use crate::broker::{Cluster, Config};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{Packetize, Protocol, ToJson, SLEEP_10MS};

use crate::v5;

/// Type handles incoming connection.
///
/// Complete the handshake by sending the appropriate CONNACK packet. This type is
/// threadable and spawned for every incoming connection, once the handshake is
/// completed, connection is handed over to the [Cluster].
pub struct Handshake {
    pub prefix: String,
    pub raddr: net::SocketAddr,
    pub config: Config,

    pub proto: Protocol,
    pub cluster: Cluster,
    pub sock: Option<mio::net::TcpStream>,
}

impl ToJson for Handshake {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {:?}, {:?}: {}, {:?}: {} }}"),
            "name", self.config.name, "connect_timeout", self.config.connect_timeout,
        )
    }

    fn to_stats_json(&self) -> String {
        "{{}}".to_string()
    }
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        use crate::broker::cluster::AddConnectionArgs;

        info!("{} raddr:{} handing over to cluster ...", prefix, raddr);
        let res = err!(
            IPCFail,
            try: self.cluster.add_connection(socket),
            "cluster.add_connection"
        );
        if let Err(err) = res {
            info!("{} raddr:{} hand over failed err:{}", prefix, raddr, err);
        }

        let mut packetr = v5::MQTTRead::new(self.config.mqtt_max_packet_size);
        let mut sock = self.sock.take().unwrap();
        let timeout = {
            let now = time::Instant::now();
            let connect_timeout = self.config.connect_timeout;
            now + time::Duration::from_secs(connect_timeout as u64)
        };
        let raddr = sock.peer_addr().unwrap();

        info!("{} spawn thread config:{}", self.prefix, self.to_config_json());
        info!(
            "{} raddr:{} new connection {:?}<-{:?}",
            self.prefix,
            raddr,
            sock.local_addr().unwrap(),
            raddr,
        );

        let (code, connack, connect) = loop {
            packetr = match packetr.read(&mut sock) {
                Ok((val, _would_block)) => val,
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{}, fail read, err:{}", self.prefix, err);
                    break (err.code(), true, None);
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{}, fail read, err:{}", self.prefix, err);
                    break (err.code(), true, None);
                }
                Err(err) => unreachable!("unexpected error {}", err),
            };
            match &packetr {
                v5::MQTTRead::Init { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                v5::MQTTRead::Header { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                v5::MQTTRead::Remain { .. } if time::Instant::now() < timeout => {
                    thread::sleep(SLEEP_10MS);
                }
                v5::MQTTRead::Fin { .. } => match packetr.parse() {
                    Ok(v5::Packet::Connect(connect)) => match connect.validate() {
                        Ok(()) => break (ReasonCode::Success, false, Some(connect)),
                        Err(err) => {
                            error!("{}, invalid connect err:{}", self.prefix, err);
                            break (err.code(), true, None);
                        }
                    },
                    Ok(pkt) => {
                        let pt = pkt.to_packet_type();
                        error!("{} packet:{:?} unexpect in connection", self.prefix, pt);
                        break (ReasonCode::ProtocolError, true, None);
                    }
                    Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                        error!("{} fail parse, err:{}", self.prefix, err);
                        break (err.code(), true, None);
                    }
                    Err(err) if err.kind() == ErrorKind::ProtocolError => {
                        error!("{} fail parse, err:{}", self.prefix, err);
                        break (err.code(), true, None);
                    }
                    Err(err) => unreachable!("unexpected error {}", err),
                },
                _ => {
                    error!(
                        "{} timeout:{:?} fail handshake",
                        self.prefix,
                        time::Instant::now()
                    );
                    break (ReasonCode::UnspecifiedError, true, None);
                }
            };
        };

        if connack {
            // if error, connect-ack shall be sent right here and ignored.
            let code = v5::ConnAckReasonCode::try_from(code as u8).unwrap();
            self.send_connack(code, &mut sock).ok();
        } else if let Some(connect) = connect {
            info!("{} raddr:{} handing over to cluster ...", self.prefix, self.raddr);
            let args = AddConnectionArgs { sock, pkt: connect };
            let res = err!(
                IPCFail,
                try: self.cluster.add_connection(args),
                "cluster.add_connection"
            );
            if let Err(err) = res {
                info!(
                    "{} raddr:{} hand over failed err:{}",
                    self.prefix, self.raddr, err
                );
            }
        } else {
            unreachable!()
        }

        self
    }
}

impl Handshake {}
