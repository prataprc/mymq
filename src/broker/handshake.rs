use log::{error, info};

use std::{io, net, thread, time};

use crate::broker::thread::{Rx, Threadable};
use crate::broker::{Cluster, Config};

use crate::{v5, MQTTRead, Packetize, ToJson, SLEEP_10MS};
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Type handles incoming connection.
///
/// Complete the handshake by sending the appropriate CONNACK packet. This type is
/// threadable and spawned for every incoming connection, once the handshake is
/// completed, connection is handed over to the [Cluster].
pub struct Handshake {
    pub prefix: String,
    pub sock: Option<mio::net::TcpStream>,
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub cluster: Cluster,
}

impl ToJson for Handshake {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {} }}"),
            "sock_mqtt_connect_timeout",
            self.config.sock_mqtt_connect_timeout,
            "mqtt_max_packet_size",
            self.config.mqtt_max_packet_size,
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

        let mut packetr = MQTTRead::new(self.config.mqtt_max_packet_size);
        let mut sock = self.sock.take().unwrap();
        let timeout = {
            let now = time::Instant::now();
            let connect_timeout = self.config.sock_mqtt_connect_timeout;
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
            let code = v5::ConnackReasonCode::try_from(code as u8).unwrap();
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

impl Handshake {
    fn send_connack<W>(&self, code: v5::ConnackReasonCode, sock: &mut W) -> Result<()>
    where
        W: io::Write,
    {
        use crate::MQTTWrite;

        let max_size = self.config.mqtt_max_packet_size;
        let timeout = {
            let now = time::Instant::now();
            let connect_timeout = self.config.sock_mqtt_connect_timeout;
            now + time::Duration::from_secs(connect_timeout as u64)
        };

        let cack = v5::ConnAck::from_reason_code(code);
        let mut packetw = MQTTWrite::new(cack.encode().unwrap().as_ref(), max_size);
        loop {
            let (val, would_block) = match packetw.write(sock) {
                Ok(args) => args,
                Err(err) => {
                    error!("{} problem writing connack packet err:{}", self.prefix, err);
                    break Err(err);
                }
            };
            packetw = val;

            if would_block && timeout < time::Instant::now() {
                thread::sleep(SLEEP_10MS);
            } else if would_block {
                break err!(
                    Disconnected,
                    desc: "{} failed writing connack after {:?}",
                    self.prefix, time::Instant::now()
                );
            } else {
                info!("{} raddr:{} connection NACK", self.prefix, self.raddr);
                break Ok(());
            }
        }
    }
}
