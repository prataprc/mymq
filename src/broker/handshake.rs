use log::{error, info};

use std::{io, thread, time};

use crate::broker::thread::{Rx, Threadable};
use crate::broker::{Cluster, Config, SLEEP_10MS};

use crate::{v5, MQTTRead, Packetize};
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Type handles incoming connection.
///
/// Complete the handshake by sending the appropriate CONNACK packet. This type is
/// threadable and spawned for every incoming connection, once the handshake is
/// completed, connection is handed over to the [Cluster].
pub struct Handshake {
    pub prefix: String,
    pub conn: Option<mio::net::TcpStream>,
    pub config: Config,
    pub cluster: Cluster,
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        use crate::broker::cluster::AddConnectionArgs;

        let now = time::Instant::now();

        let max_size = self.config.mqtt_max_packet_size;
        let connect_timeout = self.config.sock_mqtt_connect_timeout;

        let mut packetr = MQTTRead::new(max_size);
        let mut conn = self.conn.take().unwrap();
        let timeout = now + time::Duration::from_secs(connect_timeout as u64);
        let prefix = self.prefix.clone();

        info!(
            "{} new connection {:?}<-{:?} at {:?}",
            prefix,
            conn.local_addr().unwrap(),
            conn.peer_addr().unwrap(),
            now
        );

        let (code, connack, connect) = loop {
            packetr = match packetr.read(&mut conn) {
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
                    Ok(v5::Packet::Connect(connect)) => match connect.validate() {
                        Ok(()) => break (ReasonCode::Success, false, Some(connect)),
                        Err(err) => {
                            error!("{}, invalid connect {}", prefix, err);
                            break (err.code(), true, None);
                        }
                    },
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
            send_connack(&prefix, code, &mut conn, timeout, max_size).ok();
        } else if let Some(connect) = connect {
            let args = AddConnectionArgs { conn, pkt: connect };
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

impl Handshake {
    pub(crate) fn prefix(&self) -> String {
        format!("h:{}", self.config.name)
    }
}

fn send_connack<W>(
    prefix: &str,
    code: v5::ConnackReasonCode,
    conn: &mut W,
    timeout: time::Instant,
    max_size: u32,
) -> Result<()>
where
    W: io::Write,
{
    use crate::MQTTWrite;

    let cack = v5::ConnAck::from_reason_code(code);
    let mut packetw = MQTTWrite::new(cack.encode().unwrap().as_ref(), max_size);
    loop {
        let (val, would_block) = match packetw.write(conn) {
            Ok(args) => args,
            Err(err) => {
                error!("{} problem writing connack packet {}", prefix, err);
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
                prefix, time::Instant::now()
            );
        } else {
            break Ok(());
        }
    }
}
