use log::error;
use std::{io, thread, time};

use crate::{v5, Packetize, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub enum MQTTRead {
    None,
    Init {
        data: Vec<u8>,
        max_size: usize,
    },
    Header {
        byte1: u8,
        data: Vec<u8>,
        max_size: usize,
    },
    Remain {
        data: Vec<u8>,
        start: usize,
        fh: v5::FixedHeader,
        max_size: usize,
    },
    Fin {
        data: Vec<u8>,
        fh: v5::FixedHeader,
        max_size: usize,
    },
}

impl Default for MQTTRead {
    fn default() -> MQTTRead {
        MQTTRead::None
    }
}

impl MQTTRead {
    pub fn new(max_size: u32) -> MQTTRead {
        MQTTRead::Init {
            data: Vec::with_capacity(max_size as usize),
            max_size: max_size as usize,
        }
    }

    // return (self,would_block)
    // Disconnected, and implies a bad connection.
    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    pub fn read<R: io::Read>(self, mut stream: R) -> Result<(Self, bool)> {
        use MQTTRead::{Fin, Header, Init, Remain};

        let mut scratch = [0_u8; 5];
        match self {
            Init { mut data, max_size } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc: "MQTTRead::Init"),
                Ok(1) => {
                    data.push(scratch[0]);
                    let byte1 = scratch[0];
                    Ok((MQTTRead::Header { byte1, data, max_size }, false))
                }
                Ok(n) => {
                    data.extend_from_slice(&scratch[..n]);
                    let byte1 = scratch[0];
                    match scratch[1..].iter().skip_while(|b| **b > 0x80).next() {
                        Some(_) => {
                            let (remaining_len, m) = VarU32::decode(&scratch[1..])?;

                            let pkt_len = 1 + m + (*remaining_len as usize);
                            read_packet_limit(pkt_len, max_size)?;

                            let fh = v5::FixedHeader { byte1, remaining_len };
                            data.reserve(pkt_len);
                            data.resize(pkt_len, 0);
                            let start = n;
                            Ok((MQTTRead::Remain { data, start, fh, max_size }, false))
                        }
                        None => Ok((MQTTRead::Header { byte1, data, max_size }, false)),
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((MQTTRead::Init { data, max_size }, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "MQTTRead::Init"),
            },
            Header { byte1, mut data, max_size } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc:  "MQTTRead::Header"),
                Ok(n) => {
                    data.extend_from_slice(&scratch[..n]);
                    let start = data.len();
                    match scratch.into_iter().skip_while(|b| *b > 0x80).next() {
                        Some(_) => {
                            let (remaining_len, m) = VarU32::decode(&data[1..])?;

                            let pkt_len = 1 + m + (*remaining_len as usize);
                            read_packet_limit(pkt_len, max_size)?;

                            let fh = v5::FixedHeader { byte1, remaining_len };
                            data.reserve(pkt_len);
                            data.resize(pkt_len);
                            Ok((MQTTRead::Remain { data, start, fh, max_size }, false))
                        }
                        None => Ok((MQTTRead::Header { byte1, data, max_size }, false)),
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((MQTTRead::Header { byte1, data, max_size }, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "MQTTRead::Header"),
            },
            Remain { mut data, start, fh, max_size } => {
                match stream.read(&mut data[start..]) {
                    Ok(0) => err!(Disconnected, desc:  "MQTTRead::Remain"),
                    Ok(n) if (start + n) == data.len() => {
                        Ok((MQTTRead::Fin { data, fh, max_size }, false))
                    }
                    Ok(n) if (start + n) < data.len() => {
                        let start = start + n;
                        Ok((MQTTRead::Remain { data, start, fh, max_size }, false))
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        Ok((MQTTRead::Remain { data, start, fh, max_size }, true))
                    }
                    Err(err) => err!(Disconnected, try: Err(err), "MQTTRead::Remain"),
                    Ok(_) => unreachable!(),
                }
            }
            Fin { data, fh, max_size } => {
                Ok((MQTTRead::Fin { data, fh, max_size }, false))
            }
            MQTTRead::None => unreachable!(),
        }
    }

    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    pub fn parse(&self) -> Result<v5::Packet> {
        let (pkt, n, m) = match self {
            MQTTRead::Fin { data, fh, .. } => match fh.unwrap()?.0 {
                v5::PacketType::Connect => {
                    let (pkt, n) = v5::Connect::decode(&data)?;
                    (v5::Packet::Connect(pkt), n, data.len())
                }
                v5::PacketType::ConnAck => {
                    let (pkt, n) = v5::ConnAck::decode(&data)?;
                    (v5::Packet::ConnAck(pkt), n, data.len())
                }
                v5::PacketType::Publish => {
                    let (pkt, n) = v5::Publish::decode(&data)?;
                    (v5::Packet::Publish(pkt), n, data.len())
                }
                v5::PacketType::PubAck => {
                    let (pkt, n) = v5::Pub::decode(&data)?;
                    (v5::Packet::PubAck(pkt), n, data.len())
                }
                v5::PacketType::PubRec => {
                    let (pkt, n) = v5::Pub::decode(&data)?;
                    (v5::Packet::PubRec(pkt), n, data.len())
                }
                v5::PacketType::PubRel => {
                    let (pkt, n) = v5::Pub::decode(&data)?;
                    (v5::Packet::PubRel(pkt), n, data.len())
                }
                v5::PacketType::PubComp => {
                    let (pkt, n) = v5::Pub::decode(&data)?;
                    (v5::Packet::PubComp(pkt), n, data.len())
                }
                v5::PacketType::Subscribe => {
                    let (pkt, n) = v5::Subscribe::decode(&data)?;
                    (v5::Packet::Subscribe(pkt), n, data.len())
                }
                v5::PacketType::SubAck => {
                    let (pkt, n) = v5::SubAck::decode(&data)?;
                    (v5::Packet::SubAck(pkt), n, data.len())
                }
                v5::PacketType::UnSubscribe => {
                    let (pkt, n) = v5::UnSubscribe::decode(&data)?;
                    (v5::Packet::UnSubscribe(pkt), n, data.len())
                }
                v5::PacketType::UnsubAck => {
                    let (pkt, n) = v5::UnsubAck::decode(&data)?;
                    (v5::Packet::UnsubAck(pkt), n, data.len())
                }
                v5::PacketType::PingReq => {
                    let (_pkt, n) = v5::PingReq::decode(&data)?;
                    (v5::Packet::PingReq, n, data.len())
                }
                v5::PacketType::PingResp => {
                    let (_pkt, n) = v5::PingResp::decode(&data)?;
                    (v5::Packet::PingResp, n, data.len())
                }
                v5::PacketType::Disconnect => {
                    let (pkt, n) = v5::Disconnect::decode(&data)?;
                    (v5::Packet::Disconnect(pkt), n, data.len())
                }
                v5::PacketType::Auth => {
                    let (pkt, n) = v5::Auth::decode(&data)?;
                    (v5::Packet::Auth(pkt), n, data.len())
                }
            },
            _ => unreachable!(),
        };

        if n != m {
            err!(MalformedPacket, code: MalformedPacket, "MQTTRead::Fin {}!={}", n, m)
        } else {
            Ok(pkt)
        }
    }

    pub fn reset(self) -> Self {
        match self {
            MQTTRead::Fin { mut data, max_size, .. } => {
                data.truncate(0);
                MQTTRead::Init { data, max_size }
            }
            _ => unreachable!(),
        }
    }
}

pub enum MQTTWrite {
    None,
    Init {
        data: Vec<u8>,
        max_size: usize,
    },
    Remain {
        data: Vec<u8>,
        start: usize,
        max_size: usize,
    },
    Fin {
        data: Vec<u8>,
        max_size: usize,
    },
}

impl Default for MQTTWrite {
    fn default() -> MQTTWrite {
        MQTTWrite::None
    }
}

impl MQTTWrite {
    pub fn new(buf: &[u8], max_size: u32) -> MQTTWrite {
        let mut data = Vec::with_capacity(max_size as usize);
        data.extend_from_slice(buf);
        MQTTWrite::Init { data, max_size: max_size as usize }
    }

    // return (self,would_block)
    // errors shall be folded as Disconnected, and implies a bad connection.
    pub fn write<W: io::Write>(self, mut stream: W) -> Result<(Self, bool)> {
        use MQTTWrite::{Fin, Init, Remain};

        match self {
            // silently ignore if the packet size is more that requested.
            Init { data, max_size } if data.len() > max_size => {
                // TODO: add skipped packets to connection metrics.
                Ok((MQTTWrite::Fin { data, max_size }, false))
            }
            Init { data, max_size } => match stream.write(&data) {
                Ok(0) => err!(Disconnected, desc:  "MQTTWrite::Init"),
                Ok(n) if n == data.len() => {
                    Ok((MQTTWrite::Fin { data, max_size }, false))
                }
                Ok(n) if n < data.len() => {
                    Ok((MQTTWrite::Remain { data, start: n, max_size }, false))
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((MQTTWrite::Remain { data, start: 0, max_size }, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((MQTTWrite::Remain { data, start: 0, max_size }, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "MQTTWrite::Init"),
                Ok(_) => unreachable!(),
            },
            Remain { data, start, max_size } => match stream.write(&data[start..]) {
                Ok(0) => err!(Disconnected, desc:  "MQTTWrite::Remain"),
                Ok(n) if (start + n) == data.len() => {
                    Ok((MQTTWrite::Fin { data, max_size }, false))
                }
                Ok(n) if (start + n) < data.len() => {
                    let start = start + n;
                    Ok((MQTTWrite::Remain { data, start, max_size }, false))
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((MQTTWrite::Remain { data, start, max_size }, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((MQTTWrite::Remain { data, start, max_size }, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "MQTTWrite::Remain"),
                Ok(_) => unreachable!(),
            },
            Fin { data, max_size } => Ok((MQTTWrite::Fin { data, max_size }, false)),
            MQTTWrite::None => unreachable!(),
        }
    }

    pub fn reset(self, buf: &[u8]) -> Self {
        match self {
            MQTTWrite::Fin { mut data, max_size } => {
                data.truncate(0);
                data.extend_from_slice(buf);
                MQTTWrite::Init { data, max_size }
            }
            _ => unreachable!(),
        }
    }
}

pub fn send_disconnect(
    prefix: &str,
    code: v5::DisconnReasonCode,
    conn: &mio::net::TcpStream,
    timeout: time::Instant,
    max_size: u32,
) -> Result<()> {
    use crate::SLEEP_10MS;

    let dc = v5::Disconnect::new(code, None);
    let mut packetw = MQTTWrite::new(dc.encode().unwrap().as_ref(), max_size);
    loop {
        let (val, would_block) = match packetw.write(conn) {
            Ok(args) => args,
            Err(err) => {
                error!("{} problem writing disconnect packet {}", prefix, err);
                break Err(err);
            }
        };
        packetw = val;

        if would_block && timeout < time::Instant::now() {
            thread::sleep(SLEEP_10MS);
        } else if would_block {
            break err!(
                Disconnected,
                desc: "{} failed writing disconnect after {:?}",
                prefix, time::Instant::now()
            );
        } else {
            break Ok(());
        }
    }
}

pub fn send_connack(
    prefix: &str,
    code: v5::ConnackReasonCode,
    conn: &mio::net::TcpStream,
    timeout: time::Instant,
    max_size: u32,
) -> Result<()> {
    use crate::SLEEP_10MS;

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

fn read_packet_limit(pkt_len: usize, max_size: usize) -> Result<()> {
    if pkt_len > max_size {
        err!(
            MalformedPacket,
            code: PacketTooLarge,
            "MQTTRead::read packet_len:{}",
            pkt_len
        )
    } else {
        Ok(())
    }
}
