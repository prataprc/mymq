use log::error;
use std::{io, thread, time};

use crate::{v5, Packetize, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub enum PacketRead {
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
        fh: v5::FixedHeader,
        max_size: usize,
    },
    Fin {
        data: Vec<u8>,
        fh: v5::FixedHeader,
        max_size: usize,
    },
}

impl Default for PacketRead {
    fn default() -> PacketRead {
        PacketRead::None
    }
}

impl PacketRead {
    pub fn new(max_size: usize) -> PacketRead {
        use crate::MAX_PACKET_SIZE;

        PacketRead::Init {
            data: Vec::with_capacity(MAX_PACKET_SIZE),
            max_size,
        }
    }

    // return (self, retry, wouldblock),
    // Disconnected, and implies a bad connection.
    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    pub fn read<R: io::Read>(self, mut stream: R) -> Result<(Self, bool, bool)> {
        use PacketRead::{Fin, Header, Init, Remain};

        let mut scratch = [0_u8; 5];
        match self {
            Init { mut data, max_size } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc: "PacketRead::Init"),
                Ok(1) => {
                    let val = PacketRead::Header { byte1: scratch[0], data, max_size };
                    Ok((val, true, false))
                }
                Ok(n) => {
                    let byte1 = scratch[0];
                    match scratch[1..].iter().skip_while(|b| **b > 0x80).next() {
                        Some(_) => {
                            let (remaining_len, m) = VarU32::decode(&scratch[1..])?;
                            if *remaining_len > (max_size as u32) {
                                err!(
                                    MalformedPacket,
                                    code: MalformedPacket,
                                    "PacketRead::read remaining-len:{}",
                                    *remaining_len
                                )?
                            }
                            data.extend_from_slice(&scratch[m + 1..n]);
                            data.reserve(*remaining_len as usize);
                            let fh = v5::FixedHeader { byte1, remaining_len };
                            Ok((PacketRead::Remain { data, fh, max_size }, true, false))
                        }
                        None => {
                            data.extend_from_slice(&scratch[1..n]);
                            let val = PacketRead::Header { byte1, data, max_size };
                            Ok((val, true, false))
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketRead::Init { data, max_size }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketRead::Init"),
            },
            Header { byte1, mut data, max_size } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc:  "PacketRead::Header"),
                Ok(n) => match scratch.into_iter().skip_while(|b| *b > 0x80).next() {
                    Some(_) => {
                        data.extend_from_slice(&scratch[..n]);
                        let (remaining_len, m) = VarU32::decode(&data)?;
                        if *remaining_len > (max_size as u32) {
                            err!(
                                MalformedPacket,
                                code: MalformedPacket,
                                "PacketRead::read remaining-len:{}",
                                *remaining_len
                            )?
                        }
                        data.truncate(0);
                        data.extend_from_slice(&scratch[m..n]);
                        data.reserve(*remaining_len as usize);
                        let fh = v5::FixedHeader { byte1, remaining_len };
                        Ok((PacketRead::Remain { data, fh, max_size }, true, false))
                    }
                    None => {
                        data.extend_from_slice(&scratch[..n]);
                        Ok((PacketRead::Header { byte1, data, max_size }, true, false))
                    }
                },
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketRead::Header { byte1, data, max_size }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketRead::Header"),
            },
            Remain { mut data, fh, max_size } => {
                let m = data.len();
                unsafe { data.set_len(*fh.remaining_len as usize) };
                match stream.read(&mut data[m..]) {
                    Ok(0) => err!(Disconnected, desc:  "PacketRead::Remain"),
                    Ok(n) if (m + n) == (*fh.remaining_len as usize) => {
                        Ok((PacketRead::Fin { data, fh, max_size }, false, false))
                    }
                    Ok(n) => {
                        unsafe { data.set_len(m + n) };
                        Ok((PacketRead::Remain { data, fh, max_size }, true, false))
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        Ok((PacketRead::Remain { data, fh, max_size }, true, true))
                    }
                    Err(err) => err!(Disconnected, try: Err(err), "PacketRead::Remain"),
                }
            }
            Fin { data, fh, max_size } => {
                let val = PacketRead::Fin { data, fh, max_size };
                Ok((val, false, false))
            }
            PacketRead::None => unreachable!(),
        }
    }

    pub fn parse(&self) -> Result<v5::Packet> {
        let (pkt, n, m) = match self {
            PacketRead::Fin { data, fh, .. } => match fh.unwrap()?.0 {
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
            err!(MalformedPacket, code: MalformedPacket, "PacketRead::Fin {}!={}", n, m)
        } else {
            Ok(pkt)
        }
    }

    pub fn reset(self) -> Self {
        match self {
            PacketRead::Fin { mut data, max_size, .. } => {
                data.truncate(0);
                PacketRead::Init { data, max_size }
            }
            _ => unreachable!(),
        }
    }
}

pub enum PacketWrite {
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

impl Default for PacketWrite {
    fn default() -> PacketWrite {
        PacketWrite::None
    }
}

impl PacketWrite {
    pub fn new(buf: &[u8], max_size: usize) -> PacketWrite {
        use crate::MAX_PACKET_SIZE;
        use std::cmp;

        let mut data = Vec::with_capacity(cmp::max(MAX_PACKET_SIZE, buf.len()));
        data.extend_from_slice(buf);
        PacketWrite::Init { data, max_size }
    }

    // return (self, retry, wouldblock)
    // errors shall be folded as Disconnected, and implies a bad connection.
    pub fn write<W: io::Write>(self, mut stream: W) -> Result<(Self, bool, bool)> {
        use PacketWrite::{Fin, Init, Remain};

        match self {
            // silently ignore if the packet size is more that requested.
            Init { data, max_size } if data.len() > max_size => {
                Ok((PacketWrite::Fin { data, max_size }, false, false))
            }
            Init { data, max_size } => match stream.write(&data) {
                Ok(0) => err!(Disconnected, desc:  "PacketWrite::Init"),
                Ok(n) if n == data.len() => {
                    Ok((PacketWrite::Fin { data, max_size }, false, false))
                }
                Ok(n) => {
                    let val = PacketWrite::Remain { data, start: n, max_size };
                    Ok((val, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((PacketWrite::Remain { data, start: 0, max_size }, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketWrite::Remain { data, start: 0, max_size }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketWrite::Init"),
            },
            Remain { data, start, max_size } => match stream.write(&data[start..]) {
                Ok(0) => err!(Disconnected, desc:  "PacketWrite::Remain"),
                Ok(n) if (start + n) == data.len() => {
                    Ok((PacketWrite::Fin { data, max_size }, false, false))
                }
                Ok(n) => {
                    let val = PacketWrite::Remain { data, start: start + n, max_size };
                    Ok((val, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((PacketWrite::Remain { data, start, max_size }, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketWrite::Remain { data, start, max_size }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketWrite::Remain"),
            },
            Fin { data, max_size } => {
                Ok((PacketWrite::Fin { data, max_size }, false, false))
            }
            PacketWrite::None => unreachable!(),
        }
    }

    pub fn reset(self, buf: &[u8]) -> Self {
        match self {
            PacketWrite::Fin { mut data, max_size } => {
                data.truncate(0);
                data.extend_from_slice(buf);
                PacketWrite::Init { data, max_size }
            }
            _ => unreachable!(),
        }
    }
}

pub fn send_disconnect(
    prefix: &str,
    code: ReasonCode,
    conn: &mio::net::TcpStream,
) -> Result<()> {
    use crate::{MAX_CONNECT_TIMEOUT, MAX_PACKET_SIZE, MAX_SOCKET_RETRY};

    let dur = MAX_CONNECT_TIMEOUT / u64::try_from(MAX_SOCKET_RETRY).unwrap();
    let dc = v5::Disconnect::from_reason_code(code);
    let mut packetw = PacketWrite::new(dc.encode().unwrap().as_ref(), MAX_PACKET_SIZE);
    let mut retries = 0;
    loop {
        let (val, retry, would_block) = match packetw.write(conn) {
            Ok(args) => args,
            Err(err) => {
                error!("{} problem writing disconnect packet {}", prefix, err);
                break Err(err);
            }
        };
        packetw = val;

        if would_block {
            thread::sleep(time::Duration::from_millis(dur));
        } else if retry && retries < MAX_SOCKET_RETRY {
            retries += 1;
        } else if retry {
            break err!(
                Disconnected,
                desc: "{} failed writing disconnect after retries",
                prefix
            );
        } else {
            break Ok(());
        }
    }
}

// note that this can block as much as MAX_CONNECT_TIMEOUT.
pub fn send_connack(
    prefix: &str,
    code: ReasonCode,
    conn: &mio::net::TcpStream,
) -> Result<()> {
    use crate::{MAX_CONNECT_TIMEOUT, MAX_PACKET_SIZE, MAX_SOCKET_RETRY};

    let dur = MAX_CONNECT_TIMEOUT / u64::try_from(MAX_SOCKET_RETRY).unwrap();
    let dc = v5::ConnAck::from_reason_code(code);
    let mut packetw = PacketWrite::new(dc.encode().unwrap().as_ref(), MAX_PACKET_SIZE);
    let mut retries = 0;
    loop {
        let (val, retry, would_block) = match packetw.write(conn) {
            Ok(args) => args,
            Err(err) => {
                error!("{} problem writing disconnect packet {}", prefix, err);
                break Err(err);
            }
        };
        packetw = val;

        if would_block {
            thread::sleep(time::Duration::from_millis(dur));
        } else if retry && retries < MAX_SOCKET_RETRY {
            retries += 1;
        } else if retry {
            break err!(
                Disconnected,
                desc: "{} failed writing disconnect after retries",
                prefix
            );
        } else {
            break Ok(());
        }
    }
}
