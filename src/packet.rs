use std::io;

use crate::{v5, Packetize, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

pub enum PacketRead {
    None,
    Init { data: Vec<u8> },
    Header { byte1: u8, data: Vec<u8> },
    Remain { data: Vec<u8>, fh: v5::FixedHeader },
    Fin { data: Vec<u8>, fh: v5::FixedHeader },
}

impl Default for PacketRead {
    fn default() -> PacketRead {
        PacketRead::None
    }
}

impl PacketRead {
    pub fn new() -> PacketRead {
        use crate::MSG_TYPICAL_SIZE;

        PacketRead::Init { data: Vec::with_capacity(MSG_TYPICAL_SIZE) }
    }

    // return (self, retry, wouldblock),
    // errors shall be folded as Disconnected, and implies a bad connection.
    pub fn read<R: io::Read>(self, mut stream: R) -> Result<(Self, bool, bool)> {
        use PacketRead::{Fin, Header, Init, Remain};

        let mut scratch = [0_u8; 5];
        match self {
            Init { mut data } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc: "PacketRead::Init"),
                Ok(1) => {
                    Ok((PacketRead::Header { byte1: scratch[0], data }, true, false))
                }
                Ok(n) => {
                    let byte1 = scratch[0];
                    match scratch[1..].iter().skip_while(|b| **b > 0x80).next() {
                        Some(_) => {
                            let (rlen, m) = VarU32::decode(&scratch[1..]).unwrap();
                            data.extend_from_slice(&scratch[m + 1..n]);
                            data.reserve(*rlen as usize);
                            let fh = v5::FixedHeader { byte1, remaining_len: rlen };
                            Ok((PacketRead::Remain { data, fh }, true, false))
                        }
                        None => {
                            data.extend_from_slice(&scratch[1..n]);
                            Ok((PacketRead::Header { byte1, data }, true, false))
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketRead::Init { data }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketRead::Init"),
            },
            Header { byte1, mut data } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc:  "PacketRead::Header"),
                Ok(n) => match scratch.into_iter().skip_while(|b| *b > 0x80).next() {
                    Some(_) => {
                        data.extend_from_slice(&scratch[..n]);
                        let (remaining_len, m) = VarU32::decode(&data).unwrap();
                        data.truncate(0);
                        data.extend_from_slice(&scratch[m..n]);
                        data.reserve(*remaining_len as usize);
                        let fh = v5::FixedHeader { byte1, remaining_len };
                        Ok((PacketRead::Remain { data, fh }, true, false))
                    }
                    None => {
                        data.extend_from_slice(&scratch[..n]);
                        Ok((PacketRead::Header { byte1, data }, true, false))
                    }
                },
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketRead::Header { byte1, data }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketRead::Header"),
            },
            Remain { mut data, fh } => {
                let m = data.len();
                unsafe { data.set_len(*fh.remaining_len as usize) };
                match stream.read(&mut data[m..]) {
                    Ok(0) => err!(Disconnected, desc:  "PacketRead::Remain"),
                    Ok(n) if (m + n) == (*fh.remaining_len as usize) => {
                        Ok((PacketRead::Fin { data, fh }, false, false))
                    }
                    Ok(n) => {
                        unsafe { data.set_len(m + n) };
                        Ok((PacketRead::Remain { data, fh }, true, false))
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        Ok((PacketRead::Remain { data, fh }, true, true))
                    }
                    Err(err) => err!(Disconnected, try: Err(err), "PacketRead::Remain"),
                }
            }
            Fin { data, fh } => Ok((PacketRead::Fin { data, fh }, false, false)),
            PacketRead::None => unreachable!(),
        }
    }

    pub fn parse(&self) -> Result<v5::Packet> {
        let (pkt, n, m) = match self {
            PacketRead::Fin { data, fh } => match fh.unwrap()?.0 {
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
            PacketRead::Fin { mut data, .. } => {
                data.truncate(0);
                PacketRead::Init { data }
            }
            _ => unreachable!(),
        }
    }
}

pub enum PacketWrite {
    None,
    Init { data: Vec<u8> },
    Remain { data: Vec<u8>, start: usize },
    Fin { data: Vec<u8> },
}

impl Default for PacketWrite {
    fn default() -> PacketWrite {
        PacketWrite::None
    }
}

impl PacketWrite {
    pub fn new(buf: &[u8]) -> PacketWrite {
        use crate::MSG_TYPICAL_SIZE;
        use std::cmp;

        let mut data = Vec::with_capacity(cmp::max(MSG_TYPICAL_SIZE, buf.len()));
        data.extend_from_slice(buf);
        PacketWrite::Init { data }
    }

    // return (self, retry, wouldblock)
    // errors shall be folded as Disconnected, and implies a bad connection.
    pub fn write<W: io::Write>(self, mut stream: W) -> Result<(Self, bool, bool)> {
        use PacketWrite::{Fin, Init, Remain};

        match self {
            Init { data } => match stream.write(&data) {
                Ok(0) => err!(Disconnected, desc:  "PacketWrite::Init"),
                Ok(n) if n == data.len() => Ok((PacketWrite::Fin { data }, false, false)),
                Ok(n) => Ok((PacketWrite::Remain { data, start: n }, true, false)),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((PacketWrite::Remain { data, start: 0 }, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketWrite::Remain { data, start: 0 }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketWrite::Init"),
            },
            Remain { data, start } => match stream.write(&data[start..]) {
                Ok(0) => err!(Disconnected, desc:  "PacketWrite::Remain"),
                Ok(n) if (start + n) == data.len() => {
                    Ok((PacketWrite::Fin { data }, false, false))
                }
                Ok(n) => {
                    Ok((PacketWrite::Remain { data, start: start + n }, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((PacketWrite::Remain { data, start }, true, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((PacketWrite::Remain { data, start }, true, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "PacketWrite::Remain"),
            },
            Fin { data } => Ok((PacketWrite::Fin { data }, false, false)),
            PacketWrite::None => unreachable!(),
        }
    }

    pub fn reset(self, buf: &[u8]) -> Self {
        match self {
            PacketWrite::Fin { mut data } => {
                data.truncate(0);
                data.extend_from_slice(buf);
                PacketWrite::Init { data }
            }
            _ => unreachable!(),
        }
    }
}
