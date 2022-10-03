use log::info;

#[allow(unused_imports)]
use std::time;
use std::{fmt, io, result};

use crate::v5::{self, Error, ErrorKind, Packetize, ReasonCode, Result, VarU32};

/// Type implement a state machine to asynchronously read from socket using [mio].
#[derive(Debug)]
pub enum MQTTRead {
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
        rem: Vec<u8>,
        fh: v5::FixedHeader,
        max_size: usize,
    },
    None,
}

impl Default for MQTTRead {
    fn default() -> MQTTRead {
        MQTTRead::None
    }
}

impl fmt::Display for MQTTRead {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            MQTTRead::Init { .. } => write!(f, "MQTTRead::Init"),
            MQTTRead::Header { .. } => write!(f, "MQTTRead::Init"),
            MQTTRead::Remain { .. } => write!(f, "MQTTRead::Remain"),
            MQTTRead::Fin { .. } => write!(f, "MQTTRead::Fin"),
            MQTTRead::None => write!(f, "MQTTRead::None"),
        }
    }
}

impl MQTTRead {
    pub fn new(max_size: u32) -> MQTTRead {
        MQTTRead::Init {
            data: Vec::with_capacity(max_size as usize),
            max_size: max_size as usize,
        }
    }

    pub fn to_max_packet_size(&self) -> u32 {
        match self {
            MQTTRead::Init { max_size, .. } => *max_size as u32,
            MQTTRead::Header { max_size, .. } => *max_size as u32,
            MQTTRead::Remain { max_size, .. } => *max_size as u32,
            MQTTRead::Fin { max_size, .. } => *max_size as u32,
            MQTTRead::None => unreachable!(),
        }
    }

    // return (self,would_block)
    // Disconnected, and implies a bad connection.
    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    pub fn read<R: io::Read>(mut self, stream: &mut R) -> Result<(Self, bool)> {
        use MQTTRead::{Fin, Header, Init, Remain};

        self = match self.pre_read()? {
            (val, true) => return Ok((val, false)),
            (val, false) => val,
        };

        let mut scratch = [0_u8; 5];
        match self {
            Init { mut data, max_size } => match stream.read(&mut scratch) {
                Ok(0) => {
                    let s = format!("MQTTRead::Init, empty read");
                    let e = Error {
                        kind: ErrorKind::Disconnected,
                        description: s.clone(),
                        loc: format!("{}:{}", file!(), line!()),
                        ..Error::default()
                    };
                    info!("MQTTRead::Init, empty read");
                    Err(e)
                }
                Ok(n) => {
                    // println!("MQTTRead::Init datal:{} n:{}", data.len(), n);
                    data.extend_from_slice(&scratch[..n]);
                    let byte1 = data[0];
                    match data[1..].iter().skip_while(|b| **b >= 0x80).next() {
                        Some(_) => {
                            let (remaining_len, m) = VarU32::decode(&data[1..])?;

                            let pkt_len = 1 + m + (*remaining_len as usize);
                            check_packet_limit(pkt_len, max_size)?;

                            let fh = v5::FixedHeader { byte1, remaining_len };
                            // println!("MQTTRead::Init pkt_len:{}", pkt_len);
                            let res = match pkt_len {
                                pkt_len if pkt_len <= data.len() => {
                                    let rem = data[pkt_len..].to_vec();
                                    data.truncate(pkt_len);
                                    MQTTRead::Fin { data, rem, fh, max_size }
                                }
                                pkt_len => {
                                    let start = data.len();
                                    data.reserve(pkt_len);
                                    data.resize(pkt_len, 0);
                                    MQTTRead::Remain { data, start, fh, max_size }
                                }
                            };

                            Ok((res, false))
                        }
                        None => Ok((MQTTRead::Header { byte1, data, max_size }, false)),
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((MQTTRead::Init { data, max_size }, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((MQTTRead::Init { data, max_size }, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "MQTTRead::Init"),
            },
            Header { byte1, mut data, max_size } => match stream.read(&mut scratch) {
                Ok(0) => err!(Disconnected, desc:  "MQTTRead::Header, empty read"),
                Ok(n) => {
                    // println!("MQTTRead::Header data:{}", data.len());
                    data.extend_from_slice(&scratch[..n]);
                    match data[1..].iter().skip_while(|b| **b >= 0x80).next() {
                        Some(_) => {
                            let (remaining_len, m) = VarU32::decode(&data[1..])?;

                            let pkt_len = 1 + m + (*remaining_len as usize);
                            check_packet_limit(pkt_len, max_size)?;

                            let fh = v5::FixedHeader { byte1, remaining_len };
                            let res = match pkt_len {
                                pkt_len if pkt_len <= data.len() => {
                                    let rem = data[pkt_len..].to_vec();
                                    data.truncate(pkt_len);
                                    MQTTRead::Fin { data, rem, fh, max_size }
                                }
                                pkt_len => {
                                    let start = data.len();
                                    data.reserve(pkt_len);
                                    data.resize(pkt_len, 0);
                                    MQTTRead::Remain { data, start, fh, max_size }
                                }
                            };

                            Ok((res, false))
                        }
                        None => Ok((MQTTRead::Header { byte1, data, max_size }, false)),
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    Ok((MQTTRead::Header { byte1, data, max_size }, false))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    Ok((MQTTRead::Header { byte1, data, max_size }, true))
                }
                Err(err) => err!(Disconnected, try: Err(err), "MQTTRead::Header"),
            },
            Remain { mut data, start, fh, max_size } => {
                // println!("MQTTRead::Remain::read data:{} start:{}", data.len(), start);
                match stream.read(&mut data[start..]) {
                    Ok(0) => err!(Disconnected, desc:  "MQTTRead::Remain, empty read"),
                    Ok(n) if (start + n) == data.len() => {
                        let rem = Vec::new();
                        Ok((MQTTRead::Fin { data, rem, fh, max_size }, false))
                    }
                    Ok(n) if (start + n) < data.len() => {
                        let start = start + n;
                        Ok((MQTTRead::Remain { data, start, fh, max_size }, false))
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                        Ok((MQTTRead::Remain { data, start, fh, max_size }, false))
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        Ok((MQTTRead::Remain { data, start, fh, max_size }, true))
                    }
                    Err(err) => err!(Disconnected, try: Err(err), "MQTTRead::Remain"),
                    Ok(_) => unreachable!(),
                }
            }
            Fin { data, rem, fh, max_size } => {
                Ok((MQTTRead::Fin { data, rem, fh, max_size }, false))
            }
            MQTTRead::None => unreachable!(),
        }
    }

    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    pub fn parse(&self) -> Result<v5::Packet> {
        let (pkt, n, m) = match self {
            MQTTRead::Fin { data, fh, .. } => match fh.unwrap().0 {
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
            MQTTRead::Fin { mut data, rem, max_size, .. } => {
                data.truncate(0);
                data.extend(rem.into_iter());
                MQTTRead::Init { data, max_size }
            }
            val @ MQTTRead::Init { .. } => val,
            _ => unreachable!(),
        }
    }

    fn pre_read(self) -> Result<(Self, bool)> {
        match self {
            MQTTRead::Init { mut data, max_size } if data.len() > 1 => {
                let byte1 = data[0];
                let (remaining_len, m) = match VarU32::decode(&data[1..]) {
                    Ok((remaining_len, m)) => (remaining_len, m),
                    Err(_) => return Ok((MQTTRead::Init { data, max_size }, false)),
                };

                let pkt_len = 1 + m + (*remaining_len as usize);
                check_packet_limit(pkt_len, max_size)?;

                let fh = v5::FixedHeader { byte1, remaining_len };
                match pkt_len {
                    pkt_len if pkt_len <= data.len() => {
                        // println!("preread ok pkt_len:{} data:{}", pkt_len, data.len());
                        let rem = data[pkt_len..].to_vec();
                        data.truncate(pkt_len);
                        Ok((MQTTRead::Fin { data, rem, fh, max_size }, true))
                    }
                    _ => Ok((MQTTRead::Init { data, max_size }, false)),
                }
            }
            val => Ok((val, false)),
        }
    }
}

/// Type implement a state machine to asynchronously write to socket using [mio].
pub enum MQTTWrite {
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
    None,
}

impl Default for MQTTWrite {
    fn default() -> MQTTWrite {
        MQTTWrite::None
    }
}

impl fmt::Debug for MQTTWrite {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            MQTTWrite::Init { .. } => write!(f, "MQTTWrite::Init"),
            MQTTWrite::Remain { .. } => write!(f, "MQTTWrite::Remain"),
            MQTTWrite::Fin { .. } => write!(f, "MQTTWrite::Fin"),
            MQTTWrite::None => write!(f, "MQTTWrite::None"),
        }
    }
}

impl MQTTWrite {
    pub fn new(buf: &[u8], max_size: u32) -> MQTTWrite {
        let mut data = Vec::with_capacity(max_size as usize);
        data.extend_from_slice(buf);
        MQTTWrite::Init { data, max_size: max_size as usize }
    }

    pub fn to_max_packet_size(&self) -> u32 {
        match self {
            MQTTWrite::Init { max_size, .. } => *max_size as u32,
            MQTTWrite::Remain { max_size, .. } => *max_size as u32,
            MQTTWrite::Fin { max_size, .. } => *max_size as u32,
            MQTTWrite::None => unreachable!(),
        }
    }

    // return (self,would_block)
    // errors shall be folded as Disconnected, and implies a bad connection.
    pub fn write<W: io::Write>(self, stream: &mut W) -> Result<(Self, bool)> {
        use MQTTWrite::{Fin, Init, Remain};

        match self {
            // silently ignore if the packet size is more that requested.
            Init { data, max_size } if data.len() > max_size => {
                // TODO: add skipped packets to connection metrics.
                Ok((MQTTWrite::Fin { data, max_size }, false))
            }
            Init { data, max_size } if data.len() == 0 => {
                Ok((MQTTWrite::Fin { data, max_size }, false))
            }
            Init { data, max_size } => match stream.write(&data) {
                Ok(0) => err!(Disconnected, desc:  "MQTTWrite::Init, empty write"),
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
            Remain { data, start, max_size } if data[start..].len() == 0 => {
                Ok((MQTTWrite::Fin { data, max_size }, false))
            }
            Remain { data, start, max_size } => match stream.write(&data[start..]) {
                Ok(0) => err!(Disconnected, desc:  "MQTTWrite::Remain, empty write"),
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
            val @ MQTTWrite::None => unreachable!("{:?}", val),
        }
    }

    pub fn reset(self, buf: &[u8]) -> Self {
        match self {
            MQTTWrite::Init { mut data, max_size }
            | MQTTWrite::Fin { mut data, max_size } => {
                data.truncate(0);
                data.extend_from_slice(buf);
                MQTTWrite::Init { data, max_size }
            }
            _ => unreachable!(),
        }
    }
}

fn check_packet_limit(pkt_len: usize, max_size: usize) -> Result<()> {
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
