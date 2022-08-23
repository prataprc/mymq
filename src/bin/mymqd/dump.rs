use chrono::NaiveDateTime;
use log::{debug, error, info, trace};

use std::{cmp, collections::BTreeMap, mem, net, path, time};

use crate::{Opt, Result, SubCommand};
use mymq::{netw, util, v5, MQTTRead};

const PCAP_TIMEOUT: time::Duration = time::Duration::from_millis(1000);
const MQTT_SIGNATURE: [u8; 6] = [0, 4, 77, 81, 84, 84];
const MAX_PACKET_SIZE: u32 = 10 * 1024 * 1024; // 10 MB

#[derive(Clone)]
struct Dump {
    write_file: Option<path::PathBuf>,
    append_file: Option<path::PathBuf>,
    read_file: Option<path::PathBuf>,
    time: u64,
    precision: pcap::Precision,
    promisc: bool,
    inp: bool,
    out: bool,
    device: Option<String>,
    eth: bool,
    ip: bool,
    tcp: bool,
    port: u16,
}

pub fn run(opts: Opt) -> Result<()> {
    match &opts.subcmd {
        SubCommand::Dump { devices, .. } if *devices => list_devices(&opts)?,
        SubCommand::Dump { read_file: Some(_), .. } => {
            let _capture = run_offline(opts.clone())?;
        }
        SubCommand::Dump { device: Some(_), .. } => {
            let mut capture = run_active(opts.clone())?;
            println!("capture stats {:?}", capture.stats().unwrap());
        }
        SubCommand::Dump { .. } => list_connected_devices(&opts)?,
        _ => unreachable!(),
    }

    Ok(())
}

fn run_offline(opts: Opt) -> Result<pcap::Capture<pcap::Offline>> {
    let dump: Dump = opts.subcmd.try_into()?;
    let capture = pcap::Capture::<pcap::Offline>::from_file_with_precision(
        &dump.read_file.unwrap(),
        dump.precision,
    )
    .map_err(|e| e.to_string())?;

    println!("pcap version:{:?}", capture.version());

    Ok(capture)
}

fn run_active(opts: Opt) -> Result<pcap::Capture<pcap::Active>> {
    // TODO: pcap::TimestampType
    // TODO: pcap::Capture::immediate_mode
    // TODO: pcap::Capture::rfmon
    // TODO: pcap::Capture::buffer_size
    // TODO: pcap::Capture::snaplen

    let dump: Dump = opts.subcmd.try_into()?;
    let device_name = dump.device.clone().unwrap();
    let capture = {
        let device = find_device(&device_name)?;
        let capture = pcap::Capture::<pcap::Inactive>::from_device(device)
            .map_err(|e| e.to_string())?
            .timeout(PCAP_TIMEOUT.as_millis() as i32)
            .promisc(dump.promisc)
            .precision(dump.precision);
        capture.open().map_err(|e| e.to_string())?
    };

    let save_file = match dump.write_file.clone() {
        Some(wf) => Some(capture.savefile(&wf).map_err(|e| e.to_string())?),
        None => match dump.append_file.clone() {
            Some(af) => Some(capture.savefile_append(&af).map_err(|e| e.to_string())?),
            None => None,
        },
    };

    if dump.inp {
        capture.direction(pcap::Direction::In).map_err(|e| e.to_string())?;
    } else if dump.out {
        capture.direction(pcap::Direction::Out).map_err(|e| e.to_string())?;
    } else {
        capture.direction(pcap::Direction::InOut).map_err(|e| e.to_string())?;
    }

    info!("capture link {}", capture.get_datalink().get_description().unwrap());
    let mut capture_iter = {
        let codec = Codec {
            dump: dump.clone(),
            save_file,
            link_type: capture.get_datalink(),
            mac: dump.device_mac()?,
            inp_conns: BTreeMap::default(),
            out_conns: BTreeMap::default(),
        };
        capture.iter(codec)
    };

    let deadline = time::Instant::now() + time::Duration::from_secs(dump.time);
    loop {
        match capture_iter.next() {
            Some(_pkt) if dump.time > 0 && time::Instant::now() > deadline => break,
            Some(Err(pcap::Error::TimeoutExpired)) => {
                trace!("timeout({:?}) expired from pcap", PCAP_TIMEOUT);
            }
            Some(pkt) => {
                let pkt = pkt.map_err(|e| e.to_string())?;
                pkt.to_string().map(|s| println!("{}", s));
            }
            None => break,
        }
    }
    info!("capture exiting");

    let capture = {
        let device = find_device(&dump.device.unwrap())?;
        let empty = pcap::Capture::<pcap::Inactive>::from_device(device)
            .map_err(|e| e.to_string())?
            .open()
            .map_err(|e| e.to_string())?;
        mem::replace(capture_iter.capture_mut(), empty)
    };
    Ok(capture)
}

fn find_device(name: &str) -> Result<pcap::Device> {
    for device in pcap::Device::list().map_err(|e| e.to_string())?.into_iter() {
        if device.name == name {
            return Ok(device);
        }
    }

    Err(format!("cannot find device {}", name))
}

fn list_devices(opts: &Opt) -> Result<()> {
    let devices = pcap::Device::list().map_err(|e| e.to_string())?;
    util::make_table(&devices).print_tty(!opts.force_color);

    Ok(())
}

fn list_connected_devices(opts: &Opt) -> Result<()> {
    let devices: Vec<pcap::Device> = pcap::Device::list()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|d| d.flags.connection_status == pcap::ConnectionStatus::Connected)
        .collect();
    util::make_table(&devices).print_tty(!opts.force_color);

    Ok(())
}

fn into_precision(val: &str) -> Result<pcap::Precision> {
    match val {
        "micro" => Ok(pcap::Precision::Micro),
        "nano" => Ok(pcap::Precision::Nano),
        _ => Err(format!("invalid precision {}", val)),
    }
}

type Key = (net::Ipv4Addr, u16);
type Val = (Vec<u8>, MQTTRead, bool);
struct Codec {
    dump: Dump,
    save_file: Option<pcap::Savefile>,
    link_type: pcap::Linktype,
    mac: pnet::util::MacAddr,
    inp_conns: BTreeMap<Key, Val>,
    out_conns: BTreeMap<Key, Val>,
}

impl Drop for Codec {
    fn drop(&mut self) {
        match &mut self.save_file {
            Some(file) => match file.flush() {
                Ok(()) => (),
                Err(err) => println!("error saving file: {}", err),
            },
            None => (),
        }
    }
}

impl pcap::PacketCodec for Codec {
    type Item = Packet;

    fn decode(&mut self, packet: pcap::Packet<'_>) -> Self::Item {
        self.save_file.as_mut().map(|f| f.write(&packet));

        if self.dump.eth {
            Packet::parse_l2(packet, self)
        } else if self.dump.ip {
            Packet::parse_l2(packet, self).parse_l3(self)
        } else if self.dump.tcp {
            Packet::parse_l2(packet, self).parse_l3(self).parse_l4(self)
        } else {
            Packet::parse_l2(packet, self).parse_l3(self).parse_l4(self).parse_mqtt(self)
        }
    }
}

impl Codec {
    fn read_packet(
        &mut self,
        key: &(net::Ipv4Addr, u16),
        dir: pcap::Direction,
    ) -> Result<Option<v5::Packet>> {
        use mymq::MQTTRead::{Fin, Header, Init, Remain};

        let conns = match dir {
            pcap::Direction::In => &mut self.inp_conns,
            pcap::Direction::Out => &mut self.out_conns,
            _ => unreachable!(),
        };

        let (buf, pktr) = match conns.get_mut(&key) {
            Some((buf, pktr, _)) => (buf, pktr),
            None => return Ok(None),
        };

        let mut pr = mem::replace(pktr, MQTTRead::default());
        let max_packet_size = pr.to_max_packet_size();
        let mut slice = buf.as_slice();
        // println!("read_packet key:{:?} dir:{:?} slice:{}", key, dir, slice.len());
        let res = loop {
            if slice.len() == 0 {
                break Ok(None);
            }
            pr = match pr.read(&mut slice) {
                Ok((val, true)) => {
                    pr = val;
                    break Ok(None);
                }
                Ok((val, false)) => val,
                Err(err) if err.kind() == mymq::ErrorKind::MalformedPacket => {
                    pr = MQTTRead::new(max_packet_size);
                    break Err(format!("malformed packet from MQTTRead"));
                }
                Err(err) if err.kind() == mymq::ErrorKind::ProtocolError => {
                    pr = MQTTRead::new(max_packet_size);
                    break Err(format!("protocol error from MQTTRead"));
                }
                Err(err) if err.kind() == mymq::ErrorKind::Disconnected => {
                    pr = MQTTRead::new(max_packet_size);
                    break Ok(None);
                }
                Err(err) => unreachable!("unexpected error {}", err),
            };

            match &pr {
                Init { .. } | Header { .. } | Remain { .. } => (),
                Fin { .. } => match pr.parse() {
                    Ok(pkt) => {
                        pr = pr.reset();
                        break Ok(Some(pkt));
                    }
                    Err(err) => {
                        pr = pr.reset();
                        break Err(err.to_string());
                    }
                },
                MQTTRead::None => unreachable!(),
            }
        };
        let m = buf.len() - slice.len();
        buf.drain(..m);

        // println!("read_packet pr:{:?} res:{:?}", pr, res);

        let _none = mem::replace(pktr, pr);
        res
    }
}

enum Packet {
    LoopBack(LoopBack),
    Ethernet(Ethernet),
    Ipv4(Ipv4),
    Tcp(Tcp),
    Mqtt(Mqtt),
    None,
}

impl Packet {
    fn to_string(&self) -> Option<String> {
        match self {
            Packet::Ethernet(pkt) if pkt.dir == pcap::Direction::In => {
                Some(format!("{} <- {} {}", pkt.ts, pkt.eth.source, pkt.eth.ethertype))
            }
            Packet::Ethernet(pkt) if pkt.dir == pcap::Direction::Out => Some(format!(
                "{} -> {} {}",
                pkt.ts, pkt.eth.destination, pkt.eth.ethertype
            )),
            Packet::Ethernet(_) => unreachable!(),
            Packet::Ipv4(pkt) if pkt.dir == pcap::Direction::In => {
                let ip = &pkt.ip;
                Some(format!(
                    concat!(
                        "{} <- {:15} ecn:{} frag:{} flags:{:x} ttl:{:3} ",
                        "id:{:5} proto:{}"
                    ),
                    pkt.ts,
                    ip.source,
                    ip.ecn,
                    ip.fragment_offset,
                    ip.flags,
                    ip.ttl,
                    ip.identification,
                    ip.next_level_protocol,
                ))
            }
            Packet::Ipv4(pkt) if pkt.dir == pcap::Direction::Out => {
                let ip = &pkt.ip;
                Some(format!(
                    concat!(
                        "{} -> {:15} ecn:{} frag:{} flags:{:x} ttl:{:3} ",
                        "id:{:5} proto:{}"
                    ),
                    pkt.ts,
                    ip.destination,
                    ip.ecn,
                    ip.fragment_offset,
                    ip.flags,
                    ip.ttl,
                    ip.identification,
                    ip.next_level_protocol,
                ))
            }
            Packet::Ipv4(pkt) => {
                let ip = &pkt.ip;
                Some(format!(
                    concat!(
                        "{} ** ecn:{} frag:{} flags:{:x} ttl:{:3} ",
                        "id:{:5} proto:{}"
                    ),
                    pkt.ts,
                    ip.ecn,
                    ip.fragment_offset,
                    ip.flags,
                    ip.ttl,
                    ip.identification,
                    ip.next_level_protocol,
                ))
            }
            Packet::Tcp(pkt) if pkt.dir == pcap::Direction::In => {
                let tcp = &pkt.tcp;
                let payload = &tcp.payload;
                let remote = format!("{}:{}", pkt.ip_remote, tcp.source);
                let n = cmp::min(payload.len(), 4);
                Some(format!(
                    "{} <- {:19} port:{} flags:{:2x} payload:{}({:?})",
                    pkt.ts,
                    remote,
                    tcp.destination,
                    tcp.flags,
                    payload.len(),
                    &payload[..n],
                ))
            }
            Packet::Tcp(pkt) if pkt.dir == pcap::Direction::Out => {
                let tcp = &pkt.tcp;
                let payload = &tcp.payload;
                let remote = format!("{}:{}", pkt.ip_remote, tcp.destination);
                let n = cmp::min(payload.len(), 4);
                Some(format!(
                    "{} -> {:19} port:{} flags:{:2x} payload:{}({:?})",
                    pkt.ts,
                    remote,
                    tcp.source,
                    tcp.flags,
                    payload.len(),
                    &payload[..n],
                ))
            }
            Packet::Tcp(pkt) => {
                let tcp = &pkt.tcp;
                let payload = &tcp.payload;
                let n = cmp::min(payload.len(), 4);
                Some(format!(
                    "{} ** src:{:5} dst:{:5} flags:{:2x} payload:{}({:?})",
                    pkt.ts,
                    tcp.source,
                    tcp.destination,
                    tcp.flags,
                    payload.len(),
                    &payload[..n],
                ))
            }
            Packet::Mqtt(pkt) if pkt.dir == pcap::Direction::In => {
                let remote = format!("{}:{}", pkt.ip_remote, pkt.src_port);
                Some(format!("{} <- {:19} {}", pkt.ts, remote, 0))
            }
            Packet::Mqtt(pkt) if pkt.dir == pcap::Direction::Out => {
                let remote = format!("{}:{}", pkt.ip_remote, pkt.dst_port);
                Some(format!("{} -> {:19} {}", pkt.ts, remote, 0))
            }
            Packet::Mqtt(pkt) => Some(format!("{} **", pkt.ts)),
            Packet::None => None,
            Packet::LoopBack(_) => None,
        }
    }
}

impl Packet {
    fn parse_l2(packet: pcap::Packet<'_>, codec: &Codec) -> Packet {
        use pnet::packet::{ethernet::EthernetPacket, FromPacket};

        let ts = NaiveDateTime::from_timestamp(
            packet.header.ts.tv_sec,
            u32::try_from(packet.header.ts.tv_usec).unwrap(),
        );

        match codec.link_type {
            pcap::Linktype::NULL => {
                let l3_typ = u32::from_ne_bytes(packet.data[..4].try_into().unwrap());
                let l3_typ = match l3_typ {
                    2 => pnet::packet::ethernet::EtherTypes::Ipv4,
                    _ => unreachable!(),
                };

                let pkt = LoopBack {
                    ts,
                    dir: pcap::Direction::InOut,
                    l3_typ,
                    payload: packet.data[4..].to_vec(),
                };

                Packet::LoopBack(pkt).map(codec)
            }
            pcap::Linktype::ETHERNET => match EthernetPacket::new(packet.data) {
                Some(ep) => {
                    let pkt = Ethernet {
                        ts,
                        dir: pcap::Direction::InOut,
                        eth: ep.from_packet(),
                    };
                    Packet::Ethernet(pkt).map(codec)
                }
                None => Packet::None,
            },
            _ => Packet::None,
        }
    }

    fn parse_l3(self, codec: &Codec) -> Packet {
        use pnet::packet::{ipv4::Ipv4Packet, FromPacket};

        let (ts, dir, l3_typ, payload) = match self {
            Packet::LoopBack(pkt) => (pkt.ts, pkt.dir, pkt.l3_typ, pkt.payload),
            Packet::Ethernet(pkt) => {
                (pkt.ts, pkt.dir, pkt.eth.ethertype, pkt.eth.payload)
            }
            Packet::None => return Packet::None,
            _ => unreachable!(),
        };

        match l3_typ {
            pnet::packet::ethernet::EtherTypes::Ipv4 => match Ipv4Packet::new(&payload) {
                Some(ip) => {
                    let pkt = Ipv4 { ts, dir, ip: ip.from_packet() };
                    Packet::Ipv4(pkt).map(codec)
                }
                None => Packet::None,
            },
            _ => Packet::None,
        }
    }

    fn parse_l4(self, codec: &Codec) -> Packet {
        use pnet::packet::ip::IpNextHeaderProtocols;
        use pnet::packet::{tcp::TcpPacket, FromPacket};

        let (ts, dir, l4_typ, ip_remote, payload) = match self {
            Packet::Ipv4(pkt) if pkt.dir == pcap::Direction::In => (
                pkt.ts,
                pkt.dir,
                pkt.ip.next_level_protocol,
                pkt.ip.source,
                pkt.ip.payload,
            ),
            Packet::Ipv4(pkt) if pkt.dir == pcap::Direction::Out => (
                pkt.ts,
                pkt.dir,
                pkt.ip.next_level_protocol,
                pkt.ip.destination,
                pkt.ip.payload,
            ),
            Packet::Ipv4(pkt) => (
                pkt.ts,
                pkt.dir,
                pkt.ip.next_level_protocol,
                pkt.ip.source, // loopback
                pkt.ip.payload,
            ),
            Packet::None => return Packet::None,
            _ => unreachable!(),
        };

        match l4_typ {
            IpNextHeaderProtocols::Tcp => match TcpPacket::new(&payload) {
                Some(tcpp) => {
                    let pkt = Tcp { ts, dir, ip_remote, tcp: tcpp.from_packet() };
                    Packet::Tcp(pkt).map(codec)
                }
                None => Packet::None,
            },
            _ => Packet::None,
        }
    }

    fn parse_mqtt(self, codec: &mut Codec) -> Packet {
        let (ts, dir, ip_remote, tcp) = match self {
            Packet::Tcp(pkt) => (pkt.ts, pkt.dir, pkt.ip_remote, pkt.tcp),
            Packet::None => return Packet::None,
            _ => unreachable!(),
        };

        let (key, dir) = if tcp.source < 2000 || codec.dump.port == tcp.source {
            let dir = match dir {
                pcap::Direction::InOut => pcap::Direction::Out,
                dir => dir,
            };
            ((ip_remote, tcp.destination), dir)
        } else if tcp.destination < 2000 || codec.dump.port == tcp.destination {
            let dir = match dir {
                pcap::Direction::InOut => pcap::Direction::In,
                dir => dir,
            };
            ((ip_remote, tcp.source), dir)
        } else {
            error!("unexpected");
            return Packet::None;
        };

        let conns = match dir {
            pcap::Direction::In => &mut codec.inp_conns,
            pcap::Direction::Out => &mut codec.out_conns,
            _ => unreachable!(),
        };

        let flags = tcp.flags;
        if (flags & 0x1) > 0 || (flags & 0x2) > 0 || (flags & 0x4) > 0 {
            if conns.remove(&key).is_some() {
                debug!("removing key:{:?} dir:{:?} flags:{:?}", key, dir, flags);
            }
        }
        // println!("dir:{:?} flags:{:x}", dir, flags);

        let mqtt_ok = match conns.get_mut(&key) {
            Some((buf, _, mqtt_ok)) => {
                buf.extend(&tcp.payload);
                *mqtt_ok
            }
            None if (flags & 0x2) > 0 => {
                let buf = tcp.payload.to_vec();
                let pktr = MQTTRead::new(MAX_PACKET_SIZE);
                debug!("inserting key:{:?} dir:{:?}", key, dir);
                conns.insert(key, (buf, pktr, false));
                false
            }
            None => return Packet::None,
        };

        if dir == pcap::Direction::In && !mqtt_ok {
            let (buf, _, mqtt_ok) = conns.get_mut(&key).unwrap();
            let tri = check_connect(buf);
            match tri {
                Triplet::Maybe => return Packet::None,
                Triplet::True => {
                    *mqtt_ok = true;
                    let buf = Vec::default();
                    let pktr = MQTTRead::new(MAX_PACKET_SIZE);
                    let key = (ip_remote, tcp.source);
                    debug!("inserting remote key:{:?} dir:{:?}", key, dir);
                    codec.out_conns.insert(key, (buf, pktr, true));
                }
                Triplet::False => {
                    if conns.remove(&key).is_some() {
                        debug!("removing (not mqtt) key:{:?} dir:{:?}", key, dir);
                    }
                    return Packet::None;
                }
            }
        }

        // println!("dir:{:?} flags:{:x}", dir, flags);

        match codec.read_packet(&key, dir) {
            Ok(Some(mqtt)) => {
                // println!("mqtt flags:{:x} dir:{:?}", flags, dir);
                let pkt = Mqtt {
                    ts,
                    dir,
                    ip_remote,
                    src_port: tcp.source,
                    dst_port: tcp.destination,
                    mqtt,
                };
                Packet::Mqtt(pkt).map(codec)
            }
            Ok(None) => {
                // println!("none flags:{:x} dir:{:?}", flags, dir);
                Packet::None
            }
            Err(err) => {
                println!("error reading packet: {}", err);
                Packet::None
            }
        }
    }

    fn map(self, codec: &Codec) -> Packet {
        match self {
            Packet::LoopBack(pkt) => Packet::LoopBack(pkt),
            Packet::Ethernet(Ethernet { ts, eth, .. }) => {
                if codec.mac == eth.source {
                    let pkt = Ethernet { ts, dir: pcap::Direction::Out, eth };
                    Packet::Ethernet(pkt)
                } else if codec.mac == eth.destination {
                    let pkt = Ethernet { ts, dir: pcap::Direction::In, eth };
                    Packet::Ethernet(pkt)
                } else {
                    Packet::None
                }
            }
            pkt => pkt,
        }
    }
}

struct LoopBack {
    ts: NaiveDateTime,
    dir: pcap::Direction,
    l3_typ: pnet::packet::ethernet::EtherType,
    payload: Vec<u8>,
}

struct Ethernet {
    ts: NaiveDateTime,
    dir: pcap::Direction,
    eth: pnet::packet::ethernet::Ethernet,
}

struct Ipv4 {
    ts: NaiveDateTime,
    dir: pcap::Direction,
    ip: pnet::packet::ipv4::Ipv4,
}

struct Tcp {
    ts: NaiveDateTime,
    dir: pcap::Direction,
    ip_remote: net::Ipv4Addr,
    tcp: pnet::packet::tcp::Tcp,
}

struct Mqtt {
    ts: NaiveDateTime,
    dir: pcap::Direction,
    ip_remote: net::Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    mqtt: v5::Packet,
}

impl TryFrom<SubCommand> for Dump {
    type Error = String;

    fn try_from(val: SubCommand) -> Result<Dump> {
        match val {
            SubCommand::Dump {
                write_file,
                append_file,
                read_file,
                time,
                precision,
                promisc,
                inp,
                out,
                eth,
                ip,
                tcp,
                port,
                device,
                ..
            } => {
                let val = Dump {
                    write_file,
                    append_file,
                    read_file,
                    time,
                    precision: into_precision(&precision)?,
                    promisc,
                    inp,
                    out,
                    eth,
                    ip,
                    tcp,
                    port,
                    device,
                };
                Ok(val)
            }
            _ => unreachable!(),
        }
    }
}

impl Dump {
    fn device_mac(&self) -> Result<pnet::util::MacAddr> {
        let device_name = self.device.clone().unwrap();
        match netw::interfaces().into_iter().filter(|inf| inf.name == device_name).next()
        {
            Some(inf) => match inf.mac {
                Some(mac) => Ok(mac.to_string().parse::<pnet::util::MacAddr>().unwrap()),
                None => Err(format!("cannot find mac for device {:?}", device_name)),
            },
            None => Err(format!("cannot find mac for device {:?}", device_name)),
        }
    }
}

fn check_connect(buf: &[u8]) -> Triplet {
    use mymq::Packetize;

    if buf.len() < 7 {
        Triplet::Maybe
    } else {
        match mymq::VarU32::decode(&buf[1..7]).ok() {
            Some((_, n)) if buf.len() >= (1 + n + 6) => {
                if MQTT_SIGNATURE == &buf[1 + n..1 + n + 6] {
                    Triplet::True
                } else {
                    Triplet::False
                }
            }
            Some((_, _)) => Triplet::Maybe,
            None => Triplet::False,
        }
    }
}

#[derive(Debug)]
enum Triplet {
    True,
    False,
    Maybe,
}
