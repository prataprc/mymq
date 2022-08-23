use chrono::NaiveDateTime;
use log::{debug, info};

use std::{mem, net, path, time};

use crate::{Opt, Result, SubCommand};
use mymq::{netw, util, v5};

const PCAP_TIMEOUT: time::Duration = time::Duration::from_millis(1000);
const MQTT_SIGNATURE: [u8; 4] = [77, 81, 84, 84];

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
        };
        capture.iter(codec)
    };

    let deadline = time::Instant::now() + time::Duration::from_secs(dump.time);
    loop {
        match capture_iter.next() {
            Some(_pkt) if dump.time > 0 && time::Instant::now() > deadline => break,
            Some(Err(pcap::Error::TimeoutExpired)) => {
                debug!("timeout({:?}) expired from pcap", PCAP_TIMEOUT);
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

struct Codec {
    dump: Dump,
    save_file: Option<pcap::Savefile>,
    link_type: pcap::Linktype,
    mac: pnet::util::MacAddr,
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
                let remote = format!("{}:{}", pkt.ip_remote, tcp.source);
                Some(format!(
                    "{} <- {:19} port:{} flags:{:2x}",
                    pkt.ts, remote, tcp.destination, tcp.flags,
                ))
            }
            Packet::Tcp(pkt) if pkt.dir == pcap::Direction::Out => {
                let tcp = &pkt.tcp;
                let remote = format!("{}:{}", pkt.ip_remote, tcp.destination);
                Some(format!(
                    "{} -> {:19} port:{} flags:{:2x}",
                    pkt.ts, remote, tcp.source, tcp.flags,
                ))
            }
            Packet::Tcp(pkt) => {
                let tcp = &pkt.tcp;
                Some(format!(
                    "{} ** src:{:5} dst:{:5} flags:{:2x}",
                    pkt.ts, tcp.source, tcp.destination, tcp.flags,
                ))
            }
            Packet::Mqtt(pkt) if pkt.dir == pcap::Direction::In => {
                Some(format!("{} <-", pkt.ts))
            }
            Packet::Mqtt(pkt) if pkt.dir == pcap::Direction::Out => {
                Some(format!("{} ->", pkt.ts))
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

    fn parse_mqtt(self, codec: &Codec) -> Packet {
        use mymq::Packetize;

        let (ts, dir, ip_remote, src_port, dst_port, payload) = match self {
            Packet::Tcp(pkt) if pkt.dir == pcap::Direction::In => (
                pkt.ts,
                pkt.dir,
                pkt.ip_remote,
                pkt.tcp.source,
                pkt.tcp.destination,
                pkt.tcp.payload,
            ),
            Packet::Tcp(pkt) if pkt.dir == pcap::Direction::Out => (
                pkt.ts,
                pkt.dir,
                pkt.ip_remote,
                pkt.tcp.source,
                pkt.tcp.destination,
                pkt.tcp.payload,
            ),
            Packet::None => return Packet::None,
            _ => unreachable!(),
        };

        match payload.len() {
            n if n > 4 && &payload[..4] == &MQTT_SIGNATURE => {
                match v5::Packet::decode(payload) {
                    Ok((mqtt, _)) => {
                        let pkt = Mqtt { ts, dir, ip_remote, src_port, dst_port, mqtt };
                        Packet::Mqtt(pkt).map(codec)
                    }
                    Err(_err) => Packet::None,
                }
            }
            _ => Packet::None,
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
