use chrono::NaiveDateTime;
use log::debug;

use std::{fmt, mem, path, result, time};

use crate::{Opt, Result, SubCommand};
use mymq::{netw, util};

const PCAP_TIMEOUT: time::Duration = time::Duration::from_millis(1000);

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
                println!("{}", pkt);
            }
            None => break,
        }
    }

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

enum Packet {
    Ethernet(Ethernet),
    None,
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
        use pnet::packet::{ethernet::EthernetPacket, FromPacket};

        self.save_file.as_mut().map(|f| f.write(&packet));

        let ts = NaiveDateTime::from_timestamp(
            packet.header.ts.tv_sec,
            u32::try_from(packet.header.ts.tv_usec).unwrap(),
        );

        match self.link_type {
            pcap::Linktype::ETHERNET if self.dump.eth => {
                let pkt = match EthernetPacket::new(packet.data) {
                    Some(ep) => {
                        let pkt = Ethernet {
                            ts,
                            dir: pcap::Direction::InOut,
                            eth: ep.from_packet(),
                        };
                        Packet::Ethernet(pkt)
                    }
                    None => Packet::None,
                };
                self.map(pkt)
            }
            _ => Packet::None,
        }
    }
}

impl Codec {
    fn map(&self, mut pkt: Packet) -> Packet {
        match &mut pkt {
            Packet::Ethernet(Ethernet { dir, eth, .. }) => {
                if self.mac == eth.source {
                    *dir = pcap::Direction::Out;
                    pkt
                } else if self.mac == eth.destination {
                    *dir = pcap::Direction::In;
                    pkt
                } else {
                    Packet::None
                }
            }
            Packet::None => Packet::None,
        }
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Packet::Ethernet(pkt) if pkt.dir == pcap::Direction::In => {
                write!(f, "<- {} {}", pkt.eth.source, pkt.eth.ethertype)
            }
            Packet::Ethernet(pkt) if pkt.dir == pcap::Direction::Out => {
                write!(f, "-> {} {}", pkt.eth.destination, pkt.eth.ethertype)
            }
            Packet::Ethernet(_) => Ok(()),
            Packet::None => Ok(()),
        }
    }
}

struct Ethernet {
    ts: NaiveDateTime,
    dir: pcap::Direction,
    eth: pnet::packet::ethernet::Ethernet,
}

impl fmt::Display for Ethernet {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let ts = self.ts.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
        write!(
            f,
            "{} {} <- {} {}",
            ts, self.eth.destination, self.eth.source, self.eth.ethertype
        )
    }
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
