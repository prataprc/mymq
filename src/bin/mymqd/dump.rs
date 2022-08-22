use prettytable::{cell, row};

use std::{path, time};

use crate::{make_table, Opt, PrettyRow, Result, SubCommand};

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
                    device,
                };
                Ok(val)
            }
            _ => unreachable!(),
        }
    }
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
    let mut capture = {
        let capture = pcap::Capture::<pcap::Inactive>::from_device(find_device(
            &dump.device.unwrap(),
        )?)
        .map_err(|e| e.to_string())?
        .timeout(1000 /*ms*/)
        .promisc(dump.promisc)
        .precision(dump.precision);
        capture.open().map_err(|e| e.to_string())?
    };

    let mut save_file = match dump.write_file {
        Some(wf) => Some(capture.savefile(&wf).map_err(|e| e.to_string())?),
        None => match dump.append_file {
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

    let deadline = time::Instant::now() + time::Duration::from_secs(dump.time);
    loop {
        if dump.time > 0 && time::Instant::now() > deadline {
            break;
        }

        match capture.next_packet() {
            Ok(pkt) => {
                println!("pkt {}", pkt.data.len());
                save_file.as_mut().map(|f| f.write(&pkt));
            }
            Err(err) => println!("capture error: {}", err),
        }
    }

    match &mut save_file {
        Some(file) => file.flush().map_err(|e| e.to_string())?,
        None => (),
    }

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
    make_table(&devices).print_tty(!opts.force_color);

    Ok(())
}

fn list_connected_devices(opts: &Opt) -> Result<()> {
    let devices: Vec<pcap::Device> = pcap::Device::list()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|d| d.flags.connection_status == pcap::ConnectionStatus::Connected)
        .collect();
    make_table(&devices).print_tty(!opts.force_color);

    Ok(())
}

impl PrettyRow for pcap::Device {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        row![Fy => "Name", "IfFlags", "Status", "Address", "Description"]
    }

    fn to_row(&self) -> prettytable::Row {
        let addresses = self
            .addresses
            .iter()
            .map(|addr| pretty_print_address(addr))
            .collect::<Vec<String>>()
            .as_slice()
            .join("\n--------\n");

        row![
            self.name,
            format!("{:?}", self.flags.if_flags),
            format!("{:?}", self.flags.connection_status),
            addresses,
            self.desc.as_ref().map(|val| val.as_str()).unwrap_or("-")
        ]
    }
}

fn pretty_print_address(addr: &pcap::Address) -> String {
    let mut items = vec![addr.addr.to_string()];
    addr.netmask.as_ref().map(|val| items.push(val.to_string()));
    addr.broadcast_addr.as_ref().map(|val| items.push(val.to_string()));
    addr.dst_addr.as_ref().map(|val| items.push(val.to_string()));
    items.join("\n")
}

fn into_precision(val: &str) -> Result<pcap::Precision> {
    match val {
        "micro" => Ok(pcap::Precision::Micro),
        "nano" => Ok(pcap::Precision::Nano),
        _ => Err(format!("invalid precision {}", val)),
    }
}
