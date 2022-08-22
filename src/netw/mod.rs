pub use pnet_datalink::interfaces;

#[cfg(feature = "prettytable-rs")]
impl crate::util::PrettyRow for pnet_datalink::NetworkInterface {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        use prettytable::{cell, row};

        row![Fy => "Name", "Index", "MAC", "IP", "Flags", "Description"]
    }

    fn to_row(&self) -> prettytable::Row {
        use prettytable::{cell, row};

        let ips = self
            .ips
            .iter()
            .map(|val| val.to_string())
            .collect::<Vec<String>>()
            .join("\n");

        row![
            self.name,
            self.index,
            self.mac.as_ref().map(|val| val.to_string()).unwrap_or("-".to_string()),
            ips,
            format!("0x{:08x}", self.flags),
            self.description,
        ]
    }
}

#[cfg(feature = "prettytable-rs")]
impl crate::util::PrettyRow for pcap::Device {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        use prettytable::{cell, row};

        row![Fy => "Name", "IfFlags", "Status", "Address", "Description"]
    }

    fn to_row(&self) -> prettytable::Row {
        use prettytable::{cell, row};

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
