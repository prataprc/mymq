use crate::{Opt, Result, SubCommand};
use mymq::{netw, util};

pub fn run(opts: Opt) -> Result<()> {
    let ifs = match &opts.subcmd {
        SubCommand::List { ifs } => ifs,
        _ => unreachable!(),
    };

    match ifs.as_str() {
        "all" => {
            let interfaces = netw::interfaces();
            util::make_table(&interfaces).print_tty(!opts.force_color);
        }
        ifs => println!("unknown interface {}", ifs),
    }

    Ok(())
}
