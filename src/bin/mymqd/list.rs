use mymq::{netw, util};
use structopt::StructOpt;

use crate::{Opt, Result, SubCommand};

#[derive(Clone, StructOpt)]
pub struct List {
    #[structopt(long = "ifs", default_value = "all")]
    ifs: String,
}

pub fn run(opts: Opt) -> Result<()> {
    let list = match &opts.subcmd {
        SubCommand::List(list) => list.clone(),
        _ => unreachable!(),
    };

    match list.ifs.as_str() {
        "all" => {
            let interfaces = netw::interfaces();
            util::make_table(&interfaces).print_tty(!opts.force_color);
        }
        ifs => println!("unknown interface {}", ifs),
    }

    Ok(())
}
