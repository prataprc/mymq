use log::info;
use structopt::StructOpt;

use std::{io, path, result};

mod dump;
mod list;
mod start;

#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "config")]
    config_loc: Option<path::PathBuf>,

    #[structopt(short = "v")]
    v: bool,

    #[structopt(long = "vv")]
    vv: bool,

    #[structopt(long = "log-mod", default_value = "")]
    log_mod: String,

    #[structopt(long = "force-color")]
    force_color: bool,

    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SubCommand {
    Start {
        #[structopt(long = "name", default_value = "mymqd")]
        name: String,

        #[structopt(long = "port", default_value = "1883")]
        port: u16,

        #[structopt(long = "num-shards", default_value = "1")]
        num_shards: u32,
    },
    Dump {
        #[structopt(short = "w")]
        write_file: Option<path::PathBuf>,

        #[structopt(short = "a")]
        append_file: Option<path::PathBuf>,

        #[structopt(short = "r")]
        read_file: Option<path::PathBuf>,

        #[structopt(short = "t", default_value = "0")]
        time: u64,

        #[structopt(long = "precis", default_value = "micro")]
        precision: String,

        #[structopt(long = "promisc")]
        promisc: bool,

        #[structopt(long = "devices")]
        devices: bool,

        #[structopt(long = "inp")]
        inp: bool,

        #[structopt(long = "out")]
        out: bool,

        #[structopt(long = "eth")]
        eth: bool,

        #[structopt(long = "ip")]
        ip: bool,

        #[structopt(long = "tcp")]
        tcp: bool,

        device: Option<String>,
    },
    List {
        #[structopt(long = "ifs", default_value = "all")]
        ifs: String,
    },
}

pub type Result<T> = result::Result<T, String>;

fn main() {
    let opts = parse_cmd_line();

    setup_logging(&opts);
    info!("verbosity level {:?}", opts.to_verbosity());

    let res = match &opts.subcmd {
        SubCommand::Start { .. } => start::run(opts),
        SubCommand::Dump { .. } => dump::run(opts),
        SubCommand::List { .. } => list::run(opts),
    };

    res.map_err(|e| println!("error: {}", e)).ok();
}

fn parse_cmd_line() -> Opt {
    Opt::from_args()
}

use env_logger::{fmt::Target, Builder, WriteStyle};
use log::Level;
use std::io::Write;
fn setup_logging(opts: &Opt) {
    let verbosity = opts.to_verbosity();
    let opts = opts.clone();
    Builder::from_default_env()
        .parse_default_env()
        .target(Target::Stdout)
        .format(move |f, r| {
            let file = r.file().clone().unwrap();
            if file.len() > 0 && !file.contains(&opts.log_mod) {
                return Ok(());
            }

            let target = r.target();
            match target {
                "0" | "1" | "2" if target <= verbosity.as_str() => log_format(f, r),
                "0" | "1" | "2" => Ok(()),
                _ => log_format(f, r),
            }
        })
        .write_style(WriteStyle::Auto)
        .init();
}

use env_logger::fmt::{Color, Formatter};
fn log_format(f: &mut Formatter, r: &log::Record<'_>) -> io::Result<()> {
    let mut level_style = f.style();
    match r.level() {
        Level::Error => level_style.set_color(Color::Red).set_bold(false),
        Level::Warn => level_style.set_color(Color::Yellow).set_bold(false),
        Level::Info => level_style.set_color(Color::Blue).set_bold(false),
        Level::Debug => level_style.set_color(Color::Magenta).set_bold(false),
        Level::Trace => level_style.set_color(Color::Cyan).set_bold(false),
    };
    let mut mod_style = f.style();
    mod_style.set_color(Color::Green).set_bold(false);
    let file = {
        let file = path::Path::new(r.file().clone().unwrap()).file_stem().unwrap();
        file.to_str().unwrap()
    };
    let loc = mod_style.value(format!("{}:{}", file, r.line().unwrap()));
    writeln!(
        f,
        "{} [{:5}] [{:>13}] {}",
        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%Z"),
        level_style.value(r.level()),
        loc,
        r.args()
    )
}

impl Opt {
    fn to_verbosity(&self) -> String {
        if self.v {
            "1"
        } else if self.vv {
            "2"
        } else {
            "0"
        }
        .to_string()
    }
}
