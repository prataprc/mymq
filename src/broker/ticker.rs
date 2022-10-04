use log::{debug, error, info, trace};

use std::{mem, sync::mpsc, thread, time};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{AppTx, Cluster, Config, Shard};
use crate::{Error, ErrorKind, Result, ToJson};
use crate::{ToJson, SLEEP_10MS};

/// Type implement a periodic ticker and wake up other threads.
///
/// Periodically wake up other threads like like [Cluster] and [Shard]. This type is
/// threadable and singleton.
pub struct Ticker {
    pub name: String,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    // Held by Cluster
    Handle(Thread<Ticker, Request, Result<Response>>),
    // Thread
    Main(RunLoop),
    // Held by Cluser, replacing both Handle and Main.
    Close(FinState),
}

pub struct RunLoop {
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,
    /// Shard-Tx handle to all shards active and replica shards on this node.
    shards: Vec<Shard>,

    /// Born Instant
    born: time::Instant,
    /// Ticker count
    ticker_count: usize,

    /// Back channel communicate with application.
    #[allow(dead_code)]
    app_tx: AppTx,
}

pub struct FinState {
    /// Born Instant
    pub born: time::Instant,
    /// Ticker count
    pub ticker_count: usize,
    /// Dead Instant
    pub dead: time::Instant,
}

impl FinState {
    fn to_json(&self) -> String {
        let born = (time::UNIX_EPOCH.elapsed().unwrap() - self.born.elapsed()).as_secs();
        let dead = (time::UNIX_EPOCH.elapsed().unwrap() - self.dead.elapsed()).as_secs();

        format!(
            concat!("{{ {:?}: {}, {:?}: {}, {:?}: {} }}"),
            "born", born, "ticket_count", self.ticker_count, "dead", dead
        )
    }
}

impl Default for Ticker {
    fn default() -> Ticker {
        let config = Config::default();
        let mut def = Ticker {
            name: config.name.clone(),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_thrd) => debug!("{} invalid drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl ToJson for Ticker {
    fn to_config_json(&self) -> String {
        "".to_string()
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Close(fin_state) => fin_state.to_json(),
            _ => unreachable!(),
        }
    }
}

pub struct SpawnArgs {
    pub cluster: Box<Cluster>,
    pub shards: Vec<Shard>,
    pub app_tx: AppTx,
}

impl Ticker {
    pub fn from_config(config: Config) -> Result<Ticker> {
        let mut val = Ticker {
            name: config.name.clone(),
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, args: SpawnArgs) -> Result<Ticker> {
        let mut ticker = Ticker {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                cluster: args.cluster,
                shards: args.shards,

                born: time::Instant::now(),
                ticker_count: 0,

                app_tx: args.app_tx,
            }),
        };
        ticker.prefix = ticker.prefix();
        let thrd = Thread::spawn(&self.prefix, ticker);

        let mut ticker = Ticker {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        ticker.prefix = ticker.prefix();

        Ok(ticker)
    }
}

pub enum Request {
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with ticker-thread.
impl Ticker {
    pub fn close_wait(mut self) -> Ticker {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::Close).ok();
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

impl Threadable for Ticker {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Request, Result<Response>>) -> Self {
        info!("{} spawn thread, staring 10ms ticker", self.prefix);

        'outer: loop {
            thread::sleep(SLEEP_10MS);

            loop {
                match rx.try_recv() {
                    Ok((Request::Close, Some(tx))) => {
                        info!("{} closing ticker", self.prefix);
                        err!(IPCFail, try: tx.send(Ok(Response::Ok))).ok();
                        break 'outer;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break 'outer,
                    Ok(_) => unreachable!(),
                }
            }

            let RunLoop { cluster, ticker_count, shards, .. } = match &mut self.inner {
                Inner::Main(run_loop) => run_loop,
                _ => unreachable!(),
            };

            *ticker_count += 1;

            if let Err(err) = cluster.wake() {
                error!("{} waking cluster err:{}", self.prefix, err);
            }
            for shard in shards.iter() {
                if let Err(err) = shard.wake() {
                    error!("{} waking shard err:{}", self.prefix, err);
                }
            }
        }

        let inner = mem::replace(&mut self.inner, Inner::Init);
        let RunLoop { born, ticker_count, .. } = match inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let fin_state = FinState { born, ticker_count, dead: time::Instant::now() };
        info!("{} stats:{}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();

        info!("{} thread exit", self.prefix);
        self
    }
}

impl Ticker {
    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_) => "hndl",
            Inner::Main(_) => "main",
            Inner::Close(_) => "close",
        };
        format!("<t:{}:{}>", self.name, state)
    }
}
