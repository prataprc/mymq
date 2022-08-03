use log::{debug, error, info};

use std::{mem, sync::mpsc, thread, time};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{AppTx, Cluster, Shard, SLEEP_10MS};

use crate::Config;
use crate::{Error, ErrorKind, Result};

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
    /// Born Instant
    born: time::Instant,
    /// Ticker count
    ticker_count: usize,
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,
    /// Shard-Tx handle to all shards active on this node.
    active_shards: Vec<Shard>,
    /// Shard-Tx handle to all replica shards on this node.
    replica_shards: Vec<Shard>,

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

impl Default for Ticker {
    fn default() -> Ticker {
        let config = Config::default();
        let mut def = Ticker {
            name: format!("{}-ticker-init", config.name),
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
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_thrd) => info!("{} invalid drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => info!("{} drop ...", self.prefix),
        }
    }
}

pub struct SpawnArgs {
    pub cluster: Box<Cluster>,
    pub active_shards: Vec<Shard>,
    pub replica_shards: Vec<Shard>,
    pub app_tx: AppTx,
}

impl Ticker {
    pub fn from_config(config: Config) -> Result<Ticker> {
        let mut val = Ticker {
            name: format!("{}-ticker-init", config.name),
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, args: SpawnArgs) -> Result<Ticker> {
        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "ticker can be spawned only in init-state ")?;
        }

        let mut ticker = Ticker {
            name: format!("{}-ticker-main", self.config.name),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                born: time::Instant::now(),
                ticker_count: 0,
                cluster: args.cluster,
                active_shards: args.active_shards,
                replica_shards: args.replica_shards,
                app_tx: args.app_tx,
            }),
        };
        ticker.prefix = ticker.prefix();
        let thrd = Thread::spawn(&self.prefix, ticker);

        let mut ticker = Ticker {
            name: format!("{}-ticker-handle", self.config.name),
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
        'outer: loop {
            thread::sleep(SLEEP_10MS);

            loop {
                match rx.try_recv() {
                    Ok((Request::Close, Some(tx))) => {
                        err!(IPCFail, try: tx.send(Ok(Response::Ok))).ok();
                        break 'outer;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break 'outer,
                    Ok(_) => unreachable!(),
                }
            }

            let RunLoop {
                cluster,
                ticker_count,
                active_shards,
                replica_shards,
                ..
            } = match &mut self.inner {
                Inner::Main(run_loop) => run_loop,
                _ => unreachable!(),
            };

            *ticker_count += 1;

            cluster.wake();
            active_shards.iter().for_each(|shard| shard.wake());
            replica_shards.iter().for_each(|shard| shard.wake());
        }

        let inner = mem::replace(&mut self.inner, Inner::Init);
        let RunLoop { born, ticker_count, .. } = match inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let fin_state = FinState { born, ticker_count, dead: time::Instant::now() };
        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));

        self
    }
}

impl Ticker {
    fn prefix(&self) -> String {
        format!("{}-ticker", self.name)
    }

    #[allow(dead_code)]
    fn as_app_tx(&self) -> &mpsc::SyncSender<String> {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}
