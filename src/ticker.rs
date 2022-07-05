use log::{debug, error, info};

use std::{mem, sync::mpsc, thread, time};

use crate::thread::{Rx, Thread, Threadable};
use crate::{AppTx, Config, Shard, SLEEP_10MS};
use crate::{Error, ErrorKind, Result};

pub struct Ticker {
    pub name: String,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Thread<Ticker, Request, Result<Response>>),
    Main(RunLoop),
    Close(FinState),
}

pub struct RunLoop {
    /// Born Instant
    born: time::Instant,
    /// Ticker count
    ticker_count: usize,
    /// Shard-Tx handle to all shards hosted in this node.
    shards: Vec<Shard>,

    /// Back channel to communicate with application.
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

    pub fn spawn(self, shards: Vec<Shard>, app_tx: AppTx) -> Result<Ticker> {
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
                shards,
                app_tx,
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
        loop {
            thread::sleep(SLEEP_10MS);

            let exit = loop {
                match rx.try_recv() {
                    Ok((Request::Close, Some(tx))) => {
                        allow_panic!(&self, tx.send(Ok(Response::Ok)));
                        break true;
                    }
                    Err(mpsc::TryRecvError::Empty) => break false,
                    Err(mpsc::TryRecvError::Disconnected) => break true,
                    Ok(_) => unreachable!(),
                }
            };

            if exit {
                break;
            }

            let RunLoop { ticker_count, shards, .. } = match &mut self.inner {
                Inner::Main(run_loop) => run_loop,
                _ => unreachable!(),
            };

            let shards0 = mem::replace(shards, Vec::default());

            let mut shards1 = vec![];
            for shard in shards0.into_iter() {
                match shard.wake() {
                    Ok(()) => shards1.push(shard),
                    Err(err) => {
                        error!(
                            "{} fail waking shard {}: {}",
                            self.prefix, shard.shard_id, err
                        )
                    }
                }
            }

            let _empty = mem::replace(shards, shards1);
            *ticker_count += 1;
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

    fn as_app_tx(&self) -> &mpsc::SyncSender<String> {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}
