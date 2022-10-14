//! Ticker threading model.
//!
//! ```ignore
//!                        spawn()
//! from_config() -> Init -----+----> Handle
//!                            |
//!                            |
//!                            V
//!                           Main
//! ```

use log::{debug, error, info, trace};

use std::{fmt, mem, result, sync::mpsc, thread, time};

#[allow(unused_imports)]
use crate::broker::{Cluster, Shard};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{AppTx, ClusterAPI, Config, ShardAPI};
use crate::{Error, ErrorKind, Result};
use crate::{ToJson, SLEEP_10MS};

/// Type implement a periodic ticker and wake up other threads.
///
/// Periodically wake up other threads like like [Cluster] and [Shard]. This type is
/// threadable and singleton.
pub struct Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    pub name: String,
    prefix: String,
    config: Config,

    inner: Inner<C, S>,
}

pub enum Inner<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    Init,
    Main(RunLoop<C, S>),                                     // Thread
    Handle(Thread<Ticker<C, S>, Request, Result<Response>>), // Held by Cluster
    Close(FinState), // Held by Cluser, replacing both Handle and Main.
}

impl<C, S> fmt::Debug for Inner<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Ticker::Inner::Init"),
            Inner::Main(_) => write!(f, "Ticker::Inner::Main"),
            Inner::Handle(_) => write!(f, "Ticker::Inner::Handle"),
            Inner::Close(_) => write!(f, "Ticker::Inner::Close"),
        }
    }
}

pub struct RunLoop<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    /// Tx-handle to send messages to cluster.
    cluster: Box<C>,
    /// Shard-Tx handle to all master shards.
    masters: Vec<S>,
    /// Shard-Tx handle to all replica shards.
    replicas: Vec<S>,

    /// Born Instant
    born: time::Instant,
    /// Ticker count
    ticker_count: usize,

    /// Back channel communicate with application.
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

impl<C, S> Default for Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    fn default() -> Ticker<C, S> {
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

impl<C, S> Drop for Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
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

impl<C, S> ToJson for Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
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

pub struct SpawnArgs<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    pub cluster: Box<C>,
    pub masters: Vec<S>,
    pub replicas: Vec<S>,
    pub app_tx: AppTx,
}

impl<C, S> Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    pub fn from_config(config: Config) -> Result<Ticker<C, S>> {
        let mut val = Ticker {
            name: config.name.clone(),
            prefix: String::default(),
            config: config.clone(),

            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, args: SpawnArgs<C, S>) -> Result<Ticker<C, S>> {
        let mut ticker = Ticker {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),

            inner: Inner::Main(RunLoop {
                cluster: args.cluster,
                masters: args.masters,
                replicas: args.replicas,

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

    // calls to interface with ticker-thread.
    pub fn close_wait(mut self) -> Ticker<C, S> {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(thrd) => {
                app_fatal!(self, thrd.request(Request::Close).flatten());
                thrd.close_wait()
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

pub enum Request {
    Close,
}

pub enum Response {
    Ok,
}

impl<C, S> Threadable for Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
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

            let RunLoop { cluster, ticker_count, masters, replicas, .. } =
                match &mut self.inner {
                    Inner::Main(run_loop) => run_loop,
                    _ => unreachable!(),
                };

            *ticker_count += 1;

            if let Err(err) = cluster.wake() {
                error!("{} waking cluster err:{}", self.prefix, err);
            }
            for shard in masters.iter() {
                if let Err(err) = shard.wake() {
                    error!("{} waking shard err:{}", self.prefix, err);
                }
            }
            for shard in replicas.iter() {
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

impl<C, S> Ticker<C, S>
where
    C: 'static + Send + ClusterAPI,
    S: 'static + Send + ShardAPI,
{
    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_) => "hndl",
            Inner::Main(_) => "main",
            Inner::Close(_) => "close",
        };
        format!("<t:{}:{}>", self.name, state)
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}
