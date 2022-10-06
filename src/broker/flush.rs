//! Flusher threading model.
//!
//! ```
//!                        spawn()            to_tx()
//! from_config() -> Init -----+----> Handle ---------> Tx ----+
//!                            |                        ^      | to_tx()
//!                            |                        |      |
//!                            V                        +------+
//!                           Main
//! ```

use log::{debug, info, trace, warn};

use std::{fmt, mem, result, thread};

use crate::broker::thread::{Rx, Thread, Threadable, Tx};
use crate::broker::{AppTx, Config, PQueue};
use crate::{Error, ReasonCode, Result};
use crate::{QueueStatus, ToJson, SLEEP_10MS};

type ThreadRx = Rx<Request, Result<Response>>;

/// Type monitor closing connection, flushes the packets and finally send DISCONNECT.
///
/// This type is threadable and singleton.
pub struct Flusher {
    pub name: String,
    prefix: String,
    config: Config,

    inner: Inner,
}

enum Inner {
    Init,
    Main(RunLoop),                                      // Thread
    Handle(Thread<Flusher, Request, Result<Response>>), // Held by Cluster
    Tx(Tx<Request, Result<Response>>),                  // Held by each shard.
    Close(FinState), // Held by Cluster, replacing both Handle and Main.
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Flusher::Inner::Init"),
            Inner::Handle(_) => write!(f, "Flusher::Inner::Handle"),
            Inner::Main(_) => write!(f, "Flusher::Inner::Main"),
            Inner::Tx(_) => write!(f, "Flusher::Inner::Tx"),
            Inner::Close(_) => write!(f, "Flusher::Inner::Close"),
        }
    }
}

struct RunLoop {
    /// Statistics
    stats: Stats,
    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
    stats: Stats,
}

#[derive(Clone, Copy, Default)]
pub struct Stats {
    /// Number of connections flushed downstream and disconnected.
    pub n_flush_conns: usize,
    /// Number of packets flushed in all connections.
    pub n_pkts: usize,
    /// Number of bytes flushed in all connections.
    pub n_bytes: usize,
}

impl FinState {
    fn to_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {}, {:?}: {} }}"),
            "n_flush_conns",
            self.stats.n_flush_conns,
            "n_pkts",
            self.stats.n_pkts,
            "n_bytes",
            self.stats.n_bytes
        )
    }
}

impl Default for Flusher {
    fn default() -> Flusher {
        let config = Config::default();
        let mut val = Flusher {
            name: config.name.clone(),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        val.prefix = val.prefix();
        val
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_thrd) => debug!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Tx(_tx) => debug!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl ToJson for Flusher {
    fn to_config_json(&self) -> String {
        format!(
            concat!(r#"{{ {:?}: {:?}, {:?}: {} }}"#),
            "name", self.config.name, "flush_timeout", self.config.flush_timeout,
        )
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Close(fin_state) => fin_state.to_json(),
            _ => "{{}}".to_string(),
        }
    }
}

impl Flusher {
    /// Create a flusher from configuration. Flusher shall be in `Init` state. To start
    /// this flusher thread call [Flusher::spawn].
    pub fn from_config(config: &Config) -> Result<Flusher> {
        let mut val = Flusher {
            name: config.name.clone(),
            prefix: String::default(),
            config: config.clone(),

            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, app_tx: AppTx) -> Result<Flusher> {
        let mut flush = Flusher {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),

            inner: Inner::Main(RunLoop { stats: Stats::default(), app_tx }),
        };
        flush.prefix = flush.prefix();
        let thrd = Thread::spawn(&self.prefix, flush);

        let mut flush = Flusher {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        flush.prefix = flush.prefix();

        debug!("{} handle to flusher thread", flush.prefix);

        Ok(flush)
    }

    pub fn to_tx(&self, who: &str) -> Self {
        let mut flush = Flusher {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),

            inner: match &self.inner {
                Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
                Inner::Tx(tx) => Inner::Tx(tx.clone()),
                _ => unreachable!(),
            },
        };
        flush.prefix = flush.prefix();

        debug!("{} cloned for {}", flush.prefix, who);

        flush
    }
}

pub enum Request {
    FlushConnection { pq: PQueue, err: Option<Error> },
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with flusher-thread.
impl Flusher {
    pub fn flush_connection(&self, pq: PQueue, err: Option<Error>) {
        let req = Request::FlushConnection { pq, err };
        match &self.inner {
            Inner::Handle(thrd) => app_fatal!(self, thrd.request(req).flatten()),
            Inner::Tx(tx) => app_fatal!(self, tx.request(req).flatten()),
            _ => unreachable!(),
        };
    }

    pub fn close_wait(mut self) -> Flusher {
        match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Handle(thrd) => {
                app_fatal!(self, thrd.request(Request::Close).flatten());
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

impl Threadable for Flusher {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::{thread::get_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        info!("{} spawn thread config:{}", self.prefix, self.to_config_json());

        'outer: loop {
            let mut status = get_requests(&self.prefix, &rx, CONTROL_CHAN_SIZE);
            let reqs = status.take_values();
            trace!("{} n:{} requests processed", self.prefix, reqs.len());

            for req in reqs.into_iter() {
                match req {
                    (req @ FlushConnection { .. }, Some(tx)) => {
                        let resp = self.handle_flush_connection(req);
                        app_fatal!(self, tx.send(Ok(resp)));
                    }
                    (Close, Some(tx)) => {
                        app_fatal!(self, tx.send(Ok(self.handle_close())));
                        break 'outer;
                    }

                    (_, _) => unreachable!(),
                }
            }

            match status {
                QueueStatus::Ok(_) | QueueStatus::Block(_) => (),
                QueueStatus::Disconnected(_) => break,
            };
        }

        match &self.inner {
            Inner::Main(_) => self.handle_close(),
            Inner::Close(_) => Response::Ok,
            _ => unreachable!(),
        };

        info!("{} thread exit", self.prefix);
        self
    }
}

impl Flusher {
    fn handle_flush_connection(&mut self, req: Request) -> Response {
        let (mut pq, conn_err) = match req {
            Request::FlushConnection { pq, err } => (pq, err),
            _ => unreachable!(),
        };

        let (raddr, client_id) = (pq.peer_addr(), pq.to_client_id());
        info!(
            "{} raddr:{} client_id:{} flushing connection err:{:?}",
            self.prefix, raddr, *client_id, conn_err
        );

        self.incr_n_flush_conns();

        let (mut items, mut bytes) = (0_usize, 0_usize);
        loop {
            let (status, a, b) = pq.write_packets(&self.prefix);
            items += a;
            bytes += b;

            match status {
                QueueStatus::Ok(_) => {
                    break;
                }
                QueueStatus::Block(_) => {
                    thread::sleep(SLEEP_10MS);
                }
                QueueStatus::Disconnected(_) => {
                    warn!("{} stop flush, socket disconnected", self.prefix);
                    break;
                }
            }
        }
        self.incr_stats(items, bytes);

        pq.as_mut_socket().disconnect(&self.prefix, ReasonCode::Success);

        info!(
            "{} raddr:{} client_id:{} items:{} bytes:{} flushed and DISCONNECT",
            self.prefix, raddr, client_id, items, bytes
        );

        Response::Ok
    }

    fn handle_close(&mut self) -> Response {
        let run_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => run_loop,
            Inner::Close(_) => return Response::Ok,
            _ => unreachable!(),
        };
        info!("{} closing flusher", self.prefix);

        let fin_state = FinState { stats: run_loop.stats };
        info!("{} stats:{}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();
        Response::Ok
    }
}

impl Flusher {
    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_) => "hndl",
            Inner::Tx(_) => "tx",
            Inner::Main(_) => "main",
            Inner::Close(_) => "close",
        };
        format!("<f:{}:{}>", self.name, state)
    }

    fn incr_n_flush_conns(&mut self) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_flush_conns += 1,
            _ => unreachable!(),
        }
    }

    fn incr_stats(&mut self, items: usize, bytes: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => {
                stats.n_pkts += items;
                stats.n_bytes += bytes;
            }
            _ => unreachable!(),
        }
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}
