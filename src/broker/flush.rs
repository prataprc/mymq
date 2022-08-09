use log::{debug, error, info, trace, warn};

use std::{thread, time};

use crate::broker::thread::{Rx, Thread, Threadable, Tx};
use crate::broker::{socket, AppTx, Config, QueueStatus, Socket, SLEEP_10MS};

use crate::{v5, ToJson};
use crate::{Error, ErrorKind, Result};

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
    // Held by Cluster
    Handle(Thread<Flusher, Request, Result<Response>>),
    // Held by each shard.
    Tx(Tx<Request, Result<Response>>),
    // Thread
    Main(RunLoop),
    // Held by Cluser, replacing both Handle and Main.
    Close(FinState),
}

struct RunLoop {
    /// Statistics
    n_flush_conns: usize,
    n_pkts: usize,
    n_bytes: usize,

    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
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
            self.n_flush_conns,
            "n_pkts",
            self.n_pkts,
            "n_bytes",
            self.n_bytes
        )
    }
}

impl Default for Flusher {
    fn default() -> Flusher {
        let config = Config::default();
        let mut def = Flusher {
            name: config.name.clone(),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_thrd) => debug!("{} drop ...", self.prefix),
            Inner::Tx(_tx) => debug!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl ToJson for Flusher {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {} }}"),
            "sock_mqtt_flush_timeout",
            self.config.sock_mqtt_flush_timeout,
            "mqtt_max_packet_size",
            self.config.mqtt_max_packet_size
        )
    }

    fn to_stats_json(&self) -> String {
        "{{}}".to_string()
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
            inner: Inner::Main(RunLoop {
                n_flush_conns: 0,
                n_pkts: 0,
                n_bytes: 0,
                app_tx,
            }),
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

        Ok(flush)
    }

    pub fn to_tx(&self, who: &str) -> Self {
        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        let mut flush = Flusher {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner,
        };
        flush.prefix = flush.prefix();

        debug!("{} cloned for {}", flush.prefix, who);
        flush
    }
}

pub enum Request {
    FlushConnection { socket: Socket, err: Option<Error> },
    Close,
}

pub enum Response {
    Ok,
    FlushStats(socket::Stats),
}

pub struct FlushConnectionArgs {
    pub socket: Socket,
    pub err: Option<Error>,
}

// calls to interface with flusher-thread.
impl Flusher {
    pub fn flush_connection(&self, args: FlushConnectionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => thrd.request(Request::FlushConnection {
                socket: args.socket,
                err: args.err,
            })??,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn close_wait(mut self) -> Flusher {
        use std::mem;

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

impl Threadable for Flusher {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::{thread::get_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        info!("{} spawn thread config {}", self.prefix, self.to_config_json());

        'outer: loop {
            let mut status = get_requests(&self.prefix, &rx, CONTROL_CHAN_SIZE);
            let reqs = status.take_values();
            debug!("{} process {} requests closed:false", self.prefix, reqs.len());

            for req in reqs.into_iter() {
                match req {
                    (req @ FlushConnection { .. }, Some(tx)) => {
                        self.incr_n_flush_conns();
                        match self.handle_flush_connection(req) {
                            Response::FlushStats(flush_stats) => {
                                self.incr_stats(&flush_stats);
                                err!(IPCFail, try: tx.send(Ok(Response::Ok))).ok();
                            }
                            _ => unreachable!(),
                        }
                    }
                    (Close, Some(tx)) => {
                        let resp = self.handle_close();
                        err!(IPCFail, try: tx.send(Ok(resp))).ok();
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
    fn handle_flush_connection(&self, req: Request) -> Response {
        use crate::broker::socket::Stats as SockStats;
        use crate::packet::send_disconnect;

        let now = time::Instant::now();
        let max_size = self.config.mqtt_max_packet_size;
        let flush_timeout = self.config.sock_mqtt_flush_timeout;

        let (mut socket, conn_err) = match req {
            Request::FlushConnection { socket, err } => (socket, err),
            _ => unreachable!(),
        };
        let raddr = socket.conn.peer_addr().unwrap();
        info!("{} flush connection for {:?} {}", self.prefix, raddr, *socket.client_id);

        let timeout = now + time::Duration::from_secs(flush_timeout as u64);
        let mut stats = SockStats::default();
        let sock_stats = loop {
            let mut status = socket.wt.miot_rx.try_recvs(&self.prefix);
            socket.wt.packets.extend(status.take_values().into_iter());

            let sock_stats = match socket.flush_packets(&self.prefix, &self.config) {
                (QueueStatus::Ok(_), sock_stats) => sock_stats,
                (QueueStatus::Block(_), sock_stats) if time::Instant::now() > timeout => {
                    error!("{} give up flush_packets after {:?}", self.prefix, timeout);
                    break sock_stats;
                }
                (QueueStatus::Block(_), sock_stats) => {
                    thread::sleep(SLEEP_10MS);
                    sock_stats
                }
                (QueueStatus::Disconnected(_), sock_stats) => {
                    warn!("{} stop flush, socket disconnected", self.prefix);
                    break sock_stats;
                }
            };

            // after flushing the packets see, the source `miot_tx` is disconnected.
            // we will have to run this loop under `miot_tx` has disconnected or
            // downstream socket has disconnected, or timeout exceeds.

            if let QueueStatus::Disconnected(_) = status {
                break sock_stats;
            }

            stats.update(&sock_stats);
        };
        stats.update(&sock_stats);

        let timeout = now + time::Duration::from_secs(flush_timeout as u64);
        let code = match conn_err {
            Some(err) => v5::DisconnReasonCode::try_from(err.code() as u8).unwrap(),
            None => v5::DisconnReasonCode::NormalDisconnect,
        };

        if let Ok(()) =
            send_disconnect(&self.prefix, code, &mut socket.conn, timeout, max_size)
        {
            info!("{} flushed {} and disconnected", self.prefix, stats.to_json())
        }

        Response::FlushStats(stats)
    }

    fn handle_close(&mut self) -> Response {
        use std::mem;

        let run_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => run_loop,
            Inner::Close(_) => return Response::Ok,
            _ => unreachable!(),
        };
        info!("{} closing flusher", self.prefix);

        mem::drop(run_loop.app_tx);

        let fin_state = FinState {
            n_flush_conns: run_loop.n_flush_conns,
            n_pkts: run_loop.n_pkts,
            n_bytes: run_loop.n_bytes,
        };

        info!("{} stats {}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();
        Response::Ok
    }
}

impl Flusher {
    fn incr_n_flush_conns(&mut self) {
        match &mut self.inner {
            Inner::Main(RunLoop { n_flush_conns, .. }) => *n_flush_conns += 1,
            _ => unreachable!(),
        }
    }

    fn incr_stats(&mut self, stats: &socket::Stats) {
        match &mut self.inner {
            Inner::Main(RunLoop { n_pkts, n_bytes, .. }) => {
                *n_pkts += stats.items;
                *n_bytes += stats.bytes;
            }
            _ => unreachable!(),
        }
    }

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

    #[allow(dead_code)]
    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}
