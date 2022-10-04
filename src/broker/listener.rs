use log::{debug, error, info, trace};
use mio::event::Events;

use std::{fmt, io, mem, net, result, sync::Arc, time};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{AppTx, Cluster, Config};
use crate::{Error, ErrorKind, Result};
use crate::{Protocol, QueueStatus, ToJson};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;

/// Type binds to network port, like MQTT, and listens for incoming connection.
///
/// This type is threadable and singleton.
pub struct Listener {
    /// Human readable name for this mio thread.
    name: String,
    prefix: String,
    config: Config,
    proto: Protocol,
    inner: Inner,
}

enum Inner {
    Init,
    // Held by Cluster
    Handle(Arc<mio::Waker>, Thread<Listener, Request, Result<Response>>),
    // Thread
    Main(RunLoop),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState),
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Listener::Inner::Init"),
            Inner::Handle(_, _) => write!(f, "Listener::Inner::Handle"),
            Inner::Main(_) => write!(f, "Listener::Inner::Main"),
            Inner::Close(_) => write!(f, "Listener::Inner::Close"),
        }
    }
}

struct RunLoop {
    /// Mio poller for asynchronous handling, aggregate events from listener and
    /// thread-waker.
    poll: mio::Poll,
    /// TCP listener listening on `port`.
    listener: mio::net::TcpListener,
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,

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
    /// Number of times poll was woken up.
    pub n_polls: usize,
    /// Number events received via mio-poll.
    pub n_events: usize,
    /// Number of requests received from control-queue.
    pub n_requests: usize,
    /// Total number of connections accepted.
    pub n_accepted: usize,
}

impl FinState {
    fn to_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {}, {:?}: {}, {:?}: {} }}"),
            "n_polls",
            self.stats.n_polls,
            "n_events",
            self.stats.n_events,
            "n_requests",
            self.stats.n_requests,
            "n_accepted",
            self.stats.n_accepted
        )
    }
}

impl Default for Listener {
    fn default() -> Listener {
        let config = Config::default();
        let mut def = Listener {
            name: config.name.clone(),
            prefix: String::default(),
            config,
            proto: Protocol::new_v5_protocol(toml::Value::default()),
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => debug!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl ToJson for Listener {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {:?}, {:?}: {} }}"),
            "name",
            self.config.name,
            "port",
            self.proto.to_listen_port()
        )
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Close(stats) => stats.to_json(),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Listener {
    /// Poll register token for waker event.
    pub const TOKEN_WAKE: mio::Token = mio::Token(1);
    /// Poll register for listener TcpStream.
    pub const TOKEN_LISTENER: mio::Token = mio::Token(2);

    /// Create a listener from configuration. Listener shall be in `Init` state. To start
    /// this listener thread call [Listener::spawn].
    pub fn from_config(config: &Config, proto: Protocol) -> Result<Listener> {
        let mut val = Listener {
            name: config.name.clone(),
            prefix: String::default(),
            config: config.clone(),
            proto,
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster, app_tx: AppTx) -> Result<Listener> {
        use mio::{Interest, Waker};

        let mut listener = {
            let sock_addr: net::SocketAddr = self.proto.to_listen_address();
            mio::net::TcpListener::bind(sock_addr)?
        };

        let poll = {
            let interests = Interest::READABLE;
            let poll = err!(IOError, try: mio::Poll::new(), "fail creating mio::Poll")?;
            poll.registry().register(&mut listener, Self::TOKEN_LISTENER, interests)?;
            poll
        };
        let waker = Arc::new(Waker::new(poll.registry(), Self::TOKEN_WAKE)?);

        let mut listener = Listener {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            proto: self.proto.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                listener,
                cluster: Box::new(cluster),

                stats: Stats::default(),

                app_tx,
            }),
        };
        listener.prefix = listener.prefix();
        let mut thrd = Thread::spawn(&self.prefix, listener);
        thrd.set_waker(Arc::clone(&waker));

        let mut listener = Listener {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            proto: self.proto.clone(),
            inner: Inner::Handle(waker, thrd),
        };
        listener.prefix = listener.prefix();

        info!("{} port:{} listening ... ", self.prefix, self.proto.to_listen_port());

        Ok(listener)
    }
}

pub enum Request {
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with listener-thread, and shall wake the thread
impl Listener {
    pub fn close_wait(mut self) -> Listener {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(_waker, thrd) => {
                app_fatal!(self, thrd.request(Request::Close).flatten());
                thrd.close_wait()
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Threadable for Listener {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{} spawn thread config:{}", self.prefix, self.to_config_json());

        let mut events = Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            if let Err(err) = self.as_mut_poll().poll(&mut events, timeout) {
                self.as_app_tx().send("exit".to_string()).ok();
                error!("{} thread error exit {} ", self.prefix, err);
                break;
            }
            self.incr_n_polls();

            match self.mio_events(&rx, &events) {
                true => break,
                _exit => (),
            };
        }

        match &self.inner {
            Inner::Main(_) => self.handle_close(Request::Close),
            Inner::Close(_) => Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} thread exit", self.prefix);
        self
    }
}

impl Listener {
    // return (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &Events) -> bool {
        let mut count = 0_usize;
        let mut iter = events.iter();
        let exit = 'outer: loop {
            match iter.next() {
                Some(event) => {
                    count += 1;

                    match event.token() {
                        Self::TOKEN_WAKE => loop {
                            // keep repeating until all control requests are drained
                            match self.drain_control_chan(rx) {
                                (_status, true) => break 'outer true,
                                (QueueStatus::Ok(_), _exit) => (),
                                (QueueStatus::Block(_), _) => break,
                                (QueueStatus::Disconnected(_), _) => break 'outer true,
                            }
                        },
                        Self::TOKEN_LISTENER => loop {
                            match self.accept_conn() {
                                QueueStatus::Ok(_) => (),
                                QueueStatus::Block(_) => break,
                                QueueStatus::Disconnected(_) => break 'outer true,
                            };
                        },
                        _ => unreachable!(),
                    }
                }
                None => break false,
            }
        };

        self.incr_n_events(count);

        exit
    }

    // return (queue-status, exit)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (QueueReq, bool) {
        use crate::broker::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let mut status = pending_requests(&self.prefix, rx, CONTROL_CHAN_SIZE);
        let reqs = status.take_values();

        self.incr_n_requests(reqs.len());

        let mut closed = false;
        for req in reqs.into_iter() {
            match req {
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                    closed = true;
                }
                (_, _) => unreachable!(),
            }
        }

        (status, closed)
    }

    fn accept_conn(&mut self) -> QueueStatus<()> {
        use crate::broker::Handshake;

        let RunLoop { listener, cluster, stats, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        match listener.accept() {
            Ok((sock, addr)) => {
                info!("{} raddr:{} incoming connection", self.prefix, addr);
                let raddr = sock.peer_addr().unwrap();

                assert_eq!(raddr, addr);

                // for every successful accept launch a handshake thread.
                let hs = Handshake {
                    prefix: format!("<h:{}>", self.config.name),
                    raddr,
                    config: self.config.clone(),

                    proto: self.proto.clone(),
                    cluster: cluster.to_tx("handshake"),
                    sock: Some(sock),
                };
                let thrd = Thread::spawn_sync("handshake", 1, hs);
                thrd.drop(); // alternative to close_wait()

                stats.n_accepted += 1;
                QueueStatus::Ok(Vec::new())
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                QueueStatus::Block(Vec::new())
            }
            Err(err) => {
                error!("{} connection accept err:{}", self.prefix, err);
                QueueStatus::Disconnected(Vec::new())
            }
        }
    }
}

impl Listener {
    fn handle_close(&mut self, _req: Request) -> Response {
        let run_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => run_loop,
            Inner::Close(_) => return Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} closing listener", self.prefix);

        mem::drop(run_loop.poll);
        mem::drop(run_loop.listener);
        mem::drop(run_loop.cluster);
        mem::drop(run_loop.app_tx);

        let fin_state = FinState { stats: run_loop.stats };
        info!("{} stats:{}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();
        Response::Ok
    }
}

impl Listener {
    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_, _) => "hndl",
            Inner::Main(_) => "main",
            Inner::Close(_) => "close",
        };
        format!("<l:{}:{}>", self.name, state)
    }

    fn incr_n_polls(&mut self) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_polls += 1,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_events(&mut self, n: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_events += n,
            Inner::Close(finstate) => finstate.stats.n_events += n,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_requests(&mut self, n: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_requests += n,
            Inner::Close(finstate) => finstate.stats.n_requests += n,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}
