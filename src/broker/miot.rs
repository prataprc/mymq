use log::{debug, error, info, trace, warn};

use std::collections::{BTreeMap, VecDeque};
use std::{fmt, mem, net, result, sync::Arc, time};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{socket, AppTx, Config, QueueStatus, Shard, Socket};

use crate::{ClientID, MQTTRead, MQTTWrite, ToJson};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;

/// Type handle sending and receiving of raw MQTT packets.
///
/// Handles serialization of of MQTT packets, sending and receiving them to
/// the correct shard-thread that can handle this client/session. Note that there
/// will be a [Miot] instance for every [Shard] instance.
pub struct Miot {
    /// Human readable name for this miot thread.
    pub name: String,
    /// Same as the shard-id.
    pub miot_id: u32,
    prefix: String,
    config: Config,
    inner: Inner,
}

enum Inner {
    Init,
    // Help by Shard.
    Handle(Arc<mio::Waker>, Thread<Miot, Request, Result<Response>>),
    // Thread.
    Main(RunLoop),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState),
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Miot::Inner::Init"),
            Inner::Handle(_, _) => write!(f, "Miot::Inner::Handle"),
            Inner::Main(_) => write!(f, "Miot::Inner::Main"),
            Inner::Close(_) => write!(f, "Miot::Inner::Close"),
        }
    }
}

struct RunLoop {
    /// Mio poller for asynchronous handling, aggregate events from remote client and
    /// thread-waker.
    poll: mio::Poll,
    /// Shard-tx associated with the shard that is paired with this miot thread.
    shard: Box<Shard>,

    /// next available token for connections
    next_token: mio::Token,
    /// collection of all active socket connections, and its associated data.
    conns: BTreeMap<ClientID, Socket>,

    /// Statistics
    stats: Stats,

    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
    pub next_token: mio::Token,
    pub client_ids: Vec<ClientID>,
    pub addrs: Vec<net::SocketAddr>,
    pub tokens: Vec<mio::Token>,
    pub stats: Stats,
}

#[derive(Clone, Copy, Default)]
pub struct Stats {
    pub n_polls: usize,
    pub n_events: usize,
    pub n_requests: usize,
    pub n_add_conns: usize,
    pub n_rem_conns: usize,
    pub n_wpkts: usize,
    pub n_wbytes: usize,
}

impl FinState {
    fn to_json(&self) -> String {
        format!(
            concat!(
                "{{ ",
                "{:?}: {}, {:?}: {}, {:?}: {}, {:?}: {}, {:?}: {}, {:?}: {},",
                "{:?}: {}, {:?}: {}",
                "}}"
            ),
            "next_token",
            self.next_token.0,
            "n_conns",
            self.client_ids.len(),
            "n_polls",
            self.stats.n_polls,
            "n_events",
            self.stats.n_events,
            "n_add_conns",
            self.stats.n_add_conns,
            "n_rem_conns",
            self.stats.n_rem_conns,
            "n_wpkts",
            self.stats.n_wpkts,
            "n_wbytes",
            self.stats.n_wbytes,
        )
    }
}

impl Default for Miot {
    fn default() -> Miot {
        let config = Config::default();
        let mut def = Miot {
            name: config.name.clone(),
            miot_id: u32::default(),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Miot {
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

impl ToJson for Miot {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {} }}"),
            "mqtt_max_packet_size", self.config.mqtt_max_packet_size,
        )
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Close(stats) => stats.to_json(),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Miot {
    const WAKE_TOKEN: mio::Token = mio::Token(1);
    const FIRST_TOKEN: mio::Token = mio::Token(2);

    /// Create a miot thread from configuration. Miot shall be in `Init` state, to start
    /// the miot thread call [Miot::spawn].
    pub fn from_config(config: Config, miot_id: u32) -> Result<Miot> {
        let mut val = Miot {
            name: config.name.clone(),
            miot_id,
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, shard: Shard, app_tx: AppTx) -> Result<Miot> {
        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let mut miot = Miot {
            name: self.config.name.clone(),
            miot_id: self.miot_id,
            prefix: String::default(),
            config: self.config.clone(),

            inner: Inner::Main(RunLoop {
                poll,
                shard: Box::new(shard),

                next_token: Self::FIRST_TOKEN,
                conns: BTreeMap::default(),

                stats: Stats::default(),

                app_tx: app_tx.clone(),
            }),
        };
        miot.prefix = miot.prefix();
        let mut thrd = Thread::spawn(&self.prefix, miot);
        thrd.set_waker(Arc::clone(&waker));

        let mut val = Miot {
            name: self.config.name.clone(),
            miot_id: self.miot_id,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn to_waker(&self) -> Arc<mio::Waker> {
        match &self.inner {
            Inner::Handle(waker, _thrd) => Arc::clone(waker),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

pub enum Request {
    AddConnection(AddConnectionArgs),
    RemoveConnection { client_id: ClientID },
    Close,
}

pub enum Response {
    Ok,
    Removed(Socket),
}

pub struct AddConnectionArgs {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub upstream: socket::PktTx,
    pub downstream: socket::PktRx,
    pub max_packet_size: u32,
}

// calls to interface with miot-thread, and shall wake the thread
impl Miot {
    pub fn wake(&self) {
        match &self.inner {
            Inner::Handle(waker, _thrd) => allow_panic!(self, waker.wake()),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn add_connection(&self, args: AddConnectionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(_waker, thrd) => {
                let req = Request::AddConnection(args);
                match thrd.request(req)?? {
                    Response::Ok => Ok(()),
                    _ => unreachable!("{} unxpected response", self.prefix),
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn remove_connection(&self, id: &ClientID) -> Result<Option<Socket>> {
        match &self.inner {
            Inner::Handle(_waker, thrd) => {
                let req = Request::RemoveConnection { client_id: id.clone() };
                match thrd.request(req)?? {
                    Response::Removed(socket) => Ok(Some(socket)),
                    Response::Ok => Ok(None),
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn close_wait(mut self) -> Miot {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(_waker, thrd) => {
                let req = Request::Close;
                match thrd.request(req).ok().map(|x| x.ok()).flatten() {
                    Some(Response::Ok) => thrd.close_wait(),
                    _ => unreachable!("{} unxpected response", self.prefix),
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{} spawn config:{}", self.prefix, self.to_config_json());

        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(&self, self.as_mut_poll().poll(&mut events, timeout));

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

impl Miot {
    // return (exit,)
    // can happen because the control channel has disconnected, or Request::Close
    fn mio_events(&mut self, rx: &ThreadRx, events: &mio::Events) -> bool {
        let mut count = 0;
        for event in events.iter() {
            trace!("{} token:{} poll-event", self.prefix, event.token().0);
            count += 1;
        }

        let exit = loop {
            // keep repeating until all control requests are drained.
            match self.drain_control_chan(rx) {
                (_status, true) => break true,
                (QueueStatus::Ok(_), _exit) => (),
                (QueueStatus::Block(_), _) => break false,
                (QueueStatus::Disconnected(_), _) => break true,
            }
        };

        if !exit && !matches!(&self.inner, Inner::Close(_)) {
            self.socket_to_session();
            self.session_to_socket();
        }

        self.incr_n_events(count);

        exit
    }

    // Return (queue-status, exit)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (QueueReq, bool) {
        use crate::broker::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let mut status = pending_requests(&self.prefix, rx, CONTROL_CHAN_SIZE);
        let reqs = status.take_values();

        self.incr_n_requests(reqs.len());

        let mut closed = false;
        for req in reqs.into_iter() {
            match req {
                (req @ AddConnection { .. }, Some(tx)) => {
                    let resp = self.handle_add_connection(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ RemoveConnection { .. }, Some(tx)) => {
                    let resp = self.handle_remove_connection(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                    closed = true
                }
                (_, _) => unreachable!(),
            }
        }

        (status, closed)
    }
}

impl Miot {
    fn socket_to_session(&mut self) {
        let conns = match &mut self.inner {
            Inner::Main(RunLoop { conns, .. }) => conns,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut fail_queues = Vec::new();
        for (client_id, socket) in conns.iter_mut() {
            let prefix = {
                let raddr = socket.conn.peer_addr().unwrap();
                format!("rconn:{}:{}", raddr, **client_id)
            };
            match socket.read_packets(&prefix, &self.config) {
                Ok(QueueStatus::Ok(_)) | Ok(QueueStatus::Block(_)) => (),
                Ok(QueueStatus::Disconnected(_)) => {
                    fail_queues.push((client_id.clone(), None));
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{} error in read_packets err:{}", prefix, err);
                    fail_queues.push((client_id.clone(), Some(err)));
                }
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{} error in read_packets err:{}", prefix, err);
                    fail_queues.push((client_id.clone(), Some(err)));
                }
                Err(err) => unreachable!("{} unexpected err {}", self.prefix, err),
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            if let Response::Removed(socket) = self.handle_remove_connection(req) {
                allow_panic!(&self, self.as_shard().flush_connection(socket, err));
            }
        }
    }

    fn session_to_socket(&mut self) {
        use crate::broker::socket::Stats as SockStats;

        let conns = match &mut self.inner {
            Inner::Main(RunLoop { conns, .. }) => conns,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        // if thread is closed conns will be empty.
        let mut fail_queues = Vec::new(); // TODO: with_capacity ?
        let mut wstats = SockStats::default();
        for (client_id, socket) in conns.iter_mut() {
            let prefix = {
                let raddr = socket.conn.peer_addr().unwrap();
                format!("wconn:{}:{}", raddr, **client_id)
            };
            match socket.write_packets(&prefix, &self.config) {
                (QueueStatus::Ok(_), stats) => {
                    wstats.update(&stats);
                    () // TODO: should we wake the session here.
                }
                (QueueStatus::Block(_), stats) => {
                    wstats.update(&stats);
                    () // TODO: should we wake the session here.
                }
                (QueueStatus::Disconnected(_), stats) => {
                    wstats.update(&stats);
                    fail_queues.push((client_id.clone(), None))
                }
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            if let Response::Removed(socket) = self.handle_remove_connection(req) {
                allow_panic!(&self, self.as_shard().flush_connection(socket, err));
            }
        }

        self.incr_wstats(&wstats);
    }
}

impl Miot {
    fn handle_add_connection(&mut self, req: Request) -> Response {
        use mio::Interest;

        let mut args = match req {
            Request::AddConnection(args) => args,
            _ => unreachable!(),
        };
        let raddr = args.conn.peer_addr().unwrap();

        let (poll, conns, token) = match &mut self.inner {
            Inner::Main(RunLoop { poll, conns, next_token, .. }) => {
                let token = *next_token;
                *next_token = mio::Token(next_token.0 + 1);
                (poll, conns, token)
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} raddr:{} adding connection ...", self.prefix, raddr);

        let max_packet_size = self.config.mqtt_max_packet_size;
        let (session_tx, miot_rx) = (args.upstream, args.downstream);

        let interests = Interest::READABLE | Interest::WRITABLE;
        allow_panic!(self, poll.registry().register(&mut args.conn, token, interests));

        let rd = socket::Source {
            pr: MQTTRead::new(max_packet_size),
            timeout: None,
            session_tx,
            packets: VecDeque::default(),
        };
        let wt = socket::Sink {
            pw: MQTTWrite::new(&[], args.max_packet_size),
            timeout: None,
            miot_rx,
            packets: VecDeque::default(),
        };
        let (client_id, conn) = (args.client_id.clone(), args.conn);
        let socket = socket::Socket { client_id, conn, token, rd, wt };
        conns.insert(args.client_id, socket);

        self.incr_n_add_conns();

        Response::Ok
    }

    fn handle_remove_connection(&mut self, req: Request) -> Response {
        let client_id = match req {
            Request::RemoveConnection { client_id } => client_id,
            _ => unreachable!(),
        };

        let (poll, conns) = match &mut self.inner {
            Inner::Main(RunLoop { poll, conns, .. }) => (poll, conns),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let res = match conns.remove(&client_id) {
            Some(mut socket) => {
                let raddr = socket.conn.peer_addr().unwrap();
                info!("{} raddr:{} removing connection ...", self.prefix, raddr);
                allow_panic!(&self, poll.registry().deregister(&mut socket.conn));
                Response::Removed(socket)
            }
            None => {
                warn!(
                    "{} client_id:{} connection for not found ...",
                    self.prefix, *client_id
                );
                Response::Ok
            }
        };

        self.incr_n_rem_conns();
        res
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        let mut run_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} closing miot", self.prefix);

        mem::drop(run_loop.poll);
        mem::drop(run_loop.shard);
        let conns = mem::replace(&mut run_loop.conns, BTreeMap::default());

        let mut client_ids = Vec::with_capacity(conns.len());
        let mut addrs = Vec::with_capacity(conns.len());
        let mut tokens = Vec::with_capacity(conns.len());

        for (cid, sock) in conns.into_iter() {
            let raddr = sock.conn.peer_addr().unwrap();
            info!("{} raddr:{} client_id:{} closing socket", self.prefix, raddr, *cid);
            client_ids.push(sock.client_id);
            addrs.push(raddr);
            tokens.push(sock.token);
        }

        let fin_state = FinState {
            next_token: run_loop.next_token,
            client_ids,
            addrs,
            tokens,
            stats: run_loop.stats,
        };

        info!("{} stats:{}", self.prefix, fin_state.to_json());
        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();

        Response::Ok
    }
}

impl Miot {
    fn incr_n_polls(&mut self) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_polls += 1,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_events(&mut self, n: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_events += n,
            Inner::Close(finstats) => finstats.stats.n_events += n,
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

    fn incr_n_add_conns(&mut self) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_add_conns += 1,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_rem_conns(&mut self) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_rem_conns += 1,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_wstats(&mut self, wstats: &socket::Stats) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => {
                stats.n_wpkts += wstats.items;
                stats.n_wbytes += wstats.bytes;
            }
            _ => unreachable!(),
        }
    }

    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_, _) => "hndl",
            Inner::Main(_) => "main",
            Inner::Close(_) => "close",
        };
        format!("<m:{}:{}>", self.name, state)
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_shard(&self) -> &Shard {
        match &self.inner {
            Inner::Main(RunLoop { shard, .. }) => shard,
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
