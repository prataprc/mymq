//! Miot threading model.
//!
//! ```ignore
//!                        spawn()
//! from_config() -> Init -----+----> Handle
//!                            |
//!                            |
//!                            V
//!                           Main
//! ```

use log::{debug, error, info, trace, warn};

use std::collections::{BTreeMap, VecDeque};
use std::{fmt, mem, net, result, sync::Arc, time};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{AppTx, Config, ShardAPI};
use crate::{Blob, PacketRx, PacketTx, Packetize, QPacket, QueueStatus, Socket};
use crate::{ClientID, ToJson};
use crate::{ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;
type QueuePkt = QueueStatus<QPacket>;

/// Type handle sending and receiving protocol packets.
///
/// Handles serialization of packets, sending and receiving them to
/// the correct shard-thread that can handle this client/session. Note that there
/// will be a [Miot] instance for every [Shard] instance.
pub struct Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    /// Human readable name for this miot thread.
    pub name: String,
    /// Same as the shard-id.
    pub miot_id: u32,
    prefix: String,
    config: Config,

    inner: Inner<S>,
}

enum Inner<S>
where
    S: 'static + Send + ShardAPI,
{
    Init,
    Main(RunLoop<S>), // Thread.
    Handle(Arc<mio::Waker>, Thread<Miot<S>, Request, Result<Response>>), // Help by Shard.
    Close(FinState),  // Held by Cluster, replacing both Handle and Main.
}

impl<S> fmt::Debug for Inner<S>
where
    S: 'static + Send + ShardAPI,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Miot::Inner::Init"),
            Inner::Main(_) => write!(f, "Miot::Inner::Main"),
            Inner::Handle(_, _) => write!(f, "Miot::Inner::Handle"),
            Inner::Close(_) => write!(f, "Miot::Inner::Close"),
        }
    }
}

struct RunLoop<S>
where
    S: 'static + Send + ShardAPI,
{
    /// Mio poller for asynchronous handling, aggregate events from remote client and
    /// thread-waker.
    poll: mio::Poll,
    /// Shard-tx associated with the shard that is paired with this miot thread.
    shard: Box<S>,

    /// next available token for connections
    next_token: mio::Token,
    /// collection of all active packet-queue, and its associated data.
    conns: BTreeMap<ClientID, PQueue>,

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

impl<S> Default for Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    fn default() -> Miot<S> {
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

impl<S> Drop for Miot<S>
where
    S: 'static + Send + ShardAPI,
{
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

impl<S> ToJson for Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    fn to_config_json(&self) -> String {
        format!("{{}}")
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Close(stats) => stats.to_json(),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl<S> Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    const WAKE_TOKEN: mio::Token = mio::Token(1);
    const FIRST_TOKEN: mio::Token = mio::Token(2);

    /// Create a miot thread from configuration. Miot shall be in `Init` state, to start
    /// the miot thread call [Miot::spawn].
    pub fn from_config(config: Config, miot_id: u32) -> Result<Miot<S>> {
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

    pub fn spawn(self, shard: S, app_tx: AppTx) -> Result<Miot<S>> {
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
    AddConnection(PQueueArgs),
    RemoveConnection { client_id: ClientID },
    Close,
}

pub enum Response {
    Ok,
    Removed(PQueue),
}

// calls to interface with miot-thread, and shall wake the thread
impl<S> Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    pub fn wake(&self) {
        match &self.inner {
            Inner::Handle(waker, _thrd) => {
                app_fatal!(self, waker.wake());
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn add_connection(&self, pq: PQueueArgs) {
        match &self.inner {
            Inner::Handle(_waker, thrd) => {
                let req = Request::AddConnection(pq);
                app_fatal!(self, thrd.request(req).flatten());
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn remove_connection(&self, id: &ClientID) -> Result<Option<PQueue>> {
        match &self.inner {
            Inner::Handle(_waker, thrd) => {
                let req = Request::RemoveConnection { client_id: id.clone() };
                match app_fatal!(self, thrd.request(req).flatten()) {
                    Some(Response::Removed(pq)) => Ok(Some(pq)),
                    Some(_) | None => Ok(None),
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn close_wait(mut self) -> Miot<S> {
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

impl<S> Threadable for Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{} spawn config:{}", self.prefix, self.to_config_json());

        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
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

impl<S> Miot<S>
where
    S: 'static + Send + ShardAPI,
{
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
                    app_fatal!(self, tx.send(Ok(resp)));
                }
                (req @ RemoveConnection { .. }, Some(tx)) => {
                    let resp = self.handle_remove_connection(req);
                    app_fatal!(self, tx.send(Ok(resp)));
                }
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close(req);
                    app_fatal!(self, tx.send(Ok(resp)));
                    closed = true
                }
                (_, _) => unreachable!(),
            }
        }

        (status, closed)
    }
}

impl<S> Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    fn socket_to_session(&mut self) {
        let conns = match &mut self.inner {
            Inner::Main(RunLoop { conns, .. }) => conns,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut fail_queues = Vec::new();
        for (client_id, pq) in conns.iter_mut() {
            let prefix = format!("rconn:{}:{}", pq.peer_addr(), **client_id);
            match pq.read_packets(&prefix) {
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
            if let Response::Removed(pq) = self.handle_remove_connection(req) {
                self.as_shard().flush_session(pq, err)
            }
        }
    }

    fn session_to_socket(&mut self) {
        let conns = match &mut self.inner {
            Inner::Main(RunLoop { conns, .. }) => conns,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        // if thread is closed conns will be empty.
        let mut fail_queues = Vec::new(); // TODO: with_capacity ?
        let (mut items, mut bytes) = (0_usize, 0_usize);
        for (client_id, pq) in conns.iter_mut() {
            let prefix = format!("wconn:{}:{}", pq.peer_addr(), **client_id);
            let (a, b) = match pq.write_packets(&prefix) {
                (QueueStatus::Ok(_), a, b) => {
                    // TODO: should we wake the session here.
                    (a, b)
                }
                (QueueStatus::Block(_), a, b) => {
                    // TODO: should we wake the session here.
                    (a, b)
                }
                (QueueStatus::Disconnected(_), a, b) => {
                    fail_queues.push((client_id.clone(), None));
                    (a, b)
                }
            };
            items += a;
            bytes += b;
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            if let Response::Removed(pq) = self.handle_remove_connection(req) {
                self.as_shard().flush_session(pq, err)
            }
        }

        self.incr_wstats(items, bytes)
    }
}

impl<S> Miot<S>
where
    S: 'static + Send + ShardAPI,
{
    fn handle_add_connection(&mut self, req: Request) -> Response {
        use mio::Interest;

        let mut pq = match req {
            Request::AddConnection(args) => PQueue::new(args),
            _ => unreachable!(),
        };

        info!("{} raddr:{} adding connection ...", self.prefix, pq.peer_addr());

        let interests = Interest::READABLE | Interest::WRITABLE;
        let token = self.next_token();
        app_fatal!(
            self,
            self.as_mut_poll().registry().register(pq.as_mut_socket(), token, interests)
        );

        pq.as_mut_socket().set_mio_token(token);

        match &mut self.inner {
            Inner::Main(RunLoop { conns, .. }) => {
                conns.insert(pq.as_client_id().clone(), pq);
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        self.incr_n_add_conns();

        Response::Ok
    }

    fn handle_remove_connection(&mut self, req: Request) -> Response {
        let cid = match req {
            Request::RemoveConnection { client_id } => client_id,
            _ => unreachable!(),
        };

        let (poll, conns) = match &mut self.inner {
            Inner::Main(RunLoop { poll, conns, .. }) => (poll, conns),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let res = match conns.remove(&cid) {
            Some(mut pq) => {
                let raddr = pq.peer_addr();
                info!("{} raddr:{} removing connection ...", self.prefix, raddr);
                app_fatal!(&self, poll.registry().deregister(pq.as_mut_socket()));
                Response::Removed(pq)
            }
            None => {
                warn!("{} client_id:{} connection for not found ...", self.prefix, *cid);
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

        for (cid, mut pq) in conns.into_iter() {
            let raddr = pq.peer_addr();
            info!("{} raddr:{} client_id:{} closing socket", self.prefix, raddr, *cid);
            client_ids.push(pq.as_client_id().clone());
            addrs.push(raddr);
            tokens.push(pq.as_mut_socket().to_mio_token());
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

impl<S> Miot<S>
where
    S: 'static + Send + ShardAPI,
{
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

    fn incr_wstats(&mut self, items: usize, bytes: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => {
                stats.n_wpkts += items;
                stats.n_wbytes += bytes;
            }
            _ => unreachable!(),
        }
    }

    fn next_token(&mut self) -> mio::Token {
        match &mut self.inner {
            Inner::Main(RunLoop { next_token, .. }) => {
                let token = *next_token;
                *next_token = mio::Token(next_token.0 + 1);
                token
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
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

    fn as_shard(&self) -> &S {
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

pub struct PQueue {
    config: Config,
    socket: Socket,
    session_tx: PacketTx, // Outbound channel to session thread
    miot_rx: PacketRx,    // Inbound channel from session thread
    // All incoming MQTT packets on this socket first land here.
    inc_packets: VecDeque<QPacket>,
    // All out-going MQTT packets on this socket first land here.
    oug_packets: VecDeque<QPacket>,
}

pub struct PQueueArgs {
    pub config: Config,
    pub socket: Socket,
    pub session_tx: PacketTx,
    pub miot_rx: PacketRx,
}

impl PQueue {
    pub fn new(args: PQueueArgs) -> PQueue {
        PQueue {
            config: args.config,
            socket: args.socket,
            session_tx: args.session_tx,
            miot_rx: args.miot_rx,
            inc_packets: VecDeque::default(),
            oug_packets: VecDeque::default(),
        }
    }

    #[inline]
    pub fn peer_addr(&self) -> net::SocketAddr {
        self.socket.peer_addr()
    }

    #[inline]
    pub fn as_client_id(&self) -> &ClientID {
        self.socket.as_client_id()
    }

    #[inline]
    pub fn as_mut_socket(&mut self) -> &mut Socket {
        &mut self.socket
    }
}

impl PQueue {
    // returned QueueStatus shall not carry any packets, packets are booked in Socket
    // Error shall be MalformedPacket or ProtocolError.
    pub fn read_packets(&mut self, prefix: &str) -> Result<QueuePkt> {
        let batch_size = self.config.pkt_batch_size as usize;

        // before reading from socket, send remaining packets to shard.
        match self.send_upstream(prefix) {
            QueueStatus::Ok(_) => {
                let status = loop {
                    let mut status = self.socket.read_packet(prefix)?;
                    self.inc_packets.extend(status.take_values().into_iter());
                    match status {
                        QueueStatus::Ok(_) if self.inc_packets.len() < batch_size => (),
                        status => break status,
                    }
                };

                match status {
                    s @ QueueStatus::Disconnected(_) if self.inc_packets.is_empty() => {
                        Ok(s.replace(Vec::new()))
                    }
                    _status => Ok(self.send_upstream(prefix)),
                }
            }
            status => Ok(status),
        }
    }

    // QueueStatus shall not carry any packets
    fn send_upstream(&mut self, prefix: &str) -> QueueStatus<QPacket> {
        let session_tx = self.session_tx.clone(); // shard woken when dropped
        let mut status = {
            let pkts = mem::replace(&mut self.inc_packets, VecDeque::default());
            session_tx.try_sends(prefix, pkts.into())
        };
        self.inc_packets = status.take_values().into(); // left over packets
        status
    }
}

impl PQueue {
    // Return (QueueStatus, no-of-packets-written, no-of-bytes-written)
    pub fn write_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        // before reading from socket, send remaining packets to connection.
        let (mut items, mut bytes) = (0_usize, 0_usize);
        let deadline = {
            let dur = time::Duration::from_secs(u64::from(self.config.flush_timeout));
            time::Instant::now() + dur
        };

        loop {
            if time::Instant::now() > deadline {
                break (QueueStatus::Block(Vec::new()), items, bytes);
            }

            match self.flush_packets(prefix) {
                (QueueStatus::Ok(_), a, b) => {
                    items += a;
                    bytes += b;
                }
                (status @ QueueStatus::Disconnected(_), a, b) => {
                    items += a;
                    bytes += b;
                    break (status, items, bytes);
                }
                (QueueStatus::Block(_), _a, _b) => unreachable!(),
            }

            let mut status = self.miot_rx.try_recvs(prefix);
            self.oug_packets.extend(status.take_values().into_iter());

            match status {
                QueueStatus::Ok(_) => (),
                QueueStatus::Block(_) => {
                    let (status, a, b) = self.flush_packets(prefix);
                    items += a;
                    bytes += b;
                    break (status, items, bytes);
                }
                status @ QueueStatus::Disconnected(_) => break (status, items, bytes),
            }
        }
    }

    // QueueStatus shall not carry any packets, (queue, items, bytes)
    pub fn flush_packets(&mut self, prefix: &str) -> (QueuePkt, usize, usize) {
        let mut iter = {
            let val = mem::replace(&mut self.oug_packets, VecDeque::default());
            val.into_iter()
        };

        let (mut items, mut bytes) = (0_usize, 0_usize);
        let mut blob: Option<Blob> = None;

        let status = loop {
            match self.socket.write_packet(prefix, blob.take()) {
                QueueStatus::Ok(_) => match iter.next() {
                    Some(packet) => match packet.encode() {
                        Ok(blob0) => {
                            items += 1;
                            bytes += blob0.as_ref().len();
                            blob = Some(blob0);
                        }
                        Err(err) => {
                            error!("{} packet:{} skipping err:{}", prefix, packet, err);
                        }
                    },
                    None => break QueueStatus::Ok(Vec::new()),
                },
                status @ QueueStatus::Disconnected(_) => break status,
                QueueStatus::Block(_) => unreachable!(),
            }
        };

        self.oug_packets.extend(iter);

        (status, items, bytes)
    }
}
