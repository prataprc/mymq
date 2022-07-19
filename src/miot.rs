use log::{debug, error, info, trace};

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::{mem, net, time};

use crate::packet::{MQTTRead, MQTTWrite};
use crate::thread::{Rx, Thread, Threadable};
use crate::{socket, AppTx, ClientID, Config, QueueStatus, Shard, Socket};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::thread::QueueReq<Request, Result<Response>>;

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

    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
    pub next_token: mio::Token,
    pub client_ids: Vec<ClientID>,
    pub addrs: Vec<net::SocketAddr>,
    pub tokens: Vec<mio::Token>,
}

impl Default for Miot {
    fn default() -> Miot {
        let config = Config::default();
        let mut def = Miot {
            name: format!("{}-miot-init", config.name),
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
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => info!("{} drop ...", self.prefix),
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
            name: format!("{}-miot-init", config.name),
            miot_id,
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, shard: Shard, app_tx: AppTx) -> Result<Miot> {
        if matches!(&self.inner, Inner::Handle(_, _) | Inner::Main(_)) {
            err!(InvalidInput, desc: "miot can be spawned only in init-state ")?;
        }

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let mut miot = Miot {
            name: format!("{}-miot-main", self.config.name),
            miot_id: self.miot_id,
            prefix: String::default(),
            config: self.config.clone(),

            inner: Inner::Main(RunLoop {
                poll,
                shard: Box::new(shard),

                next_token: Self::FIRST_TOKEN,
                conns: BTreeMap::default(),

                app_tx: app_tx.clone(),
            }),
        };
        miot.prefix = miot.prefix();
        let mut thrd = Thread::spawn(&self.prefix, miot);
        thrd.set_waker(Arc::clone(&waker));

        let mut val = Miot {
            name: format!("{}-miot-handle", self.config.name),
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
            _ => unreachable!(),
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
    pub addr: net::SocketAddr,
    pub upstream: socket::PktTx,
    pub downstream: socket::PktRx,
    pub client_max_packet_size: u32,
}

// calls to interface with miot-thread, and shall wake the thread
impl Miot {
    pub fn wake(&self) {
        match &self.inner {
            Inner::Handle(waker, _thrd) => allow_panic!(self, waker.wake()),
            _ => unreachable!(),
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
            _ => unreachable!(),
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
            _ => unreachable!(),
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
            _ => unreachable!(),
        }
    }
}

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        info!("{} spawn ...", self.prefix);

        let mut events = mio::Events::with_capacity(crate::POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(&self, self.as_mut_poll().poll(&mut events, timeout));

            match self.mio_events(&rx, &events) {
                true => break,
                _exit => (),
            };
        }

        match &self.inner {
            Inner::Main(_) => self.handle_close(Request::Close),
            Inner::Close(_) => Response::Ok,
            _ => unreachable!(),
        };

        info!("{} thread exit...", self.prefix);
        self
    }
}

impl Miot {
    // return (exit,)
    // can happen because the control channel has disconnected, or Request::Close
    fn mio_events(&mut self, rx: &ThreadRx, events: &mio::Events) -> bool {
        let mut count = 0;
        for event in events.iter() {
            trace!("{} poll-event token:{}", self.prefix, event.token().0);
            count += 1;
        }
        debug!("{} polled {} events", self.prefix, count);

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

        exit
    }

    // Return (queue-status, exit)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (QueueReq, bool) {
        use crate::thread::pending_requests;
        use Request::*;

        let mut status = pending_requests(&self.prefix, rx, crate::CONTROL_CHAN_SIZE);
        let reqs = status.take_values();
        debug!("{} process {} requests closed:false", self.prefix, reqs.len());

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
        let (shard, conns) = match &mut self.inner {
            Inner::Main(RunLoop { shard, conns, .. }) => (shard, conns),
            _ => unreachable!(),
        };
        let shard = shard.to_tx();

        let mut fail_queues = Vec::new();
        for (client_id, socket) in conns.iter_mut() {
            let prefix = format!("rconn:{}:{}", socket.addr, **client_id);
            match socket.read_packets(&prefix, &self.config) {
                Ok(QueueStatus::Ok(_)) | Ok(QueueStatus::Block(_)) => (),
                Ok(QueueStatus::Disconnected(_)) => {
                    let err: Result<()> = err!(Disconnected, desc: "{} socketrx", prefix);
                    fail_queues.push((client_id.clone(), err.unwrap_err()));
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    error!("{} error in read_packets : {}", prefix, err);
                    fail_queues.push((client_id.clone(), err));
                }
                Err(err) if err.kind() == ErrorKind::MalformedPacket => {
                    error!("{} error in read_packets : {}", prefix, err);
                    fail_queues.push((client_id.clone(), err));
                }
                Err(err) => unreachable!("{} unexpected err {}", self.prefix, err),
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            if let Response::Removed(socket) = self.handle_remove_connection(req) {
                allow_panic!(&self, shard.flush_connection(socket, err));
            }
        }
    }

    fn session_to_socket(&mut self) {
        let (shard, conns) = match &mut self.inner {
            Inner::Main(RunLoop { shard, conns, .. }) => (shard, conns),
            _ => unreachable!(),
        };
        let shard = shard.to_tx();

        // if thread is closed conns will be empty.
        let mut fail_queues = Vec::new(); // TODO: with_capacity ?
        for (client_id, socket) in conns.iter_mut() {
            let prefix = format!("wconn:{}:{}", socket.addr, **client_id);
            match socket.write_packets(&prefix, &self.config) {
                QueueStatus::Ok(_) | QueueStatus::Block(_) => {
                    // TODO: should we wake the session here.
                    ()
                }
                QueueStatus::Disconnected(_) => {
                    error!("{} disconnected write_packets ...", prefix);
                    let err: Result<()> = err!(Disconnected, desc: "");
                    fail_queues.push((client_id.clone(), err.unwrap_err()));
                }
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            if let Response::Removed(socket) = self.handle_remove_connection(req) {
                allow_panic!(&self, shard.flush_connection(socket, err));
            }
        }
    }
}

impl Miot {
    fn handle_add_connection(&mut self, req: Request) -> Response {
        use mio::Interest;

        let (poll, conns, token) = match &mut self.inner {
            Inner::Main(RunLoop { poll, conns, next_token, .. }) => {
                let token = *next_token;
                *next_token = mio::Token(next_token.0 + 1);
                (poll, conns, token)
            }
            _ => unreachable!(),
        };

        let mut args = match req {
            Request::AddConnection(args) => args,
            _ => unreachable!(),
        };
        let (session_tx, miot_rx) = (args.upstream, args.downstream);

        let interests = Interest::READABLE | Interest::WRITABLE;
        allow_panic!(&self, poll.registry().register(&mut args.conn, token, interests));

        let rd = socket::Source {
            pr: MQTTRead::new(self.config.mqtt_max_packet_size()),
            timeout: None,
            session_tx,
            packets: VecDeque::default(),
        };
        let wt = socket::Sink {
            pw: MQTTWrite::new(&[], args.client_max_packet_size),
            timeout: None,
            miot_rx,
            packets: VecDeque::default(),
        };
        let (client_id, conn, addr) = (args.client_id.clone(), args.conn, args.addr);
        let socket = socket::Socket { client_id, conn, addr, token, rd, wt };
        conns.insert(args.client_id, socket);

        Response::Ok
    }

    fn handle_remove_connection(&mut self, req: Request) -> Response {
        error!("{} removing connection ...", self.prefix);

        let (poll, conns) = match &mut self.inner {
            Inner::Main(RunLoop { poll, conns, .. }) => (poll, conns),
            _ => unreachable!(),
        };

        let client_id = match req {
            Request::RemoveConnection { client_id } => client_id,
            _ => unreachable!(),
        };

        match conns.remove(&client_id) {
            Some(mut socket) => {
                allow_panic!(&self, poll.registry().deregister(&mut socket.conn));
                Response::Removed(socket)
            }
            None => Response::Ok,
        }
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        let run_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        info!("{} closing connections:{} ...", self.prefix, run_loop.conns.len());

        mem::drop(run_loop.poll);
        mem::drop(run_loop.shard);

        let mut client_ids = Vec::with_capacity(run_loop.conns.len());
        let mut addrs = Vec::with_capacity(run_loop.conns.len());
        let mut tokens = Vec::with_capacity(run_loop.conns.len());

        for (cid, sock) in run_loop.conns.into_iter() {
            info!("{} closing socket {:?} client-id:{:?}", self.prefix, sock.addr, *cid);
            client_ids.push(sock.client_id);
            addrs.push(sock.addr);
            tokens.push(sock.token);
        }

        let fin_state = FinState {
            next_token: run_loop.next_token,
            client_ids,
            addrs,
            tokens,
        };
        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));

        Response::Ok
    }
}

impl Miot {
    fn prefix(&self) -> String {
        format!("{}:{}", self.name, self.miot_id)
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            _ => unreachable!(),
        }
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}
