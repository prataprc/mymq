use log::{debug, error, info, trace};

use std::{collections::BTreeMap, net, ops::Deref, sync::Arc, time};

use crate::packet::{MQTTRead, MQTTWrite};
use crate::thread::{Rx, Thread, Threadable};
use crate::{queue, v5, AppTx, ClientID, Config, Packetize, Shard};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Miot {
    /// Human readable name for this miot thread.
    pub name: String,
    /// Same as the shard-id.
    pub miot_id: u32,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    // Help by Shard.
    Handle(Arc<mio::Waker>, Thread<Miot, Request, Result<Response>>),
    // Thread.
    Main(RunLoop),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState),
}

pub struct RunLoop {
    /// Mio poller for asynchronous handling, aggregate events from remote client and
    /// thread-waker.
    poll: mio::Poll,
    /// Shard-tx associated with the shard that is paired with this miot thread.
    shard: Box<Shard>,

    /// next available token for connections
    next_token: mio::Token,
    /// collection of all active socket connections, and its associated data.
    conns: BTreeMap<ClientID, queue::Socket>,

    /// Back channel to communicate with application.
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
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => info!("{} invalid drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Miot {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

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
        use crate::FIRST_TOKEN;

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

                next_token: FIRST_TOKEN,
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
}

pub enum Request {
    AddConnection(AddConnectionArgs),
    RemoveConnection { client_id: ClientID },
    Close,
}

pub enum Response {
    Ok,
    Removed(queue::Socket),
}

pub struct AddConnectionArgs {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub upstream: queue::PktTx,
    pub downstream: queue::PktRx,
    pub client_max_packet_size: u32,
}

// calls to interface with miot-thread, and shall wake the thread
impl Miot {
    pub fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, _thrd) => err!(IOError, try: waker.wake(), "miot-wake"),
            _ => unreachable!(),
        }
    }
    pub fn add_connection(&self, args: AddConnectionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(_waker, thrd) => {
                thrd.request(Request::AddConnection(args))??;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    pub fn remove_connection(&self, client_id: ClientID) -> Result<queue::Socket> {
        match &self.inner {
            Inner::Handle(_waker, thrd) => {
                match thrd.request(Request::RemoveConnection { client_id })?? {
                    Response::Removed(socket) => Ok(socket),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn close_wait(mut self) -> Miot {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(_waker, thrd) => {
                thrd.request(Request::Close).ok();
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::POLL_EVENTS_SIZE;

        info!("{} spawn ...", self.prefix);

        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(&self, self.as_mut_poll().poll(&mut events, timeout));

            match self.mio_events(&rx, &events) {
                true /*disconnected*/ => break,
                false => (),
            };

            // TODO: We do granular wake up, in read_conns and write_conns, remove this ?
            // allow_panic!(&self, self.as_shard().wake());
        }

        self.handle_close(Request::Close);

        info!("{} thread exit...", self.prefix);
        self
    }
}

impl Miot {
    // return (disconnected,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &mio::Events) -> bool {
        let mut count = 0;
        for event in events.iter() {
            trace!("{} poll-event token:{}", self.prefix, event.token().0);
            count += 1;
        }
        debug!("{} polled {} events", self.prefix, count);

        let disconnected = loop {
            // keep repeating until all control requests are drained.
            match self.drain_control_chan(rx) {
                (_empty, true) => break true,
                (true, _disconnected) => break false,
                (false, false) => (),
            }
        };

        if !disconnected && !matches!(&self.inner, Inner::Close(_)) {
            self.read_conns();
            self.write_conns();
        }

        disconnected
    }

    // Return (empty, disconnected)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (bool, bool) {
        use crate::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let (mut qs, empty, disconnected) = pending_requests(&rx, CONTROL_CHAN_SIZE);

        if matches!(&self.inner, Inner::Close(_)) {
            info!("{} skipping {} requests closed:true", self.prefix, qs.len());
            qs.drain(..);
        } else {
            debug!("{} process {} requests closed:false", self.prefix, qs.len());
        }

        for q in qs.into_iter() {
            match q {
                (q @ AddConnection { .. }, Some(tx)) => {
                    allow_panic!(&self, tx.send(Ok(self.handle_add_connection(q))));
                }
                (q @ RemoveConnection { .. }, Some(tx)) => {
                    allow_panic!(&self, tx.send(Ok(self.handle_remove_connection(q))));
                }
                (q @ Close, Some(tx)) => {
                    allow_panic!(&self, tx.send(Ok(self.handle_close(q))));
                }
                (_, _) => unreachable!(),
            }
        }

        (empty, disconnected)
    }
}

impl Miot {
    fn read_conns(&mut self) {
        let (shard, conns) = match &mut self.inner {
            Inner::Main(RunLoop { shard, conns, .. }) => (shard, conns),
            _ => unreachable!(),
        };
        let shard = shard.to_tx();

        let mut fail_queues = vec![];
        for (client_id, socket) in conns.iter_mut() {
            let prefix = format!("rconn:{}:{}", socket.addr, client_id.deref());
            match Self::read_packets(&prefix, &self.config, socket) {
                Ok((_would_block, true)) => allow_panic!(&self, shard.wake()),
                Ok((_would_block, false)) => (),
                Err(err) => {
                    error!("{} failed read_packets ...", prefix);
                    fail_queues.push((client_id.clone(), err));
                }
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            match self.handle_remove_connection(req) {
                Response::Removed(socket) => {
                    allow_panic!(&self, shard.flush_connection(socket, err));
                }
                Response::Ok => (),
            }
        }
    }

    // return (would_block,wake)
    // Disconnected, MalformedPacket, ProtocolError, RxClosed
    fn read_packets(
        prefix: &str,
        config: &Config,
        socket: &mut queue::Socket,
    ) -> Result<(bool, bool)> {
        let msg_batch_size = config.mqtt_msg_batch_size() as usize;

        // before reading from socket, send remaining packets to shard.
        let (upstream_block, mut wake) = Self::send_upstream(prefix, socket)?;
        match upstream_block {
            true => Ok((true, wake)),
            false => loop {
                match Self::read_packet(prefix, config, socket)? {
                    (Some(pkt), _) if socket.rd.packets.len() < msg_batch_size => {
                        socket.rd.packets.push(pkt);
                    }
                    (Some(pkt), would_block) => {
                        socket.rd.packets.push(pkt);
                        let (_, w) = Self::send_upstream(prefix, socket)?;
                        wake = wake | w;
                        break Ok((would_block, wake));
                    }
                    (None, would_block) => {
                        let (_, w) = Self::send_upstream(prefix, socket)?;
                        wake = wake | w;
                        break Ok((would_block, wake));
                    }
                }
            },
        }
    }

    // return (packet,would_block)
    // Disconnected, and implies a bad connection.
    // MalformedPacket, implies a DISCONNECT and socket close
    // ProtocolError, implies DISCONNECT and socket close
    fn read_packet(
        prefix: &str,
        config: &Config,
        socket: &mut queue::Socket,
    ) -> Result<(Option<v5::Packet>, bool)> {
        use crate::packet::MQTTRead::{Fin, Header, Init, Remain};
        use std::mem;

        let timeout = config.mqtt_read_timeout();
        let pr = mem::replace(&mut socket.rd.pr, MQTTRead::default());

        let (mut pr, would_block) = pr.read(&socket.conn)?;
        let pkt = match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !socket.read_elapsed() => {
                trace!("{} read retrying", prefix);
                socket.set_read_timeout(true, timeout);
                None
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                socket.set_read_timeout(false, timeout);
                err!(Disconnected, desc: "{} fail after {:?}", prefix, socket.rd.timeout)?
            }
            Fin { .. } => {
                socket.set_read_timeout(false, timeout);
                let pkt = pr.parse()?;
                pr = pr.reset();
                Some(pkt)
            }
            MQTTRead::None => unreachable!(),
        };

        let _pr_none = mem::replace(&mut socket.rd.pr, pr);
        Ok((pkt, would_block))
    }

    // return (would_block,wake)
    // RxClosed, if the receiving end of the queue, in shard/session, has closed.
    pub fn send_upstream(
        prefix: &str,
        socket: &mut queue::Socket,
    ) -> Result<(bool, bool)> {
        use std::sync::mpsc;

        let mut iter = {
            let packets: Vec<v5::Packet> = socket.rd.packets.drain(..).collect();
            packets.into_iter()
        };
        let session_tx = &socket.rd.session_tx;
        let mut wake = false;
        let res = loop {
            match iter.next() {
                Some(packet) => match session_tx.try_send(packet) {
                    Ok(()) => wake = true,
                    Err(mpsc::TrySendError::Full(p)) => {
                        socket.rd.packets.push(p);
                        break Ok((true, wake));
                    }
                    Err(mpsc::TrySendError::Disconnected(p)) => {
                        socket.rd.packets.push(p);
                        err!(
                            RxClosed,
                            desc: "{} upstream queue closed",
                            prefix
                        )?;
                    }
                },
                None => break Ok((false, wake)),
            }
        };

        iter.for_each(|p| socket.rd.packets.push(p));
        res
    }

    fn write_conns(&mut self) {
        let (shard, conns) = match &mut self.inner {
            Inner::Main(RunLoop { shard, conns, .. }) => (shard, conns),
            _ => unreachable!(),
        };
        let shard = shard.to_tx();

        // if thread is closed conns will be empty.
        let mut fail_queues = vec![];
        for (client_id, socket) in conns.iter_mut() {
            let prefix = format!("wconn:{}:{}", socket.addr, client_id.deref());
            match Self::write_packets(&prefix, &self.config, socket) {
                Ok(false) => allow_panic!(&self, shard.wake()),
                Ok(_would_block) => (),
                Err(err) => {
                    error!("{} failed write_packets ...", prefix);
                    fail_queues.push((client_id.clone(), err));
                }
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let req = Request::RemoveConnection { client_id };
            match self.handle_remove_connection(req) {
                Response::Removed(socket) => {
                    allow_panic!(&self, shard.flush_connection(socket, err));
                }
                _ => unreachable!(),
            }
        }
    }

    // write packets to connection, return (would_block,wake)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    // TxFinish, if the transmitting end of the queue, in shard/session, has closed.
    fn write_packets(
        prefix: &str,
        config: &Config,
        socket: &mut queue::Socket,
    ) -> Result<bool> {
        let msg_batch_size = config.mqtt_msg_batch_size() as usize;

        // before reading from socket, send remaining packets to connection.
        let would_block = Self::flush_packets(prefix, config, socket)?;
        match would_block {
            true => Ok(true),
            false => match rx_packets(&socket.wt.miot_rx, msg_batch_size) {
                (qs, _empty, true) => {
                    socket.wt.packets.extend_from_slice(&qs);
                    Self::flush_packets(prefix, config, socket)?;
                    err!(TxFinish, desc: "{} upstream finished", prefix)
                }
                (qs, _empty, _disconnected) => {
                    socket.wt.packets.extend_from_slice(&qs);
                    Self::flush_packets(prefix, config, socket)?;
                    Ok(false)
                }
            },
        }
    }

    // return (would_block,)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    pub fn flush_packets(
        prefix: &str,
        config: &Config,
        socket: &mut queue::Socket,
    ) -> Result<bool> {
        use std::mem;

        let mut pw = mem::replace(&mut socket.wt.pw, MQTTWrite::default());
        let mut iter = {
            let iter = socket.wt.packets.drain(..);
            iter.collect::<Vec<v5::Packet>>().into_iter()
        };
        let would_block = loop {
            if Self::write_packet(prefix, config, socket)? {
                break true;
            }

            match iter.next() {
                Some(packet) => {
                    let blob = match packet.encode() {
                        Ok(blob) => blob,
                        Err(err) => {
                            let pt = packet.to_packet_type();
                            error!("{} skipping packet {:?} : {}", prefix, pt, err);
                            continue;
                        }
                    };
                    pw = pw.reset(blob.as_ref());
                }
                None => break false,
            }
        };

        iter.for_each(|p| socket.wt.packets.push(p));
        let _pw_none = mem::replace(&mut socket.wt.pw, pw);

        Ok(would_block)
    }

    // return (would_block,)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    fn write_packet(
        prefix: &str,
        config: &Config,
        socket: &mut queue::Socket,
    ) -> Result<bool> {
        use crate::packet::MQTTWrite::{Fin, Init, Remain};
        use std::mem;

        let timeout = config.mqtt_write_timeout();
        let pw = mem::replace(&mut socket.wt.pw, MQTTWrite::default());

        let (pw, _would_block) = pw.write(&socket.conn)?;
        let res = match &pw {
            Init { .. } | Remain { .. } if !socket.write_elapsed() => {
                trace!("{} write retrying", prefix);
                socket.set_write_timeout(true, timeout);
                Ok(true)
            }
            Init { .. } | Remain { .. } => {
                socket.set_write_timeout(false, timeout);
                err!(Disconnected, desc: "{} fail after {:?}", prefix, socket.wt.timeout)
            }
            Fin { .. } => {
                socket.set_write_timeout(false, timeout);
                Ok(false)
            }
            MQTTWrite::None => unreachable!(),
        };

        let _pw_none = mem::replace(&mut socket.wt.pw, pw);
        res
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

        let AddConnectionArgs {
            client_id,
            mut conn,
            addr,
            upstream,
            downstream,
            client_max_packet_size,
        } = match req {
            Request::AddConnection(args) => args,
            _ => unreachable!(),
        };
        let (session_tx, miot_rx) = (upstream, downstream);

        let interests = Interest::READABLE | Interest::WRITABLE;
        allow_panic!(&self, poll.registry().register(&mut conn, token, interests));

        let rd = queue::Source {
            pr: MQTTRead::new(self.config.mqtt_max_packet_size()),
            timeout: None,
            session_tx,
            packets: Vec::default(),
        };
        let wt = queue::Sink {
            pw: MQTTWrite::new(&[], client_max_packet_size),
            timeout: None,
            miot_rx,
            packets: Vec::default(),
        };
        let id = client_id.clone();
        let socket = queue::Socket { client_id, conn, addr, token, rd, wt };
        conns.insert(id, socket);

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
        use std::mem;

        match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => {
                info!("{} closing connections:{} ...", self.prefix, run_loop.conns.len());

                mem::drop(run_loop.poll);
                mem::drop(run_loop.shard);

                let mut client_ids = vec![];
                let mut addrs = vec![];
                let mut tokens = vec![];

                for (cid, socket) in run_loop.conns.into_iter() {
                    info!(
                        "{} closing socket {:?} client-id:{:?}",
                        self.prefix, socket.addr, *cid
                    );
                    client_ids.push(socket.client_id);
                    addrs.push(socket.addr);
                    tokens.push(socket.token);
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
            Inner::Close(_) => Response::Ok,
            _ => unreachable!(),
        }
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

/// Return (requests, empty, disconnected)
pub fn rx_packets(rx: &queue::PktRx, max: usize) -> (Vec<v5::Packet>, bool, bool) {
    use std::sync::mpsc;

    let mut reqs = vec![];
    loop {
        match rx.try_recv() {
            Ok(req) if reqs.len() < max => reqs.push(req),
            Ok(req) => {
                reqs.push(req);
                break (reqs, false, false);
            }
            Err(mpsc::TryRecvError::Disconnected) => break (reqs, false, true),
            Err(mpsc::TryRecvError::Empty) => break (reqs, true, false),
        }
    }
}
