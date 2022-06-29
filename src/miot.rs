use log::{debug, error, info, trace};

use std::{collections::BTreeMap, net, ops::Deref, sync::Arc, time};

use crate::packet::{PacketRead, PacketWrite};
use crate::thread::{Rx, Thread, Threadable};
use crate::{queue, v5, ClientID, Config, Flush, Packetize, Shard};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Miot {
    /// Human readable name for this miot thread.
    pub name: String,
    /// Same as the shard-id.
    pub miot_id: u32,
    /// Read timeout on MQTT socket. Refer [Config::mqtt_read_timeout]
    pub read_timeout: u32,
    /// Write timeout on MQTT socket. Refer [Config::mqtt_write_timeout]
    pub write_timeout: u32,
    /// Flush timeout for connection shutdown. Refer [Config::mqtt_flush_timeout]
    pub flush_timeout: u32,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Arc<mio::Waker>, Thread<Miot, Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Shard instance that is paired with this miot thread.
    shard: Box<Shard>,
    /// Mio poller for asynchronous handling.
    poll: mio::Poll,
    /// collection of all active socket connections abstracted as queue.
    conns: BTreeMap<ClientID, queue::Socket>,
    /// next available token for connections
    next_token: mio::Token,
    /// whether thread is closed.
    closed: bool,
}

impl Default for Miot {
    fn default() -> Miot {
        let config = Config::default();
        let mut def = Miot {
            name: format!("{}-miot-init", config.name),
            miot_id: u32::default(),
            read_timeout: config.mqtt_read_timeout(),
            write_timeout: config.mqtt_write_timeout(),
            flush_timeout: config.mqtt_flush_timeout(),
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
            Inner::Handle(_waker, _thrd) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Miot {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    /// Create a miot thread from configuration. Miot shall be in `Init` state, to start
    /// the miot thread call [Miot::spawn].
    pub fn from_config(config: Config, miot_id: u32) -> Result<Miot> {
        let m = Miot::default();
        let mut val = Miot {
            name: m.name.clone(),
            miot_id,
            read_timeout: config.mqtt_read_timeout(),
            write_timeout: config.mqtt_write_timeout(),
            flush_timeout: config.mqtt_flush_timeout(),
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, shard: Shard) -> Result<Miot> {
        use crate::FIRST_TOKEN;

        if matches!(&self.inner, Inner::Handle(_, _) | Inner::Main(_)) {
            err!(InvalidInput, desc: "miot can be spawned only in init-state ")?;
        }

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let miot = Miot {
            name: format!("{}-miot-main", self.config.name),
            miot_id: self.miot_id,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            flush_timeout: self.flush_timeout,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                shard: Box::new(shard),
                poll,
                conns: BTreeMap::default(),
                next_token: FIRST_TOKEN,
                closed: false,
            }),
        };
        let thrd = Thread::spawn(&self.prefix, miot);

        let val = Miot {
            name: format!("{}-miot-handle", self.config.name),
            miot_id: self.miot_id,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            flush_timeout: self.flush_timeout,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };

        Ok(val)
    }
}

// calls to interface with listener-thread, and shall wake the thread
impl Miot {
    pub fn add_connection(
        &self,
        client_id: ClientID,
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        upstream: queue::QueueTx,
    ) -> Result<queue::QueueTx> {
        let req = Request::AddConnection { client_id, conn, addr, upstream };
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                match thrd.request(req)?? {
                    Response::Downstream(tx) => Ok(tx),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn close_wait(mut self) -> Result<Miot> {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                thrd.request(Request::Close)??;
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

pub enum Request {
    AddConnection {
        client_id: ClientID,
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        upstream: queue::QueueTx,
    },
    Close,
}

pub enum Response {
    Ok,
    Downstream(queue::QueueTx),
}

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::REQ_CHANNEL_SIZE;

        info!("{} spawn ...", self.prefix);

        let mut events = mio::Events::with_capacity(REQ_CHANNEL_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(self.prefix, self.as_mut_poll().poll(&mut events, timeout));

            let exit = self.mio_events(&rx, &events);
            if exit {
                break;
            }

            // wake the shard
            allow_panic!(self.prefix, self.as_shard().wake());
        }

        self.handle_close(Request::Close);
        info!("{} thread exit...", self.prefix);

        self
    }
}

impl Miot {
    // return (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &mio::Events) -> bool {
        let mut count = 0;
        for event in events.iter() {
            trace!("{} poll-event token:{}", self.prefix, event.token().0);
            count += 1;
        }
        debug!("{} polled {} events", self.prefix, count);

        let exit = loop {
            // keep repeating until all control requests are drained.
            match self.control_chan(rx) {
                (_empty, true) => break true,
                (true, _disconnected) => break false,
                (false, false) => (),
            }
        };

        self.read_conns();
        self.write_conns();

        exit
    }

    // Return (empty, disconnected)
    fn control_chan(&mut self, rx: &ThreadRx) -> (bool, bool) {
        use crate::{thread::pending_requests, REQ_CHANNEL_SIZE};
        use Request::*;

        let closed = match &self.inner {
            Inner::Main(RunLoop { closed, .. }) => *closed,
            _ => unreachable!(),
        };

        let (mut qs, empty, disconnected) = pending_requests(&rx, REQ_CHANNEL_SIZE);

        if closed {
            info!("{} skipping {} requests closed:{}", self.prefix, qs.len(), closed);
            qs.drain(..);
        } else {
            debug!("{} process {} requests closed:{}", self.prefix, qs.len(), closed);
        }

        for q in qs.into_iter() {
            match q {
                (q @ AddConnection { .. }, Some(tx)) => {
                    let resp = self.handle_add_connection(q);
                    allow_panic!(self.prefix, tx.send(Ok(resp)));
                }
                (q @ Close, Some(tx)) => {
                    allow_panic!(self.prefix, tx.send(Ok(self.handle_close(q))));
                }
                (_, _) => unreachable!(),
            }
        }

        (empty, disconnected)
    }
}

impl Miot {
    fn read_conns(&mut self) {
        let (shard, conns, closed) = match &mut self.inner {
            Inner::Main(RunLoop { shard, conns, closed, .. }) => (shard, conns, *closed),
            _ => unreachable!(),
        };

        if closed {
            info!("{} skipping read connections, closed:{}", self.prefix, closed);
        } else {
            debug!("{} processing read connections. closed:{}", self.prefix, closed);
        }

        // if thread is closed, conns will be empty.
        let mut fail_queues = vec![];
        for (client_id, queue) in conns.iter_mut() {
            let prefix = format!("rconn:{}:{}", queue.addr, client_id.deref());
            match Self::read_packets(&prefix, self.read_timeout, queue) {
                Ok(_) => (),
                Err(err) => fail_queues.push((client_id.clone(), err)),
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let queue = conns.remove(&client_id).unwrap();
            let prefix = format!("rconn:{}:{}", queue.addr, *client_id);
            error!("{} removing connection {} ...", prefix, err);
            err!(IPCFail, try: shard.failed_connection(client_id)).ok();

            let flush = Flush {
                prefix,
                err: Some(err),
                queue,
                flush_timeout: self.flush_timeout,
            };
            let _thrd = Thread::spawn_sync("flush", 1, flush);
        }
    }

    // return (would_block,)
    // Disconnected, MalformedPacket, ProtocolError, RxClosed
    fn read_packets(
        prefix: &str,
        timeout: u32,
        queue: &mut queue::Socket,
    ) -> Result<bool> {
        use crate::MSG_CHANNEL_SIZE;

        // before reading from socket, send remaining packets to shard.
        let upstream_block = Self::send_upstream(prefix, queue)?;
        match upstream_block {
            true => Ok(true),
            false => loop {
                match Self::read_packet(prefix, timeout, queue)? {
                    (Some(pkt), _) if queue.rd.packets.len() < MSG_CHANNEL_SIZE => {
                        queue.rd.packets.push(pkt);
                    }
                    (Some(pkt), would_block) => {
                        queue.rd.packets.push(pkt);
                        Self::send_upstream(prefix, queue)?;
                        break Ok(would_block);
                    }
                    (None, would_block) => {
                        Self::send_upstream(prefix, queue)?;
                        break Ok(would_block);
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
        timeout: u32,
        queue: &mut queue::Socket,
    ) -> Result<(Option<v5::Packet>, bool)> {
        use crate::packet::PacketRead::{Fin, Header, Init, Remain};
        use std::mem;

        let pr = mem::replace(&mut queue.rd.pr, PacketRead::default());

        let (mut pr, would_block) = pr.read(&queue.conn)?;
        let pkt = match &pr {
            Init { .. } | Header { .. } | Remain { .. } if !queue.read_elapsed() => {
                trace!("{} read retrying", prefix);
                queue.set_read_timeout(true, timeout);
                None
            }
            Init { .. } | Header { .. } | Remain { .. } => {
                queue.set_read_timeout(false, timeout);
                err!(Disconnected, desc: "{} fail after {:?}", prefix, queue.rd.timeout)?
            }
            Fin { .. } => {
                queue.set_read_timeout(false, timeout);
                let pkt = pr.parse()?;
                pr = pr.reset();
                Some(pkt)
            }
            PacketRead::None => unreachable!(),
        };

        let _pr_none = mem::replace(&mut queue.rd.pr, pr);
        Ok((pkt, would_block))
    }

    // return (would_block,)
    // RxClosed, if the receiving end of the queue, in shard/session, has closed.
    pub fn send_upstream(prefix: &str, queue: &mut queue::Socket) -> Result<bool> {
        use std::sync::mpsc;

        let mut iter = {
            let packets: Vec<v5::Packet> = queue.rd.packets.drain(..).collect();
            packets.into_iter()
        };
        let res = loop {
            match iter.next() {
                Some(packet) => match queue.rd.tx.try_send(packet) {
                    Ok(()) => (),
                    Err(mpsc::TrySendError::Full(p)) => {
                        queue.rd.packets.push(p);
                        break Ok(true);
                    }
                    Err(mpsc::TrySendError::Disconnected(p)) => {
                        queue.rd.packets.push(p);
                        err!(
                            RxClosed,
                            desc: "{} upstream queue closed",
                            prefix
                        )?;
                    }
                },
                None => break Ok(false),
            }
        };

        iter.for_each(|p| queue.rd.packets.push(p));
        res
    }

    fn write_conns(&mut self) {
        let (shard, conns, closed) = match &mut self.inner {
            Inner::Main(RunLoop { shard, conns, closed, .. }) => (shard, conns, *closed),
            _ => unreachable!(),
        };

        if closed {
            info!("{} skipping write connections, closed:{}", self.prefix, closed);
        } else {
            debug!("{} processing write connections, closed:{}", self.prefix, closed);
        }

        // if thread is closed conns will be empty.
        let mut fail_queues = vec![];
        for (client_id, queue) in conns.iter_mut() {
            let prefix = format!("wconn:{}:{}", queue.addr, client_id.deref());
            match Self::write_packets(&prefix, self.write_timeout, queue) {
                Ok(_) => (),
                Err(err) => fail_queues.push((client_id.clone(), err)),
            }
        }

        for (client_id, err) in fail_queues.into_iter() {
            let queue = conns.remove(&client_id).unwrap();
            let prefix = format!("wconn:{}:{}", queue.addr, *client_id);
            error!("{} removing connection {} ...", prefix, err);
            err!(IPCFail, try: shard.failed_connection(client_id)).ok();

            let flush = Flush {
                prefix,
                err: Some(err),
                queue,
                flush_timeout: self.flush_timeout,
            };
            let _thrd = Thread::spawn_sync("flush", 1, flush);
        }
    }

    // write packets to connection, return (would_block,)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    // TxFinish, if the transmitting end of the queue, in shard/session, has closed.
    fn write_packets(
        prefix: &str,
        timeout: u32,
        queue: &mut queue::Socket,
    ) -> Result<bool> {
        use crate::MSG_CHANNEL_SIZE;

        // before reading from socket, send remaining packets to connection.
        let would_block = Self::flush_packets(prefix, timeout, queue)?;
        match would_block {
            true => Ok(true),
            false => match rx_packets(&queue.wt.rx, MSG_CHANNEL_SIZE) {
                (qs, _empty, true) => {
                    queue.wt.packets.extend_from_slice(&qs);
                    Self::flush_packets(prefix, timeout, queue)?;
                    err!(TxFinish, desc: "{} upstream finished", prefix)
                }
                (qs, _empty, _disconnected) => {
                    queue.wt.packets.extend_from_slice(&qs);
                    Self::flush_packets(prefix, timeout, queue)?;
                    Ok(false)
                }
            },
        }
    }

    // return (would_block,)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    pub fn flush_packets(
        prefix: &str,
        timeout: u32,
        queue: &mut queue::Socket,
    ) -> Result<bool> {
        use std::mem;

        let mut pw = mem::replace(&mut queue.wt.pw, PacketWrite::default());
        let mut iter = {
            let iter = queue.wt.packets.drain(..);
            iter.collect::<Vec<v5::Packet>>().into_iter()
        };
        let would_block = loop {
            if Self::write_packet(prefix, timeout, queue)? {
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

        iter.for_each(|p| queue.wt.packets.push(p));
        let _pw_none = mem::replace(&mut queue.wt.pw, pw);

        Ok(would_block)
    }

    // return (would_block,)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    fn write_packet(
        prefix: &str,
        timeout: u32,
        queue: &mut queue::Socket,
    ) -> Result<bool> {
        use crate::packet::PacketWrite::{Fin, Init, Remain};
        use std::mem;

        let pw = mem::replace(&mut queue.wt.pw, PacketWrite::default());

        let (pw, _would_block) = pw.write(&queue.conn)?;
        let res = match &pw {
            Init { .. } | Remain { .. } if !queue.write_elapsed() => {
                trace!("{} write retrying", prefix);
                queue.set_write_timeout(true, timeout);
                Ok(true)
            }
            Init { .. } | Remain { .. } => {
                queue.set_write_timeout(false, timeout);
                err!(Disconnected, desc: "{} fail after {:?}", prefix, queue.wt.timeout)
            }
            Fin { .. } => {
                queue.set_write_timeout(false, timeout);
                Ok(false)
            }
            PacketWrite::None => unreachable!(),
        };

        let _pw_none = mem::replace(&mut queue.wt.pw, pw);
        res
    }
}

impl Miot {
    fn handle_add_connection(&mut self, req: Request) -> Response {
        use crate::{MAX_PACKET_SIZE, MSG_CHANNEL_SIZE};
        use mio::Interest;

        let (client_id, mut conn, addr, tx) = match req {
            Request::AddConnection { client_id, conn, addr, upstream } => {
                (client_id, conn, addr, upstream)
            }
            _ => unreachable!(),
        };
        let (poll, conns, token) = match &mut self.inner {
            Inner::Main(RunLoop { poll, conns, next_token, .. }) => {
                let token = *next_token;
                *next_token = mio::Token(next_token.0 + 1);
                (poll, conns, token)
            }
            _ => unreachable!(),
        };

        let interests = Interest::READABLE | Interest::WRITABLE;
        allow_panic!(self.prefix, poll.registry().register(&mut conn, token, interests));

        let (downstream, rx) = queue::queue_channel(MSG_CHANNEL_SIZE);

        let rd = queue::Source {
            pr: PacketRead::new(MAX_PACKET_SIZE), // TODO: use configurable values
            timeout: None,
            tx,
            packets: Vec::default(),
        };
        let wt = queue::Sink {
            pw: PacketWrite::new(&[], MAX_PACKET_SIZE), // TODO: use configurable values
            timeout: None,
            rx,
            packets: Vec::default(),
        };
        let id = client_id.clone();
        let queue = queue::Socket { client_id, conn, addr, token, rd, wt };
        conns.insert(id, queue);

        Response::Downstream(downstream)
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        use std::mem;

        let RunLoop { shard, conns, closed, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        if *closed == false {
            info!("{} connections:{} ...", self.prefix, conns.len());

            let shard = mem::replace(shard, Box::new(Shard::default()));
            mem::drop(shard);

            let conns = mem::replace(conns, BTreeMap::default());
            for (cid, queue) in conns.into_iter() {
                info!(
                    "{} closing socket {:?} client-id:{:?}",
                    self.prefix, queue.addr, *cid
                );
                mem::drop(queue.conn);
            }
            *closed = true;
        }
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

    fn as_shard(&self) -> &Shard {
        match &self.inner {
            Inner::Main(RunLoop { shard, .. }) => shard,
            _ => unreachable!(),
        }
    }
}

/// Return (requests, empty, disconnected)
pub fn rx_packets(rx: &queue::QueueRx, max: usize) -> (Vec<v5::Packet>, bool, bool) {
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
