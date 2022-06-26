use log::{debug, error, info, trace};

use std::{collections::BTreeMap, net, sync::Arc, time};

use crate::packet::{PacketRead, PacketWrite};
use crate::thread::{Rx, Thread, Threadable};
use crate::{queue, v5, ClientID, Config, Shard};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Miot {
    /// Human readable name for this miot thread.
    pub name: String,
    /// Same as the shard-id.
    pub miot_id: usize,
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
            miot_id: usize::default(),
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
    pub fn from_config(config: Config, miot_id: usize) -> Result<Miot> {
        let m = Miot::default();
        let mut val = Miot {
            name: m.name.clone(),
            miot_id,
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
            match Self::read_packets(&self.prefix, queue) {
                Ok(()) => (),
                Err(err) if err.kind() == ErrorKind::RxClosed => {
                    // upstream queue, in shard/session is closed.
                    fail_queues.push((client_id.clone(), false));
                }
                Err(err) if err.kind() == ErrorKind::BadPacket => {
                    fail_queues.push((client_id.clone(), true)); // connt has gone bad
                }
                Err(err) if err.kind() == ErrorKind::Disconnected => {
                    fail_queues.push((client_id.clone(), true)); // connt has gone bad
                }
                Err(_err) => unreachable!(),
            }
        }

        for (client_id, up_flush) in fail_queues.into_iter() {
            let mut queue = conns.remove(&client_id).unwrap();
            let mut packets: Vec<v5::Packet> = Vec::default();
            if up_flush {
                packets = queue.rd.packets.drain(..).collect()
            }
            error!("{} removing queue {:?}", self.prefix, *client_id);
            ignore_error!(
                self.prefix,
                "shard.failed_connection",
                err!(IPCFail, try: shard.failed_connection(client_id, packets))
            );
        }
    }

    // return packets from connection and send it upstream.
    // Disconnected, BadPacket, RxClosed
    fn read_packets(prefix: &str, queue: &mut queue::Socket) -> Result<()> {
        use crate::MSG_CHANNEL_SIZE;

        // before reading from socket, send remaining packets to shard.
        let upstream_block = Self::send_upstream(prefix, queue)?;
        match upstream_block {
            true => Ok(()),
            false => loop {
                match Self::read_packet(prefix, queue)? {
                    Some(pkt) if queue.rd.packets.len() < MSG_CHANNEL_SIZE => {
                        queue.rd.packets.push(pkt);
                    }
                    Some(pkt) => {
                        queue.rd.packets.push(pkt);
                        Self::send_upstream(prefix, queue)?;
                        break Ok(());
                    }
                    None => {
                        Self::send_upstream(prefix, queue)?;
                        break Ok(());
                    }
                }
            },
        }
    }

    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    // BadPacket, if data from connection has gone bad, same as Disconnected.
    fn read_packet(
        prefix: &str,
        queue: &mut queue::Socket,
    ) -> Result<Option<v5::Packet>> {
        use crate::MAX_SOCKET_RETRY;
        use std::mem;

        let mut pr = mem::replace(&mut queue.rd.pr, PacketRead::default());
        let res = loop {
            let (val, retry, would_block) = pr.read(&queue.conn)?;
            pr = val;

            if would_block {
                break Ok(None);
            } else if retry && queue.rd.retries < MAX_SOCKET_RETRY {
                queue.rd.retries += 1;
            } else if retry {
                err!(
                    Disconnected,
                    desc: "{} fail after {} retries",
                    prefix,
                    queue.rd.retries
                )?;
            } else {
                match pr.parse() {
                    Ok(pkt) => {
                        pr = pr.reset();
                        break Ok(Some(pkt));
                    }
                    Err(err) => err!(
                        BadPacket, desc: "{} parse failed {}", prefix, err
                    )?,
                }
            };
        };

        let _pr_none = mem::replace(&mut queue.rd.pr, pr);
        res
    }

    // return (would_block,)
    // RxClosed, if the receiving end of the queue, in shard/session, has closed.
    fn send_upstream(prefix: &str, queue: &mut queue::Socket) -> Result<bool> {
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
        use crate::MAX_FLUSH_RETRY;

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
            match Self::write_packets(&self.prefix, queue) {
                Ok(()) => (),
                Err(err) if err.kind() == ErrorKind::TxFinish => {
                    match Self::write_downstream(&self.prefix, queue) {
                        Ok(false) => fail_queues.push(client_id.clone()),
                        Ok(true) if queue.wt.flush_retries < MAX_FLUSH_RETRY => {
                            queue.wt.flush_retries += 1;
                        }
                        Ok(true) => {
                            error!(
                                "{} flush retry failed after {}, ignoring {} packets",
                                self.prefix,
                                queue.wt.flush_retries,
                                queue.wt.packets.len()
                            );
                            fail_queues.push(client_id.clone())
                        }
                        Err(err) if err.kind() == ErrorKind::Disconnected => {
                            fail_queues.push(client_id.clone())
                        }
                        Err(_err) => unreachable!(),
                    }
                }
                Err(err) if err.kind() == ErrorKind::Disconnected => {
                    fail_queues.push(client_id.clone());
                }
                Err(_err) => unreachable!(),
            }
        }

        for client_id in fail_queues.into_iter() {
            let _queue = conns.remove(&client_id);
            conns.remove(&client_id);
            error!("{} removing queue {:?}", self.prefix, *client_id);
            ignore_error!(
                self.prefix,
                "shard.failed_connection",
                err!(IPCFail, try: shard.failed_connection(client_id, vec![]))
            );
        }
    }

    // write packets to connection.
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    // TxFinish, if the transmitting end of the queue, in shard/session, has closed.
    fn write_packets(prefix: &str, queue: &mut queue::Socket) -> Result<()> {
        use crate::MSG_CHANNEL_SIZE;

        // before reading from socket, send remaining packets to connection.
        let downstream_block = Self::write_downstream(prefix, queue)?;
        match downstream_block {
            true => Ok(()),
            false => match rx_packets(&queue.wt.rx, MSG_CHANNEL_SIZE) {
                (qs, _empty, true) => {
                    queue.wt.packets = qs;
                    err!(
                        TxFinish,
                        desc: "{} upstream tx-channel finished", prefix
                    )?;
                    Ok(())
                }
                (qs, _empty, _disconnected) => {
                    queue.wt.packets = qs;
                    Self::write_downstream(prefix, queue)?;
                    Ok(())
                }
            },
        }
    }

    // return (would_block,)
    // Disconnected, if connection has gone bad or attempted maximum retries on socket.
    fn write_downstream(prefix: &str, queue: &mut queue::Socket) -> Result<bool> {
        use crate::{Packetize, MAX_SOCKET_RETRY};
        use std::mem;

        let mut iter = {
            let packets: Vec<v5::Packet> = queue.wt.packets.drain(..).collect();
            packets.into_iter()
        };
        let mut pw = mem::replace(&mut queue.wt.pw, PacketWrite::default());
        let would_block = loop {
            match iter.next() {
                Some(packet) => {
                    let blob = match packet.encode() {
                        Ok(blob) => blob,
                        Err(err) => {
                            error!(
                                "{} skipping packet {:?} to {:?} : {}",
                                prefix,
                                packet.to_packet_type(),
                                *queue.client_id,
                                err
                            );
                            continue;
                        }
                    };

                    pw = pw.reset(blob.as_ref());
                    let would_block = loop {
                        let (val, retry, would_block) = pw.write(&mut queue.conn)?;
                        pw = val;

                        if would_block {
                            break true;
                        } else if retry && queue.wt.retries < MAX_SOCKET_RETRY {
                            queue.wt.retries += 1;
                        } else if retry {
                            err!(
                                Disconnected,
                                desc: "{} fail after {} retries",
                                prefix,
                                queue.wt.retries
                            )?;
                        } else {
                            break false;
                        }
                    };

                    if would_block {
                        break true;
                    }
                }
                None => break false,
            }
        };

        let _pw = mem::replace(&mut queue.wt.pw, pw);
        Ok(would_block)
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
            retries: 0,
            tx,
            packets: Vec::default(),
        };
        let wt = queue::Sink {
            pw: PacketWrite::new(&[], MAX_PACKET_SIZE), // TODO: use configurable values
            retries: 0,
            rx,
            packets: Vec::default(),
            flush_retries: 0,
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
fn rx_packets(rx: &queue::QueueRx, max: usize) -> (Vec<v5::Packet>, bool, bool) {
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
