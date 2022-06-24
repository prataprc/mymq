use log::{debug, error, info};
use mio::Events;

use std::{collections::BTreeMap, net, sync::Arc, time};

use crate::packet::{PacketRead, PacketWrite};
use crate::queue::{Queue, QueueRx, QueueTx, SocketRd, SocketWt};
use crate::thread::{Rx, Thread, Threadable};
use crate::{v5, ClientID, Config, Shard};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Miot {
    /// Human readable name for this miot thread.
    pub name: String,
    /// Input channel size for the miot thread.
    pub chan_size: usize,
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
    /// collection of all active collection of socket connections.
    conns: BTreeMap<ClientID, Queue>,
    /// next available token for connections
    next_token: mio::Token,
    /// whether thread is closed.
    closed: bool,
}

impl Default for Miot {
    fn default() -> Miot {
        use crate::REQ_CHANNEL_SIZE;

        let config = Config::default();
        Miot {
            name: format!("{}-miot-init", config.name),
            chan_size: REQ_CHANNEL_SIZE,
            config,
            inner: Inner::Init,
        }
    }
}

impl Drop for Miot {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.pp()),
            Inner::Handle(_waker, _thrd) => {
                error!("{} invalid drop ...", self.pp());
                panic!("{} invalid drop ...", self.pp());
            }
            Inner::Main(_run_loop) => info!("{} drop ...", self.pp()),
        }
    }
}

impl Miot {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    /// Create a miot thread from configuration. Miot shall be in `Init` state, to start
    /// the miot thread call [Miot::spawn].
    pub fn from_config(config: Config) -> Result<Miot> {
        let m = Miot::default();
        let val = Miot {
            name: format!("{}-miot-init", config.name),
            chan_size: m.chan_size,
            config: config.clone(),
            inner: Inner::Init,
        };

        Ok(val)
    }

    pub fn spawn(self, shard: Shard) -> Result<Miot> {
        use crate::FIRST_TOKEN;
        use mio::Waker;

        if matches!(&self.inner, Inner::Handle(_, _) | Inner::Main(_)) {
            err!(InvalidInput, desc: "miot can be spawned only in init-state ")?;
        }

        let poll = mio::Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let miot = Miot {
            name: format!("{}-miot-main", self.config.name),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                shard: Box::new(shard),
                poll,
                conns: BTreeMap::default(),
                next_token: FIRST_TOKEN,
                closed: false,
            }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, miot);

        let val = Miot {
            name: format!("{}-miot-handle", self.config.name),
            chan_size: self.chan_size,
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
        rd_chan: QueueTx,
        wt_chan: QueueRx,
    ) -> Result<()> {
        use Request::AddConnection;

        match &self.inner {
            Inner::Handle(waker, thrd) => {
                thrd.request(AddConnection { client_id, conn, addr, rd_chan, wt_chan })??;
                waker.wake()?;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn close_wait(mut self) -> Result<Miot> {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(waker, thrd) => {
                thrd.request(Request::Close)??;
                waker.wake()?;
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
        rd_chan: QueueTx,
        wt_chan: QueueRx,
    },
    Close,
}

pub enum Response {
    Ok,
}

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        info!("{} spawn chan_size:{} ...", self.pp(), self.chan_size);

        let mut events = mio::Events::with_capacity(2);
        let res = loop {
            let timeout: Option<time::Duration> = None;
            match self.as_mut_poll().poll(&mut events, timeout) {
                Ok(()) => (),
                Err(err) => {
                    break err!(IOError, try: Err(err), "{} poll error", self.pp());
                }
            }

            match self.mio_events(&rx, &events) {
                Ok(true) => break Ok(()),
                Ok(false) => (),
                Err(err) => break Err(err),
            };
        };

        match res {
            Ok(()) => {
                info!("{} thread normal exit...", self.pp());
            }
            Err(err) => {
                error!("{} fatal error, try restarting thread `{}`", self.pp(), err);
                allow_panic!(self.as_shard().restart_miot());
            }
        }

        self
    }
}

impl Miot {
    // return whether we are doing normal exit, which is rx-disconnected
    fn mio_events(&mut self, rx: &ThreadRx, _es: &Events) -> Result<bool> {
        let res = loop {
            match self.mio_chan(rx)? {
                (_empty, true) => break Ok(true),
                (false, _disconnected) => break Ok(false),
                (true, false) => todo!(),
            }
        };

        while !self.mio_conns()? {}

        debug!("{} polled", self.pp()); // TODO: log number of events.
        res
    }

    // Return (empty, disconnected)
    fn mio_chan(&mut self, rx: &Rx<Request, Result<Response>>) -> Result<(bool, bool)> {
        use crate::thread::pending_requests;
        use Request::*;

        let closed = match &self.inner {
            Inner::Main(RunLoop { closed, .. }) => *closed,
            _ => unreachable!(),
        };

        let (mut qs, empty, disconnected) = pending_requests(&rx, self.chan_size);

        if closed {
            info!("{} skipping {} requests closed:{}", self.pp(), qs.len(), closed);
            qs.drain(..);
        } else {
            debug!("{} process {} requests closed:{}", self.pp(), qs.len(), closed);
        }

        for q in qs.into_iter() {
            match q {
                (q @ AddConnection { .. }, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_add_connection(q)))?
                }
                (Close, Some(tx)) => err!(IPCFail, try: tx.send(self.handle_close()))?,
                (_, _) => unreachable!(),
            }
        }
        Ok((empty, disconnected))
    }

    // Return empty
    fn mio_conns(&mut self) -> Result<bool> {
        todo!()
        //use std::io;

        //let (server, cluster) = match &self.inner {
        //    Inner::Main(RunLoop { server, cluster, .. }) => (server, cluster),
        //    _ => unreachable!(),
        //};

        //let (conn, addr) = match server.as_ref().unwrap().accept() {
        //    Ok((conn, addr)) => (conn, addr),
        //    Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(true),
        //    Err(err) => {
        //        error!("{} accept failed {}", self.pp(), err);
        //        err!(IOError, try: Err(err))?
        //    }
        //};

        //let hs = Handshake {
        //    conn: Some(conn),
        //    addr,
        //    cluster: cluster.to_tx(),
        //    retries: 0,
        //};
        //let _thrd = Thread::spawn_sync("handshake", 1, hs);

        //Ok(false)
    }

    fn handle_add_connection(&mut self, req: Request) -> Result<Response> {
        use mio::Interest;

        let (client_id, mut conn, addr, msg_tx, msg_rx) = match req {
            Request::AddConnection { client_id, conn, addr, rd_chan, wt_chan } => {
                (client_id, conn, addr, rd_chan, wt_chan)
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

        poll.registry().register(
            &mut conn,
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;

        {
            let rd = SocketRd {
                pr: PacketRead::new(),
                retry: false,
                retries: 0,
                msg_tx,
                packets: Vec::default(),
            };
            let wt = SocketWt {
                pw: PacketWrite::new(&[]),
                msg_rx,
                packets: Vec::default(),
            };
            let id = client_id.clone();
            let queue = Queue { client_id, conn, addr, token, rd, wt };
            conns.insert(id, queue);
        }

        Ok(Response::Ok)
    }

    fn handle_close(&mut self) -> Result<Response> {
        use std::mem;

        let prefix = self.pp();
        let RunLoop { shard, poll, conns, closed, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };
        *closed = true;

        info!("{} connections:{} ...", prefix, conns.len());

        let shard = mem::replace(shard, Box::new(Shard::default()));
        mem::drop(shard);

        let conns = mem::replace(conns, BTreeMap::default());
        for (cid, queue) in conns.into_iter() {
            info!("{} closing socket {:?} client-id:{:?}", prefix, queue.addr, cid);
            mem::drop(queue.conn);
        }

        let poll = mem::replace(poll, mio::Poll::new()?);
        mem::drop(poll);

        Ok(Response::Ok)
    }
}

impl Miot {
    fn read_conns(&mut self) -> Result<()> {
        let prefix = self.pp();

        let (conns, closed) = match &mut self.inner {
            Inner::Main(RunLoop { conns, closed, .. }) => (conns, *closed),
            _ => unreachable!(),
        };

        if closed {
            info!("{} skipping closed:{}", prefix, closed);
        } else {
            debug!("{} process closed:{}", prefix, closed);
        }

        // if thread is closed, conns will be empty.
        for (_, queue) in conns.iter_mut() {
            Self::read_packets(&prefix, queue)?;
        }

        Ok(())
    }

    // return packets from connection.
    fn read_packets(prefix: &str, queue: &mut Queue) -> Result<()> {
        use crate::MSG_CHANNEL_SIZE;

        // before reading from socket, send packets to shard.
        let upstream_block = Self::send_upstream(prefix, queue)?;
        match upstream_block {
            true => Ok(()),
            false => {
                //
                loop {
                    match Self::read_packet(prefix, queue)? {
                        Some(pkt) if queue.rd.packets.len() < MSG_CHANNEL_SIZE => {
                            queue.rd.packets.push(pkt);
                        }
                        Some(pkt) => {
                            Self::send_upstream(prefix, queue)?;
                            queue.rd.packets.push(pkt);
                            break Ok(());
                        }
                        None => {
                            Self::send_upstream(prefix, queue)?;
                            break Ok(());
                        }
                    }
                }
            }
        }
    }

    fn read_packet(prefix: &str, queue: &mut Queue) -> Result<Option<v5::Packet>> {
        use crate::MAX_SOCKET_RETRY;
        use std::mem;

        let mut pr = mem::replace(&mut queue.rd.pr, PacketRead::default());
        let res = loop {
            let (val, retry, block) = pr.read(&queue.conn)?;
            pr = val;

            if block {
                break Ok(None);
            } else if retry && queue.rd.retries < MAX_SOCKET_RETRY {
                queue.rd.retries += 1;
            } else if retry {
                err!(
                    InsufficientBytes,
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
                        IOError, desc: "{} parse failed {}", prefix, err
                    )?,
                }
            };
        };

        let _pr_none = mem::replace(&mut queue.rd.pr, pr);
        res
    }

    // return (block,)
    fn send_upstream(prefix: &str, queue: &mut Queue) -> Result<bool> {
        use std::sync::mpsc;

        let mut iter = {
            let packets: Vec<v5::Packet> = queue.rd.packets.drain(..).collect();
            packets.into_iter()
        };
        let res = loop {
            match iter.next() {
                Some(packet) => match queue.rd.msg_tx.try_send(packet) {
                    Ok(()) => (),
                    Err(mpsc::TrySendError::Full(p)) => {
                        queue.rd.packets.push(p);
                        break Ok(true);
                    }
                    Err(mpsc::TrySendError::Disconnected(p)) => {
                        queue.rd.packets.push(p);
                        err!(
                            Disconnected,
                            desc: "{} upstream channel disconnected",
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
        todo!()
    }

    fn write_packets(&mut self) {
        todo!()
    }

    fn write_packet(&mut self) {
        todo!()
    }

    fn send_downstream(&mut self) {
        todo!()
    }
}

impl Miot {
    fn pp(&self) -> String {
        format!("{}", self.name)
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
