use log::{debug, error, info};
use mio::Events;

use std::{collections::BTreeMap, net, sync::Arc, time};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{ClientID, Config, Shard};
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
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Shard instance that is paired with this miot thread.
    shard: Box<Shard>,
    /// Mio poller for asynchronous handling.
    poll: mio::Poll,
    /// Tokens registered with poll, note that, for every new connection, a new token
    /// shall be assigned.
    tokens: BTreeMap<mio::Token, ClientID>,
    /// collection of all active collection of socket connections.
    conns: BTreeMap<ClientID, Connection>,
    /// next available token for connections
    next_token: mio::Token,
    /// whether thread is closed.
    closed: bool,
}

struct Connection {
    client_id: ClientID,
    conn: mio::net::TcpStream,
    addr: net::SocketAddr,
    token: mio::Token,
}

impl Default for Miot {
    fn default() -> Miot {
        use crate::CHANNEL_SIZE;

        let config = Config::default();
        Miot {
            name: format!("{}-miot-init", config.name),
            chan_size: CHANNEL_SIZE,
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
            Inner::Tx(_waker, _tx) => info!("{} drop ...", self.pp()),
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
                tokens: BTreeMap::default(),
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

    pub fn to_tx(&self) -> Self {
        info!("{} cloning tx ...", self.pp());

        let inner = match &self.inner {
            Inner::Handle(waker, thrd) => Inner::Tx(Arc::clone(waker), thrd.to_tx()),
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            _ => unreachable!(),
        };

        Miot {
            name: format!("{}-miot-tx", self.config.name),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
    }
}

// calls to interface with listener-thread, and shall wake the thread
impl Miot {
    pub fn add_connection(
        &self,
        client_id: ClientID,
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
    ) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                thrd.request(Request::AddConnection { client_id, conn, addr })??;
                Ok(waker.wake()?)
            }
            _ => unreachable!(),
        }
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
    },
    Close,
}

pub enum Response {
    Ok,
}

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Self {
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
                Err(err) => {
                    break Err(err);
                }
            };
        };

        match res {
            Ok(()) => {
                info!("{} thread normal exit...", self.pp());
            }
            Err(err) => {
                error!("{} fatal error, try restarting thread `{}`", self.pp(), err);
                // TODO: allow_panic!(self.as_cluster().restart_miot());
            }
        }

        self
    }
}

impl Miot {
    // return whether we are doing normal exit.
    fn mio_events(&mut self, _rx: &ThreadRx, _es: &Events) -> Result<bool> {
        todo!()
        //loop {
        //    match self.mio_chan(rx)? {
        //        (_empty, true) => break Ok(true),
        //        (false, false) => continue,
        //        (true, false) => (),
        //    }
        //    match self.mio_conns(es)? {
        //        empty if !empty => continue,
        //        _ => break Ok(false),
        //    }
        //}
        //debug!("{} polled and got {} events", self.pp(), es.len());
    }

    // Return (empty, disconnected)
    fn mio_chan(&mut self, _rx: &Rx<Request, Result<Response>>) -> Result<(bool, bool)> {
        todo!()
        //use crate::thread::pending_requests;
        //use Request::*;

        //let closed = match &self.inner {
        //    Inner::Main(RunLoop { closed, .. }) => *closed,
        //    _ => unreachable!(),
        //};

        //let (qs, empty, disconnected) = pending_requests(&rx, self.chan_size);

        //debug!("{} processing {} requests closed:{} ...", self.pp(), qs.len(), closed);
        //for q in qs.into_iter() {
        //    if closed {
        //        continue;
        //    }
        //    match q {
        //        (q @ AddConnection { .. }, Some(tx)) => {
        //            err!(IPCFail, try: tx.send(self.handle_add_connection(q)))?
        //        }
        //        (Close, Some(tx)) => {
        //            err!(IPCFail, try: tx.send(self.handle_close()))?
        //        }
        //        (_, _) => unreachable!(),
        //    }
        //}
        //Ok((empty, disconnected))
    }

    // Return empty
    fn mio_conns(&mut self, _es: Vec<mio::event::Event>) -> Result<bool> {
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

        let (client_id, mut conn, addr) = match req {
            Request::AddConnection { client_id, conn, addr } => (client_id, conn, addr),
            _ => unreachable!(),
        };
        let (poll, tokens, conns, token) = match &mut self.inner {
            Inner::Main(RunLoop { poll, tokens, conns, next_token, .. }) => {
                (poll, tokens, conns, mio::Token(next_token.0 + 1))
            }
            _ => unreachable!(),
        };

        poll.registry().register(
            &mut conn,
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;
        tokens.insert(token, client_id.clone());
        conns.insert(client_id.clone(), Connection { client_id, conn, addr, token });

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
        for (cid, conntn) in conns.into_iter() {
            info!("{} closing socket {:?} client-id:{:?}", prefix, conntn.addr, cid);
            mem::drop(conntn.conn);
        }

        let poll = mem::replace(poll, mio::Poll::new()?);
        mem::drop(poll);

        Ok(Response::Ok)
    }
}

impl Miot {
    fn pp(&self) -> String {
        format!("{}", self.name)
    }

    fn get_connection(&self, token: mio::Token) -> &Connection {
        match &self.inner {
            Inner::Main(RunLoop { tokens, conns, .. }) => {
                let client_id = tokens.get(&token).unwrap();
                let connt = conns.get(client_id).unwrap();
                connt
            }
            _ => unreachable!(),
        }
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
