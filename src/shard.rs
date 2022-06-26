use log::{debug, error, info, trace};
use uuid::Uuid;

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::{collections::BTreeMap, net, sync::Arc};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{queue, v5, ClientID, Cluster, Config, Miot, Session, Shardable};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Shard {
    /// Human readable name for shard.
    pub name: String,
    /// Shard id, must unique withing the [Cluster].
    pub shard_id: usize,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,
    /// Active count of sessions maintained by this shard.
    pub n_sessions: AtomicUsize, // used by cluster as well.
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Arc<mio::Waker>, Thread<Shard, Request, Result<Response>>),
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Cluster::Tx to communicate back to cluster.
    /// Shall be dropped after close_wait call, when the thread returns, will point
    /// to Inner::Init
    cluster: Box<Cluster>,
    /// Mio poller for asynchronous handling.
    poll: mio::Poll,
    /// Collection of sessions and corresponding clients managed by this shard.
    /// Shall be dropped after close_wait call, when the thread returns, will be empty.
    sessions: BTreeMap<ClientID, Session>,
    /// Unlike `sessions` that are managed by this shard, `subscribers` are full set of
    /// all sessions that are managed by all shards, including this one.
    subscribers: BTreeMap<ClientID, queue::QueueTx>,
    /// Inner::Handle to corresponding miot-thread.
    /// Shall be dropped after close_wait call, when the thread returns, will point
    /// to Inner::Init
    miot: Miot,
    /// whether thread is closed.
    closed: bool,
}

impl Default for Shard {
    fn default() -> Shard {
        let config = Config::default();
        let mut def = Shard {
            name: format!("{}-shard-init", config.name),
            shard_id: usize::default(),
            uuid: Uuid::new_v4(),
            n_sessions: AtomicUsize::new(0),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Shardable for Shard {
    fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Tx(_waker, _tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Shard {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    pub fn from_config(config: Config, shard_id: usize) -> Result<Shard> {
        let def = Shard::default();
        let mut val = Shard {
            name: def.name.clone(),
            shard_id,
            uuid: def.uuid,
            n_sessions: AtomicUsize::new(0),
            prefix: def.prefix.clone(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster) -> Result<Shard> {
        if matches!(&self.inner, Inner::Handle(_, _) | Inner::Main(_)) {
            err!(InvalidInput, desc: "shard can be spawned only in init-state ")?;
        }

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let shard = Shard {
            name: format!("{}-shard-main", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            n_sessions: AtomicUsize::new(0),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                cluster: Box::new(cluster),
                poll,
                sessions: BTreeMap::default(),
                subscribers: BTreeMap::default(),
                miot: Miot::default(),
                closed: false,
            }),
        };
        let thrd = Thread::spawn(&self.prefix, shard);

        let shard = Shard {
            name: format!("{}-shard-handle", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            n_sessions: AtomicUsize::new(0),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };
        {
            let (config, miot_id) = (self.config.clone(), self.shard_id);
            let miot = Miot::from_config(config, miot_id)?.spawn(shard.to_tx())?;
            match &shard.inner {
                Inner::Handle(_waker, thrd) => {
                    thrd.request(Request::SetMiot(miot))??;
                }
                _ => unreachable!(),
            }
        }

        Ok(shard)
    }

    pub fn to_tx(&self) -> Self {
        info!("{} cloning tx ...", self.prefix);

        let inner = match &self.inner {
            Inner::Handle(waker, thrd) => Inner::Tx(Arc::clone(waker), thrd.to_tx()),
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            _ => unreachable!(),
        };

        Shard {
            name: format!("{}-shard-tx", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            n_sessions: AtomicUsize::new(0),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        }
    }
}

// calls to interfacw with cluster-thread.
impl Shard {
    pub fn add_session(
        &self,
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    ) -> Result<queue::QueueTx> {
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                match thrd.request(Request::AddSession { conn, addr, pkt })?? {
                    Response::SubscribedTx(tx) => Ok(tx),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn book_session(&self, client_id: ClientID, tx: queue::QueueTx) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                thrd.request(Request::BookSession { client_id, subscribed_tx: tx })??;
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn unbook_session(&self, client_id: ClientID) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                thrd.request(Request::UnBookSession { client_id })??;
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn failed_connection(&self, id: ClientID, ps: Vec<v5::Packet>) -> Result<()> {
        match &self.inner {
            Inner::Tx(waker, tx) => {
                waker.wake()?;
                tx.post(Request::FailedConnection { client_id: id, packets: ps })?;
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, _) => err!(IOError, try: waker.wake(), "shard-wake"),
            Inner::Tx(waker, _) => err!(IOError, try: waker.wake(), "shard-wake"),
            _ => unreachable!(),
        }
    }

    pub fn close_wait(mut self) -> Result<Shard> {
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
    SetMiot(Miot),
    AddSession {
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    },
    BookSession {
        client_id: ClientID,
        subscribed_tx: queue::QueueTx,
    },
    UnBookSession {
        client_id: ClientID,
    },
    FailedConnection {
        client_id: ClientID,
        packets: Vec<v5::Packet>,
    },
    Close,
}

pub enum Response {
    Ok,
    SubscribedTx(queue::QueueTx),
}

impl Threadable for Shard {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::REQ_CHANNEL_SIZE;
        use std::time;

        info!("{} spawn ...", self.prefix);

        let mut events = mio::Events::with_capacity(REQ_CHANNEL_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(self.prefix, self.as_mut_poll().poll(&mut events, timeout));

            let exit = self.mio_events(&rx, &events);
            if exit {
                break;
            }
        }

        self.handle_close(Request::Close);
        info!("{} thread exit ...", self.prefix);

        self
    }
}

impl Shard {
    // (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &mio::Events) -> bool {
        let mut count = 0;
        for event in events.iter() {
            trace!("{} poll-event token:{}", self.prefix, event.token().0);
            count += 1;
        }
        debug!("{} polled {} events", self.prefix, count);

        loop {
            // keep repeating until all control requests are drained.
            match self.control_chan(rx) {
                (_empty, true) => break true,
                (true, _disconnected) => break false,
                (false, false) => (),
            }
        }
    }
}

impl Shard {
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
                (q @ SetMiot(_), Some(tx)) => {
                    let resp = self.handle_set_miot(q);
                    allow_panic!(self.prefix, tx.send(Ok(resp)));
                }
                (q @ AddSession { .. }, Some(tx)) => {
                    let resp = self.handle_add_session(q);
                    allow_panic!(self.prefix, tx.send(Ok(resp)));
                }
                (q @ BookSession { .. }, Some(tx)) => {
                    let resp = self.handle_book_session(q);
                    allow_panic!(self.prefix, tx.send(Ok(resp)));
                }
                (q @ UnBookSession { .. }, Some(tx)) => {
                    let resp = self.handle_unbook_session(q);
                    allow_panic!(self.prefix, tx.send(Ok(resp)));
                }
                (q @ FailedConnection { .. }, None) => {
                    self.handle_failed_connection(q);
                }
                (q @ Close, Some(tx)) => {
                    allow_panic!(self.prefix, tx.send(Ok(self.handle_close(q))))
                }

                (_, _) => unreachable!(),
            };
        }

        (empty, disconnected)
    }
}

impl Shard {
    fn handle_set_miot(&mut self, req: Request) -> Response {
        let miot = match req {
            Request::SetMiot(miot) => miot,
            _ => unreachable!(),
        };
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        run_loop.miot = miot;
        Response::Ok
    }

    fn handle_add_session(&mut self, req: Request) -> Response {
        use crate::{queue::queue_channel, session::SessionArgs, MSG_CHANNEL_SIZE};

        let (conn, addr, pkt) = match req {
            Request::AddSession { conn, addr, pkt } => (conn, addr, pkt),
            _ => unreachable!(),
        };
        let RunLoop { sessions, subscribers, miot, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let (upstream, miot_rx) = queue_channel(MSG_CHANNEL_SIZE);
        let client_id = pkt.payload.client_id.clone();
        let miot_tx = {
            let res = miot.add_connection(client_id.clone(), conn, addr, upstream);
            allow_panic!(self.prefix, res)
        };

        let session = {
            let args = SessionArgs {
                addr,
                client_id: client_id.clone(),
                miot_tx,
                miot_rx,
            };
            Session::from_args(args, pkt)
        };
        let subscribed_tx = session.to_subscribed_tx();
        sessions.insert(client_id.clone(), session);
        subscribers.insert(client_id.clone(), subscribed_tx.clone());

        self.n_sessions.fetch_add(1, SeqCst);
        Response::SubscribedTx(subscribed_tx)
    }

    fn handle_book_session(&mut self, req: Request) -> Response {
        let (client_id, subscribed_tx) = match req {
            Request::BookSession { client_id, subscribed_tx } => {
                (client_id, subscribed_tx)
            }
            _ => unreachable!(),
        };
        let RunLoop { subscribers, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        subscribers.insert(client_id, subscribed_tx);
        Response::Ok
    }

    fn handle_unbook_session(&mut self, req: Request) -> Response {
        let client_id = match req {
            Request::UnBookSession { client_id } => client_id,
            _ => unreachable!(),
        };
        let RunLoop { sessions, subscribers, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        sessions.remove(&client_id);
        subscribers.remove(&client_id);
        Response::Ok
    }

    fn handle_failed_connection(&mut self, req: Request) {
        let (client_id, packets) = match req {
            Request::FailedConnection { client_id, packets } => (client_id, packets),
            _ => unreachable!(),
        };
        let RunLoop { cluster, sessions, subscribers, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        subscribers.remove(&client_id);
        match sessions.remove(&client_id) {
            Some(session) => {
                // TODO: handle the final batch of messages from this connection.
                session.do_mqtt_packets(packets);
                session.close();
            }
            None => (),
        }
        allow_panic!(self.prefix, cluster.remove_connection(client_id));
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        use std::mem;

        let RunLoop { cluster, sessions, miot, closed, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        if *closed == false {
            info!("{} sessions:{} ...", self.prefix, sessions.len());

            *miot = match mem::replace(miot, Miot::default()).close_wait() {
                Ok(miot) => miot,
                Err(err) => {
                    error!("{} miot close_wait failed, ignored {} ...", self.prefix, err);
                    Miot::default()
                }
            };

            let mut new_sessions = BTreeMap::default();
            let iter = mem::replace(sessions, BTreeMap::default()).into_iter();
            for (client_id, sess) in iter {
                new_sessions.insert(client_id, sess.close());
            }
            let _sesss = mem::replace(sessions, new_sessions);

            let cluster = mem::replace(cluster, Box::new(Cluster::default()));
            mem::drop(cluster);

            *closed = true;
        }
        Response::Ok
    }
}

impl Shard {
    pub fn num_sessions(&self) -> usize {
        self.n_sessions.load(SeqCst)
    }

    fn prefix(&self) -> String {
        format!("{}:{}", self.name, self.shard_id)
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            _ => unreachable!(),
        }
    }
}
