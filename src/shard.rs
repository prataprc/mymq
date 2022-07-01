use log::{debug, error, info, trace};
use uuid::Uuid;

use std::{collections::BTreeMap, net, sync::Arc};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{queue, v5};
use crate::{ClientID, Cluster, Config, Flusher, Miot, Session, Shardable, TopicTrie};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Shard {
    /// Human readable name for shard.
    pub name: String,
    /// Shard id, must unique withing the [Cluster].
    pub shard_id: u32,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    // Held by Cluster.
    Handle(Handle),
    // Held by Miot, Session(s).
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    // Held by all Shard threads.
    MsgTx(Arc<mio::Waker>, queue::MsgTx),
    Main(RunLoop),
}

pub struct Handle {
    waker: Arc<mio::Waker>,
    thrd: Thread<Shard, Request, Result<Response>>,
    msg_tx: queue::MsgTx,
}

pub struct RunLoop {
    /// Mio poller for asynchronous handling, all events are from consensus port and
    /// thread-waker.
    poll: mio::Poll,
    /// Cluster::Tx handle to communicate back to cluster.
    /// Shall be dropped after close_wait call, when the thread returns, will point
    /// to Inner::Init
    cluster: Box<Cluster>,
    /// Flusher::Tx handle to communicate with flusher.
    flusher: Flusher,
    /// Inner::Handle to corresponding miot-thread.
    /// Shall be dropped after close_wait call, when the thread returns, will point
    /// to Inner::Init
    miot: Miot,

    /// Every shard has an input queue for queue::Message, which receives all locally
    /// hopping MQTT messages for sessions managed by this shard.
    msg_rx: queue::MsgRx,
    /// Collection of sessions and corresponding clients managed by this shard.
    /// Shall be dropped after close_wait call, when the thread returns, will be empty.
    // TODO: remove session only when Session::session_rx channel has disconnected.
    sessions: BTreeMap<ClientID, Session>,
    /// Corresponding Tx handle for all other shards, as Shard::MsgTx,
    shard_queues: BTreeMap<u32, Shard>,
    /// MVCC clone of Cluster::topic_filters
    topic_filters: TopicTrie,

    /// whether thread is closed.
    closed: bool,
}

impl Default for Shard {
    fn default() -> Shard {
        let config = Config::default();
        let mut def = Shard {
            name: format!("{}-shard-init", config.name),
            shard_id: u32::default(),
            uuid: Uuid::new_v4(),
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
            Inner::Handle(_hndl) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Tx(_waker, _tx) => info!("{} drop ...", self.prefix),
            Inner::MsgTx(_waker, _tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

pub struct SpawnArgs {
    pub cluster: Cluster,
    pub flusher: Flusher,
    pub topic_filters: TopicTrie,
}

impl Shard {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    pub fn from_config(config: Config, shard_id: u32) -> Result<Shard> {
        let def = Shard::default();
        let mut val = Shard {
            name: def.name.clone(),
            shard_id,
            uuid: def.uuid,
            prefix: def.prefix.clone(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, args: SpawnArgs) -> Result<Shard> {
        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "shard can be spawned only in init-state ")?;
        }

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        // This is the local queue that carries queue::Message from one local-session
        // to another local-session. Note that the queue is shared by all the sessions
        // in this shard, hence the queue-capacity is correspondingly large.
        let (msg_tx, msg_rx) = {
            let size = self.config.mqtt_msg_batch_size() * self.config.num_shards();
            queue::msg_channel(size as usize)
        };
        let shard = Shard {
            name: format!("{}-shard-main", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                cluster: Box::new(args.cluster),
                flusher: args.flusher,
                miot: Miot::default(),

                msg_rx,
                sessions: BTreeMap::default(),
                shard_queues: BTreeMap::default(),
                topic_filters: args.topic_filters,

                closed: false,
            }),
        };
        let thrd = Thread::spawn(&self.prefix, shard);

        let shard = Shard {
            name: format!("{}-shard-handle", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Handle(Handle { waker, thrd, msg_tx }),
        };
        {
            let (config, miot_id) = (self.config.clone(), self.shard_id);
            let miot = Miot::from_config(config, miot_id)?.spawn(shard.to_tx())?;
            match &shard.inner {
                Inner::Handle(Handle { waker, thrd, .. }) => {
                    waker.wake()?;
                    thrd.request(Request::SetMiot(miot))??;
                }
                _ => unreachable!(),
            }
        }

        Ok(shard)
    }

    pub fn to_tx(&self) -> Self {
        trace!("{} cloning tx ...", self.prefix);

        let inner = match &self.inner {
            Inner::Handle(Handle { waker, thrd, .. }) => {
                Inner::Tx(Arc::clone(waker), thrd.to_tx())
            }
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            _ => unreachable!(),
        };

        let mut shard = Shard {
            name: format!("{}-shard-tx", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        };
        shard.prefix = self.prefix();
        shard
    }

    pub fn to_msg_tx(&self) -> Self {
        trace!("{} cloning tx ...", self.prefix);

        let inner = match &self.inner {
            Inner::Handle(Handle { waker, msg_tx, .. }) => {
                Inner::MsgTx(Arc::clone(waker), msg_tx.clone())
            }
            _ => unreachable!(),
        };

        Shard {
            name: format!("{}-shard-tx", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        }
    }
}

pub struct AddSessionArgs {
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub pkt: v5::Connect,
}

pub enum Request {
    SetMiot(Miot),
    SetShardQueues(BTreeMap<u32, Shard>),
    AddSession(AddSessionArgs),
    FailedConnection { socket: queue::Socket, err: Error },
    Close,
}

pub enum Response {
    Ok,
}

// calls to interfacw with cluster-thread.
impl Shard {
    pub fn set_shard_queues(&self, shards: BTreeMap<u32, Shard>) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { waker, thrd, .. }) => {
                waker.wake()?;
                thrd.request(Request::SetShardQueues(shards))??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn add_session(&self, args: AddSessionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { waker, thrd, .. }) => {
                waker.wake()?;
                thrd.request(Request::AddSession(args))??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn failed_connection(&self, socket: queue::Socket, err: Error) -> Result<()> {
        match &self.inner {
            Inner::Tx(waker, tx) => {
                waker.wake()?;
                tx.post(Request::FailedConnection { socket, err })?;
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { waker, .. }) => {
                err!(IOError, try: waker.wake(), "shard-wake")
            }
            Inner::Tx(waker, _) => err!(IOError, try: waker.wake(), "shard-wake"),
            _ => unreachable!(),
        }
    }

    pub fn close_wait(mut self) -> Result<Shard> {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(Handle { waker, thrd, .. }) => {
                waker.wake()?;
                thrd.request(Request::Close)??;
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

impl Threadable for Shard {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use std::time;

        info!("{} spawn ...", self.prefix);

        let mut events = mio::Events::with_capacity(crate::POLL_EVENTS_SIZE);
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
            match self.drain_control_chan(rx) {
                (_empty, true) => break true,
                (true, _disconnected) => break false,
                (false, false) => (),
            }
        }
    }
}

impl Shard {
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (bool, bool) {
        use crate::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let closed = match &self.inner {
            Inner::Main(RunLoop { closed, .. }) => *closed,
            _ => unreachable!(),
        };

        let (mut qs, empty, disconnected) = pending_requests(&rx, CONTROL_CHAN_SIZE);

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
                (q @ SetShardQueues(_), Some(tx)) => {
                    let resp = self.handle_set_shard_queues(q);
                    allow_panic!(self.prefix, tx.send(Ok(resp)));
                }
                (q @ AddSession { .. }, Some(tx)) => {
                    let resp = self.handle_add_session(q);
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

    fn handle_set_shard_queues(&mut self, req: Request) -> Response {
        let shard_queues = match req {
            Request::SetShardQueues(shard_queues) => shard_queues,
            _ => unreachable!(),
        };
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        run_loop.shard_queues = shard_queues;
        Response::Ok
    }

    fn handle_add_session(&mut self, req: Request) -> Response {
        use crate::session::SessionArgs;

        let AddSessionArgs { conn, addr, pkt } = match req {
            Request::AddSession(args) => args,
            _ => unreachable!(),
        };
        let RunLoop { sessions, miot, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        // This queue is wired up with miot-thread. This queue carries queue::Packet,
        // and there is a separate queue for every session.
        let (upstream, session_rx) = {
            let size = self.config.mqtt_msg_batch_size() as usize;
            queue::pkt_channel(size)
        };
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
                session_rx,
            };
            Session::start(args, self.config.clone(), pkt)
        };
        sessions.insert(client_id.clone(), session);

        Response::Ok
    }

    fn handle_failed_connection(&mut self, req: Request) {
        let (socket, err) = match req {
            Request::FailedConnection { socket, err } => (socket, err),
            _ => unreachable!(),
        };
        let RunLoop { flusher, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        // NOTE: Session shall be removed when session_rx says disconnected.
        err!(IPCFail, try: flusher.flush_connection_err(socket, err)).ok();
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
