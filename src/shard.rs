use log::{debug, error, info, trace, warn};
use uuid::Uuid;

use std::{collections::BTreeMap, mem, net, sync::Arc};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{message, session, socket, v5};
use crate::{AppTx, ClientID, Config, RetainedTrie, Session, Shardable, SubscribedTrie};
use crate::{Cluster, Flusher, Message, Miot, MsgRx, QueueStatus, Socket};
use crate::{Error, ErrorKind, ReasonCode, Result};

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
    // Held by Miot.
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    // Held by all Shard threads.
    MsgTx(Arc<mio::Waker>, message::MsgTx),
    // Thread.
    Main(RunLoop),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState),
}

pub struct Handle {
    waker: Arc<mio::Waker>,
    thrd: Thread<Shard, Request, Result<Response>>,
    msg_tx: message::MsgTx,
}

pub struct RunLoop {
    /// Mio poller for asynchronous handling, all events are from consensus port and
    /// thread-waker.
    poll: mio::Poll,
    /// Self waker
    waker: Arc<mio::Waker>,
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

    /// Collection of sessions and corresponding clients managed by this shard.
    /// Shall be dropped after close_wait call, when the thread returns, will be empty.
    // TODO: remove session only when Session::session_rx channel has disconnected.
    sessions: BTreeMap<ClientID, Session>,

    /// Corresponding MsgTx handle for all other shards, as Shard::MsgTx,
    shard_queues: BTreeMap<u32, Shard>,
    /// MVCC clone of Cluster::topic_filters
    topic_filters: SubscribedTrie,
    /// MVCC clone of Cluster::retained_messages
    retained_messages: RetainedTrie,

    /// Back channel to communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
    pub miot: Miot,
    pub sessions: BTreeMap<ClientID, session::SessionStats>,
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
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_hndl) => info!("{} drop ...", self.prefix),
            Inner::Tx(_waker, _tx) => info!("{} drop ...", self.prefix),
            Inner::MsgTx(_waker, _tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => info!("{} drop ...", self.prefix),
        }
    }
}

pub struct SpawnArgs {
    pub cluster: Cluster,
    pub flusher: Flusher,
    pub topic_filters: SubscribedTrie,
    pub retained_messages: RetainedTrie,
}

impl Shard {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    pub fn from_config(config: Config, shard_id: u32) -> Result<Shard> {
        let def = Shard::default();
        let mut val = Shard {
            name: format!("{}-shard-init", config.name),
            shard_id,
            uuid: def.uuid,
            prefix: def.prefix.clone(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, args: SpawnArgs, app_tx: AppTx) -> Result<Shard> {
        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "shard can be spawned only in init-state ")?;
        }

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        // This is the local queue that carries [Message] from one local-session
        // to another local-session. Note that the queue is shared by all the sessions
        // in this shard, hence the queue-capacity is correspondingly large.
        let (msg_tx, msg_rx) = {
            let size = self.config.mqtt_msg_batch_size() * self.config.num_shards();
            message::msg_channel(self.shard_id, size as usize, Arc::clone(&waker))
        };
        let mut shard = Shard {
            name: format!("{}-shard-main", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                waker,
                cluster: Box::new(args.cluster),
                flusher: args.flusher,
                miot: Miot::default(),

                sessions: BTreeMap::default(),
                shard_queues: BTreeMap::default(),
                topic_filters: args.topic_filters,
                retained_messages: args.retained_messages,

                app_tx: app_tx.clone(),
            }),
        };
        shard.prefix = shard.prefix();
        let mut thrd = Thread::spawn(&self.prefix, shard);
        thrd.set_waker(Arc::clone(&waker));

        let mut shard = Shard {
            name: format!("{}-shard-handle", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(Handle { waker, thrd, msg_tx }),
        };
        shard.prefix = shard.prefix();
        {
            let (config, miot_id) = (self.config.clone(), self.shard_id);
            let miot = {
                let miot = Miot::from_config(config, miot_id)?;
                miot.spawn(shard.to_tx(), app_tx.clone())?
            };
            match &shard.inner {
                Inner::Handle(Handle { thrd, .. }) => {
                    thrd.request(Request::SetMiot(miot, msg_rx))??;
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
            prefix: String::default(),
            config: self.config.clone(),
            inner,
        };
        shard.prefix = shard.prefix();
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

        let mut shard = Shard {
            name: format!("{}-shard-msg-tx", self.config.name),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        };
        shard.prefix = shard.prefix();
        shard
    }
}

pub enum Request {
    SetMiot(Miot, MsgRx),
    SetShardQueues(BTreeMap<u32, Shard>),
    SetMsgRx(MsgRx),
    AddSession(AddSessionArgs),
    FlushConnection { socket: Socket, err: Error },
    Close,
}

pub enum Response {
    Ok,
}

pub struct AddSessionArgs {
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub pkt: v5::Connect,
}

// calls to interface with shard-thread.
impl Shard {
    pub fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { waker, .. }) => {
                err!(IOError, try: waker.wake(), "shard-wake")
            }
            Inner::Tx(waker, _) => err!(IOError, try: waker.wake(), "shard-wake"),
            _ => unreachable!(),
        }
    }

    pub fn set_shard_queues(&self, shards: BTreeMap<u32, Shard>) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { thrd, .. }) => {
                thrd.request(Request::SetShardQueues(shards))??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn add_session(&self, args: AddSessionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { thrd, .. }) => {
                thrd.request(Request::AddSession(args))??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn flush_connection(&self, socket: Socket, err: Error) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => tx.post(Request::FlushConnection { socket, err })?,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn close_wait(mut self) -> Shard {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(Handle { thrd, .. }) => {
                thrd.request(Request::Close).ok();
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

        let req = allow_panic!(self, rx.recv());
        let msg_rx = match req {
            (Request::SetMiot(miot, msg_rx), Some(tx)) => {
                let run_loop = match &mut self.inner {
                    Inner::Main(run_loop) => run_loop,
                    _ => unreachable!(),
                };
                run_loop.miot = miot;
                allow_panic!(&self, tx.send(Ok(Response::Ok)));
                msg_rx
            }
            _ => unreachable!(),
        };

        let mut events = mio::Events::with_capacity(crate::POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(&self, self.as_mut_poll().poll(&mut events, timeout));

            match self.mio_events(&rx, &events) {
                true /*disconnected*/ => break,
                false => (),
            };

            self.in_messages(&msg_rx);
            self.flush_messages();
            self.route_packets();

            // wake up miot every time shard wakes up
            allow_panic!(self, self.as_miot().wake())
        }

        self.handle_close(Request::Close);

        info!("{} thread exit ...", self.prefix);

        self
    }
}

impl Shard {
    // (disconnected,)
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
                (q @ SetShardQueues(_), Some(tx)) => {
                    allow_panic!(&self, tx.send(Ok(self.handle_set_shard_queues(q))));
                }
                (q @ AddSession { .. }, Some(tx)) => {
                    allow_panic!(&self, tx.send(Ok(self.handle_add_session(q))));
                }
                (q @ FlushConnection { .. }, None) => {
                    let _resp = self.handle_flush_connection(q);
                }
                (q @ Close, Some(tx)) => {
                    allow_panic!(&self, tx.send(Ok(self.handle_close(q))));
                }

                (_, _) => unreachable!(),
            };
        }

        (empty, disconnected)
    }

    fn in_messages(&mut self, msg_rx: &MsgRx) -> QueueStatus<Message> {
        let RunLoop { sessions, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        // receive messages targeting all the sessions.
        let msgs = match msg_rx.try_recvs() {
            QueueStatus::Ok(msgs) => msgs,
            QueueStatus::Block(msgs) => msgs,
            QueueStatus::Disconnected(_) => {
                return QueueStatus::Disconnected(vec![]);
            }
        };

        // partition the messages for each session.
        let mut session_msgs: BTreeMap<ClientID, Vec<Message>> = BTreeMap::default();
        for msg in msgs.into_iter() {
            let client_id = msg.as_client_id();
            match session_msgs.get_mut(client_id) {
                Some(msgs) => msgs.push(msg),
                None => {
                    session_msgs.insert(client_id.clone(), vec![msg]);
                }
            }
        }

        // book the partitioned messages under each session
        for (client_id, msgs) in session_msgs.into_iter() {
            match sessions.get_mut(&client_id) {
                Some(session) => session.in_messages(msgs),
                None => warn!("{} msg-rx, session {} is gone", self.prefix, *client_id),
            }
        }

        QueueStatus::Ok(vec![])
    }

    // flush the booked messages down stream
    fn flush_messages(&mut self) {
        let RunLoop { sessions, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        for (_, session) in sessions.iter_mut() {
            session.flush_messages();
        }
    }

    // For each session interface convert packets to messages and route them to other
    // shards/bridges and consume messages from other shards.
    fn route_packets(&mut self) {
        let mut inner = mem::replace(&mut self.inner, Inner::Init);
        let RunLoop { sessions, .. } = match &mut inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let mut failed_sessions = vec![];
        for (client_id, session) in sessions.iter_mut() {
            match session.route_packets(self) {
                Ok(status) => todo!(),
                Err(err) => failed_sessions.push((client_id.clone(), err)),
            }
        }
        let _init = mem::replace(&mut self.inner, inner);

        for (client_id, err) in failed_sessions {
            let RunLoop { miot, .. } = match &mut self.inner {
                Inner::Main(run_loop) => run_loop,
                _ => unreachable!(),
            };
            match allow_panic!(&self, miot.remove_connection(&client_id)) {
                Some(socket) => {
                    let req = Request::FlushConnection { socket, err };
                    self.handle_flush_connection(req);
                }
                None => (),
            }
        }
    }

    // For each session interface with miot pkt_channels to read/write packets.
    fn route_messages(&mut self) {
        todo!()
        //let mut inner = mem::replace(&mut self.inner, Inner::Init);
        //let RunLoop { sessions, .. } = match &mut inner {
        //    Inner::Main(run_loop) => run_loop,
        //    _ => unreachable!(),
        //};

        //let mut failed_sessions = vec![];
        //for (client_id, session) in sessions.iter_mut() {
        //    match session.route_packets(self) {
        //        Ok(()) => (),
        //        Err(err) if err.kind() == ErrorKind::ProtocolError => {
        //            failed_sessions.push((client_id.clone(), err));
        //        }
        //        Err(err) if err.kind() == ErrorKind::Disconnected => {
        //            failed_sessions.push((client_id.clone(), err));
        //        }
        //        Err(err) => unreachable!("unexpected err {}", err),
        //    }
        //}
        //let _init = mem::replace(&mut self.inner, inner);

        //for (client_id, err) in failed_sessions {
        //    let socket = {
        //        let RunLoop { miot, .. } = match &mut self.inner {
        //            Inner::Main(run_loop) => run_loop,
        //            _ => unreachable!(),
        //        };
        //        allow_panic!(&self, miot.remove_connection(&client_id))
        //    };
        //    self.handle_flush_connection(Request::FlushConnection { socket, err });
        //}
    }
}

impl Shard {
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
        use crate::{miot::AddConnectionArgs, session::SessionArgs};

        let AddSessionArgs { conn, addr, pkt } = match req {
            Request::AddSession(args) => args,
            _ => unreachable!(),
        };

        let client_id = ClientID::from_connect(&pkt.payload.client_id);

        // TODO: handle pkt.flags.clean_start here.

        let (mut session, upstream, downstream) = {
            // This queue is wired up with miot-thread. This queue carries v5::Packet,
            // and there is a separate queue for every session.
            let (upstream, session_rx) = {
                let size = self.config.mqtt_msg_batch_size() as usize;
                socket::pkt_channel(self.shard_id, size, self.as_waker())
            };
            // This queue is wired up with miot-thread. This queue carries v5::Packet,
            // and there is a separate queue for every session.
            let (miot_tx, downstream) = {
                let size = self.config.mqtt_msg_batch_size() as usize;
                socket::pkt_channel(self.shard_id, size, self.as_miot().to_waker())
            };
            let args = SessionArgs {
                addr,
                client_id: client_id.clone(),
                shard_id: self.shard_id,
                miot_tx,
                session_rx,
            };
            (Session::start(args, self.config.clone(), &pkt), upstream, downstream)
        };

        // send back the connection acknowledgment CONNACK here.
        let packet = v5::Packet::ConnAck(session.success_ack(&pkt, self));
        session.in_messages(vec![Message::new_client_ack(packet)]);
        match session.flush_messages() {
            QueueStatus::Disconnected(_) | QueueStatus::Block(_) => {
                error!("{} fail to send CONNACK in add_session", self.prefix);
                Response::Ok
            }
            _ => {
                // add_connection further down shall wake miot-thread.
                let RunLoop { sessions, miot, .. } = match &mut self.inner {
                    Inner::Main(run_loop) => run_loop,
                    _ => unreachable!(),
                };
                // nuke existing session.
                let old_session = match sessions.get(&client_id) {
                    Some(_) => sessions.remove(&client_id),
                    None => None,
                };

                {
                    let client_id = client_id.clone();
                    let args = AddConnectionArgs {
                        client_id,
                        conn,
                        addr,
                        upstream,
                        downstream,
                        client_max_packet_size: session.client_max_packet_size(),
                    };
                    allow_panic!(&self, miot.add_connection(args));
                }

                sessions.insert(client_id.clone(), session);

                let socket = allow_panic!(self, miot.remove_connection(&client_id));

                match (old_session, socket) {
                    (Some(_old_session), Some(socket)) => {
                        let err: Result<()> = err!(
                            SessionTakenOver,
                            code: SessionTakenOver,
                            "client {}",
                            addr
                        );
                        let err = err.unwrap_err();
                        let arg = Request::FlushConnection { socket, err };
                        mem::drop(sessions);
                        mem::drop(miot);
                        self.handle_flush_connection(arg);
                    }
                    (_, _) => (), // there no exiting session with same client_id
                }

                Response::Ok
            }
        }
    }

    fn handle_flush_connection(&mut self, req: Request) -> Response {
        use crate::flush::FlushConnectionArgs;

        let (socket, err) = match req {
            Request::FlushConnection { socket, err } => (socket, err),
            _ => unreachable!(),
        };
        let RunLoop { flusher, sessions, topic_filters, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        match sessions.remove(&socket.client_id) {
            Some(mut session) => {
                session.remove_topic_filters(topic_filters);
                session.close();
            }
            None => (),
        }

        let args = FlushConnectionArgs { socket, err: Some(err) };
        allow_panic!(&self, flusher.flush_connection(args));

        Response::Ok
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(mut run_loop) => {
                info!("{} close sessions:{} ...", self.prefix, run_loop.sessions.len());

                let miot = mem::replace(&mut run_loop.miot, Miot::default()).close_wait();

                mem::drop(run_loop.poll);
                mem::drop(run_loop.cluster);
                mem::drop(run_loop.flusher);

                mem::drop(run_loop.shard_queues);
                mem::drop(run_loop.topic_filters);
                mem::drop(run_loop.retained_messages);

                let mut new_sessions = BTreeMap::default();
                for (client_id, sess) in run_loop.sessions.into_iter() {
                    new_sessions.insert(client_id, sess.close());
                }

                let fin_state = FinState { miot, sessions: new_sessions };
                let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));

                Response::Ok
            }
            Inner::Close(_) => Response::Ok,
            _ => unreachable!(),
        }
    }
}

impl Shard {
    pub fn prefix(&self) -> String {
        format!("{}:{}", self.name, self.shard_id)
    }

    pub fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            _ => unreachable!(),
        }
    }

    pub fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }

    pub fn as_miot(&self) -> &Miot {
        match &self.inner {
            Inner::Main(RunLoop { miot, .. }) => miot,
            _ => unreachable!(),
        }
    }

    pub fn as_waker(&self) -> Arc<mio::Waker> {
        match &self.inner {
            Inner::Main(RunLoop { waker, .. }) => Arc::clone(waker),
            _ => unreachable!(),
        }
    }
}
