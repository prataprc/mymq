use log::{debug, error, info, trace};
use mio::event::Events;

use std::{net, sync::Arc, time};

use crate::broker::thread::{Rx, Thread, Threadable};
use crate::broker::{AppTx, Cluster, Config, QueueStatus};

use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;

/// Type binds to MQTT port and listens for incoming connection.
///
/// This type is threadable and singleton.
pub struct Listener {
    /// Human readable name for this mio thread.
    name: String,
    prefix: String,
    config: Config,
    inner: Inner,
}

enum Inner {
    Init,
    // Held by Cluster
    Handle(Arc<mio::Waker>, Thread<Listener, Request, Result<Response>>),
    // Thread
    Main(RunLoop),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState),
}

struct RunLoop {
    /// Mio poller for asynchronous handling, aggregate events from server and
    /// thread-waker.
    poll: mio::Poll,
    /// MQTT server listening on `port`.
    server: mio::net::TcpListener,
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,

    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState;

impl Default for Listener {
    fn default() -> Listener {
        let config = Config::default();
        let mut def = Listener {
            name: config.name.clone(),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Listener {
    /// Poll register token for waker event.
    pub const TOKEN_WAKE: mio::Token = mio::Token(1);
    /// Poll register for server TcpStream.
    pub const TOKEN_SERVER: mio::Token = mio::Token(2);

    /// Create a listener from configuration. Listener shall be in `Init` state. To start
    /// this listener thread call [Listener::spawn].
    pub fn from_config(config: &Config) -> Result<Listener> {
        let mut val = Listener {
            name: format!("{}-listener-init", config.name),
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster, app_tx: AppTx) -> Result<Listener> {
        use mio::{Interest, Waker};

        if matches!(&self.inner, Inner::Handle(_, _) | Inner::Main(_)) {
            err!(InvalidInput, desc: "listener can be spawned only in init-state ")?;
        }

        let mut server = {
            let sock_addr: net::SocketAddr = self.server_address().parse().unwrap();
            mio::net::TcpListener::bind(sock_addr)?
        };

        let poll = err!(IOError, try: mio::Poll::new(), "fail creating mio::Poll")?;
        poll.registry().register(&mut server, Self::TOKEN_SERVER, Interest::READABLE)?;
        let waker = Arc::new(Waker::new(poll.registry(), Self::TOKEN_WAKE)?);

        let mut listener = Listener {
            name: format!("{}-listener-main", self.config.name),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                server: server,
                cluster: Box::new(cluster),

                app_tx,
            }),
        };
        listener.prefix = listener.prefix();
        let mut thrd = Thread::spawn(&self.prefix, listener);
        thrd.set_waker(Arc::clone(&waker));

        let mut listener = Listener {
            name: format!("{}-listener-handle", self.config.name),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };
        listener.prefix = listener.prefix();

        Ok(listener)
    }
}

pub enum Request {
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with listener-thread, and shall wake the thread
impl Listener {
    pub fn close_wait(mut self) -> Listener {
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

impl Threadable for Listener {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{}, spawn thread port:{} ...", self.prefix, self.config.port);

        let mut events = Events::with_capacity(POLL_EVENTS_SIZE);
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

        info!("{}, thread exit ...", self.prefix);
        self
    }
}

impl Listener {
    // return (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &Events) -> bool {
        let mut count = 0_usize;
        let mut iter = events.iter();
        let exit = 'outer: loop {
            match iter.next() {
                Some(event) => {
                    trace!("{}r poll-event token:{}", self.prefix, event.token().0);
                    count += 1;

                    match event.token() {
                        Self::TOKEN_WAKE => loop {
                            // keep repeating until all control requests are drained
                            match self.drain_control_chan(rx) {
                                (_status, true) => break 'outer true,
                                (QueueStatus::Ok(_), _exit) => (),
                                (QueueStatus::Block(_), _) => break,
                                (QueueStatus::Disconnected(_), _) => break 'outer true,
                            }
                        },
                        Self::TOKEN_SERVER => loop {
                            match self.accept_conn() {
                                QueueStatus::Ok(_) => (),
                                QueueStatus::Block(_) => break,
                                QueueStatus::Disconnected(_) => break 'outer true,
                            };
                        },
                        _ => unreachable!(),
                    }
                }
                None => break false,
            }
        };

        debug!("{}, polled and got {} events", self.prefix, count);
        exit
    }

    // return (queue-status, exit)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (QueueReq, bool) {
        use crate::broker::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let mut status = pending_requests(&self.prefix, rx, CONTROL_CHAN_SIZE);
        let reqs = status.take_values();
        debug!("{} process {} requests closed:false", self.prefix, reqs.len());

        let mut closed = false;
        for req in reqs.into_iter() {
            match req {
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                    closed = true;
                }
                (_, _) => unreachable!(),
            }
        }

        (status, closed)
    }

    fn accept_conn(&mut self) -> QueueStatus<()> {
        use crate::broker::Handshake;
        use std::io;

        let (server, cluster) = match &self.inner {
            Inner::Main(RunLoop { server, cluster, .. }) => (server, cluster),
            _ => unreachable!(),
        };

        match server.accept() {
            Ok((conn, addr)) => {
                assert_eq!(conn.peer_addr().unwrap(), addr);
                // for every successful accept launch a handshake thread.
                let hs = Handshake {
                    prefix: format!("{}:handshake:{}", self.prefix, addr),
                    conn: Some(conn),
                    config: self.config.clone(),
                    cluster: cluster.to_tx(),
                };
                let _thrd = Thread::spawn_sync("handshake", 1, hs);
                QueueStatus::Ok(Vec::new())
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                QueueStatus::Block(Vec::new())
            }
            Err(err) => {
                error!("{}, connection accept error, {}", self.prefix, err);
                QueueStatus::Disconnected(Vec::new())
            }
        }
    }
}

impl Listener {
    fn handle_close(&mut self, _req: Request) -> Response {
        use std::mem;

        match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(_run_loop) => {
                info!("{} closing ...", self.prefix);

                // Drop, poll, server, cluster and app_tx.
                let _init = mem::replace(&mut self.inner, Inner::Close(FinState));
                Response::Ok
            }
            Inner::Close(_) => Response::Ok,
            _ => unreachable!(),
        }
    }
}

impl Listener {
    fn server_address(&self) -> String {
        format!("0.0.0.0:{}", self.config.port)
    }

    fn prefix(&self) -> String {
        format!("{}:{}", self.name, self.server_address())
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
