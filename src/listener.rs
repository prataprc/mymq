use log::{debug, error, info, trace};
use mio::event::Events;

use std::{net, sync::Arc, time};

use crate::thread::{Rx, Thread, Threadable};
use crate::{Cluster, Config};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Listener {
    /// Human readable name for this mio thread.
    pub name: String,
    /// Port to listen to.
    pub port: u16,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    // Held by Cluster
    Handle(Arc<mio::Waker>, Thread<Listener, Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Mio poller for asynchronous handling, aggregate events from server and
    /// thread-waker.
    poll: mio::Poll,
    /// MQTT server listening on `port`.
    server: Option<mio::net::TcpListener>,
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,
    /// thread is already closed.
    closed: bool,
}

impl Default for Listener {
    fn default() -> Listener {
        let config = Config::default();
        let mut def = Listener {
            name: format!("{}-listener-init", config.name),
            port: config.port.unwrap(),
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
            Inner::Handle(_waker, _thrd) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Listener {
    /// Poll register token for waker event, OTP calls made to this thread shall trigger
    /// this event.
    pub const TOKEN_WAKE: mio::Token = mio::Token(1);
    /// Poll register for server TcpStream.
    pub const TOKEN_SERVER: mio::Token = mio::Token(2);

    /// Create a listener from configuration. Listener shall be in `Init` state. To start
    /// this listener thread call [Listener::spawn].
    pub fn from_config(config: Config) -> Result<Listener> {
        let def = Listener::default();
        let mut val = Listener {
            name: def.name.clone(),
            port: config.port.unwrap_or(def.port),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster) -> Result<Listener> {
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
            port: self.port,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                server: Some(server),
                cluster: Box::new(cluster),
                closed: false,
            }),
        };
        listener.prefix = listener.prefix();
        let thrd = Thread::spawn(&self.prefix, listener);

        let mut listener = Listener {
            name: format!("{}-listener-handle", self.config.name),
            port: self.port,
            prefix: self.prefix.clone(),
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
    pub fn close_wait(mut self) -> Result<Listener> {
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

impl Threadable for Listener {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        info!("{}, spawn thread port:{} ...", self.prefix, self.port);

        let mut events = Events::with_capacity(crate::POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            allow_panic!(self.prefix, self.as_mut_poll().poll(&mut events, timeout));

            match self.mio_events(&rx, &events) {
                // Exit or not.
                Ok(true) => break,
                Ok(false) => (),
                Err(err) if err.kind() == ErrorKind::IPCFail => {
                    panic!("{} err: {}", self.prefix, err)
                }
                Err(err) => unreachable!("unexpected error {}", err),
            };
        }

        info!("{}, thread exit ...", self.prefix);

        self
    }
}

impl Listener {
    // return (exit,)
    // IPCFail on local channel communication.
    fn mio_events(&mut self, rx: &ThreadRx, events: &Events) -> Result<bool> {
        let mut count = 0_usize;
        let mut iter = events.iter();
        let res = 'outer: loop {
            match iter.next() {
                Some(event) => {
                    trace!("{}r poll-event token:{}", self.prefix, event.token().0);
                    count += 1;

                    match event.token() {
                        Self::TOKEN_WAKE => loop {
                            // keep repeating until all control requests are drained
                            match self.drain_control_chan(rx)? {
                                (_empty, true) => break 'outer Ok(true),
                                (true, _disconnected) => break,
                                (false, false) => (),
                            }
                        },
                        Self::TOKEN_SERVER => loop {
                            match self.accept_conn() {
                                true => break,
                                false => (),
                            };
                        },
                        _ => unreachable!(),
                    }
                }
                None => break Ok(false),
            }
        };

        debug!("{}, polled and got {} events", self.prefix, count);
        res
    }

    // Return (empty, disconnected)
    // IPCFail on local channel communication.
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> Result<(bool, bool)> {
        use crate::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let closed = match &self.inner {
            Inner::Main(RunLoop { closed, .. }) => *closed,
            _ => unreachable!(),
        };

        let (mut qs, empty, mut disconnected) = pending_requests(rx, CONTROL_CHAN_SIZE);

        if closed {
            info!("{} skipping {} requests closed:{}", self.prefix, qs.len(), closed);
            qs.drain(..);
        } else {
            debug!("{} process {} requests closed:{}", self.prefix, qs.len(), closed);
        }

        for q in qs.into_iter() {
            match q {
                (q @ Close, Some(tx)) => {
                    disconnected = true;
                    err!(IPCFail, try: tx.send(Ok(self.handle_close(q))))?
                }
                (_, _) => unreachable!(),
            }
        }

        Ok((empty, disconnected))
    }

    // Return (would_block,)
    fn accept_conn(&mut self) -> bool {
        use crate::Handshake;
        use std::io;

        let (server, cluster) = match &self.inner {
            Inner::Main(RunLoop { server, cluster, .. }) => (server, cluster),
            _ => unreachable!(),
        };

        match server.as_ref().unwrap().accept() {
            Ok((conn, addr)) => {
                // for every successful accept launch a handshake thread.
                let hs = Handshake {
                    prefix: format!("{}:handshake:{}", self.prefix, addr),
                    conn: Some(conn),
                    addr,
                    config: self.config.clone(),
                    cluster: cluster.to_tx(),
                };
                let _thrd = Thread::spawn_sync("handshake", 1, hs);
                false
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => true,
            Err(err) => {
                error!("{}, connection accept error, {}", self.prefix, err);
                false
            }
        }
    }
}

impl Listener {
    fn handle_close(&mut self, _req: Request) -> Response {
        use std::mem;

        let RunLoop { server, cluster, closed, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        if *closed == false {
            let cluster = mem::replace(cluster, Box::new(Cluster::default()));
            mem::drop(cluster);
            mem::drop(server.take());

            info!("{} closed ...", self.prefix);
            *closed = true;
        }
        Response::Ok
    }
}

impl Listener {
    fn server_address(&self) -> String {
        format!("0.0.0.0:{}", self.port)
    }

    fn prefix(&self) -> String {
        format!("{}:listener:{}", self.name, self.server_address())
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            _ => unreachable!(),
        }
    }
}
