use log::{debug, error, info};
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
    /// Input channel size for this thread.
    pub chan_size: usize,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Arc<mio::Waker>, Thread<Listener, Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Mio poller for asynchronous handling.
    poll: mio::Poll,
    /// MQTT server listening on `port`.
    server: Option<mio::net::TcpListener>,
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,
    /// whether thread is closed.
    closed: bool,
}

impl Default for Listener {
    fn default() -> Listener {
        use crate::REQ_CHANNEL_SIZE;

        let config = Config::default();
        Listener {
            name: format!("{}-listener-init", config.name),
            port: config.port.unwrap(),
            chan_size: REQ_CHANNEL_SIZE,
            config,
            inner: Inner::Init,
        }
    }
}

impl Drop for Listener {
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

impl Listener {
    const TOKEN_WAKE: mio::Token = mio::Token(1);
    const TOKEN_SERVER: mio::Token = mio::Token(2);

    /// Create a listener from configuration. Listener shall be in `Init` state, to start
    /// the listener thread call [Listener::spawn].
    pub fn from_config(config: Config) -> Result<Listener> {
        let l = Listener::default();
        let val = Listener {
            name: format!("{}-listener-init", config.name),
            port: config.port.unwrap_or(l.port),
            chan_size: l.chan_size,
            config,
            inner: Inner::Init,
        };

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

        let poll = mio::Poll::new()?;
        poll.registry().register(&mut server, Self::TOKEN_SERVER, Interest::READABLE)?;
        let waker = Arc::new(Waker::new(poll.registry(), Self::TOKEN_WAKE)?);

        let listener = Listener {
            name: format!("{}-listener-main", self.config.name),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                server: Some(server),
                cluster: Box::new(cluster),
                closed: false,
            }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, listener);

        let listener = Listener {
            name: format!("{}-listener-handle", self.config.name),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };

        Ok(listener)
    }
}

// calls to interface with listener-thread, and shall wake the thread
impl Listener {
    pub fn close_wait(mut self) -> Result<Listener> {
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
    Close,
}

pub enum Response {
    Ok,
}

impl Threadable for Listener {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        info!("{} spawn port:{} chan_size:{} ...", self.pp(), self.port, self.chan_size);

        let mut events = Events::with_capacity(2);
        let res = loop {
            let timeout: Option<time::Duration> = None;
            match self.as_mut_poll().poll(&mut events, timeout) {
                Ok(()) => (),
                Err(err) => {
                    break err!(IOError, try: Err(err), "{} poll error", self.pp())
                }
            };

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
                allow_panic!(self.as_cluster().restart_listener());
            }
        }

        self
    }
}

impl Listener {
    // return whether we are doing normal exit, which is rx-disconnected
    fn mio_events(&mut self, rx: &ThreadRx, es: &Events) -> Result<bool> {
        let mut count = 0_usize;
        let mut iter = es.iter();
        let res = 'outer: loop {
            match iter.next() {
                Some(e) => {
                    count += 1;
                    match e.token() {
                        Self::TOKEN_WAKE => loop {
                            match self.mio_chan(rx)? {
                                (_empty, true) => break 'outer Ok(true),
                                (true, _disconnected) => break,
                                (false, false) => (),
                            }
                        },
                        Self::TOKEN_SERVER => while !self.mio_conn() {},
                        _ => unreachable!(),
                    }
                }
                None => break Ok(false),
            }
        };
        debug!("{} polled and got {} events", self.pp(), count);

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
                (q @ Close, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_close(q)))?
                }
                (_, _) => unreachable!(),
            }
        }

        Ok((empty, disconnected))
    }

    // Return would_block
    fn mio_conn(&mut self) -> bool {
        use std::io;

        let (server, cluster) = match &self.inner {
            Inner::Main(RunLoop { server, cluster, .. }) => (server, cluster),
            _ => unreachable!(),
        };

        match server.as_ref().unwrap().accept() {
            Ok((conn, addr)) => {
                let hs = Handshake {
                    prefix: format!("{}-handshake:{}", self.pp(), addr),
                    conn: Some(conn),
                    addr,
                    cluster: cluster.to_tx(),
                };
                let _thrd = Thread::spawn_sync("handshake", 1, hs);
                false
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => true,
            Err(err) => {
                error!("{} accept-failed {}", self.pp(), err);
                false
            }
        }
    }

    fn handle_close(&mut self, _req: Request) -> Result<Response> {
        use std::mem;

        info!("{} close ...", self.pp());

        let RunLoop { poll, server, cluster, closed } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };
        *closed = true;

        let cluster = mem::replace(cluster, Box::new(Cluster::default()));
        mem::drop(cluster);
        mem::drop(server.take());
        mem::drop(mem::replace(poll, mio::Poll::new()?));

        Ok(Response::Ok)
    }
}

impl Listener {
    fn server_address(&self) -> String {
        format!("0.0.0.0:{}", self.port)
    }

    fn pp(&self) -> String {
        format!("{}:{}", self.name, self.server_address())
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            _ => unreachable!(),
        }
    }

    fn as_cluster(&self) -> &Cluster {
        match &self.inner {
            Inner::Main(RunLoop { cluster, .. }) => cluster,
            _ => unreachable!(),
        }
    }
}

struct Handshake {
    prefix: String,
    conn: Option<mio::net::TcpStream>,
    addr: net::SocketAddr,
    cluster: Cluster,
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        use crate::{packet::PacketRead, v5, MAX_CONNECT_TIMEOUT, MAX_SOCKET_RETRY};
        use std::thread;

        info!("new connection {}", self.addr);

        let mut packetr = PacketRead::new();
        let (conn, addr) = (self.conn.take().unwrap(), self.addr);
        let dur = MAX_CONNECT_TIMEOUT / u64::try_from(MAX_SOCKET_RETRY).unwrap();
        let (mut retries, prefix) = (0, self.prefix.clone());

        let pkt_connect = loop {
            packetr = match packetr.read(&conn) {
                Ok((pr, true, _)) if retries < MAX_SOCKET_RETRY => {
                    retries += 1;
                    pr
                }
                Ok((_pr, true, _)) => {
                    break err!(
                        InsufficientBytes,
                        desc: "{} fail after {} retries",
                        prefix,
                        retries
                    )
                }
                Ok((pr, false, _)) => match pr.parse() {
                    Ok(v5::Packet::Connect(pkt_connect)) => break Ok(pkt_connect),
                    Ok(pkt) => {
                        break err!(
                            IOError,
                            desc: "{} unexpect {:?} on new connection",
                            prefix,
                            pkt.to_packet_type()
                        );
                    }
                    Err(err) => {
                        break err!(IOError, desc: "{} parse failed {}", prefix, err);
                    }
                },
                Err(err) => break err!(IOError, desc: "{} read failed {}", prefix, err),
            };
            thread::sleep(time::Duration::from_millis(dur));
        };

        match pkt_connect {
            Ok(pkt_connect) => {
                err!(IPCFail, try: self.cluster.add_connection(conn, addr, pkt_connect))
                    .ok();
            }
            Err(_) => (),
        }

        self
    }
}
