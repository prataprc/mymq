use log::{debug, error, info};

use std::{net, sync::Arc, time};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{v5, Cluster, Config};
use crate::{Error, ErrorKind, Result};

const BATCH_SIZE: usize = 1024;

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
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Mio poller for asynchronous handling.
    poll: mio::Poll,
    /// MQTT server listening on `port`.
    server: Option<mio::net::TcpListener>,
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,
}

impl Default for Listener {
    fn default() -> Listener {
        use crate::CHANNEL_SIZE;

        let config = Config::default();
        Listener {
            name: "<listener-thrd>".to_string(),
            port: config.port.unwrap(),
            chan_size: CHANNEL_SIZE,
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
            Inner::Tx(_waker, _tx) => info!("{} drop ...", self.pp()),
            Inner::Main(_run_loop) => info!("{} drop ...", self.pp()),
        }
    }
}

impl Listener {
    const SERVER_TOKEN: mio::Token = mio::Token(1);
    const WAKE_TOKEN: mio::Token = mio::Token(2);

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
        poll.registry().register(&mut server, Self::SERVER_TOKEN, Interest::READABLE)?;
        let waker = Arc::new(Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let listener = Listener {
            name: format!("{}-listener-main", self.config.name),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                poll,
                server: Some(server),
                cluster: Box::new(cluster),
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

    pub fn to_tx(&self) -> Self {
        info!("{} cloning tx ...", self.pp());

        let inner = match &self.inner {
            Inner::Handle(waker, thrd) => Inner::Tx(Arc::clone(&waker), thrd.to_tx()),
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(&waker), tx.clone()),
            _ => unreachable!(),
        };

        Listener {
            name: format!("{}-listener-tx", self.config.name),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
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

    fn main_loop(mut self, rx: Rx<Request, Result<Response>>) -> Result<Self> {
        info!("{} spawn port:{} chan_size:{} ...", self.pp(), self.port, self.chan_size);

        let mut events = mio::Events::with_capacity(2);
        loop {
            let timeout: Option<time::Duration> = None;
            match self.as_mut_poll().poll(&mut events, timeout) {
                Ok(()) => (),
                Err(err) => {
                    error!("{} poll error `{}`", self.pp(), err);
                    self.as_cluster().failed_listener().unwrap();
                    err!(IOError, try: Err(err))?
                }
            };
            let n = events.iter().collect::<Vec<&mio::event::Event>>().len();
            debug!("{} polled and got {} events", self.pp(), n);

            let exit = match self.mio_events(&rx) {
                Ok(exit) => exit,
                Err(err) => {
                    error!("{} exiting with failure `{}`", self.pp(), err);
                    self.as_cluster().failed_listener().unwrap();
                    break Err(err);
                }
            };

            if exit {
                break Ok(self);
            }
        }
    }
}

impl Listener {
    // return whether we are doing normal exit.
    fn mio_events(&mut self, rx: &Rx<Request, Result<Response>>) -> Result<bool> {
        loop {
            match self.mio_chan(rx)? {
                (_empty, true) => break Ok(true),
                (false, false) => continue,
                (true, false) => (),
            }
            match self.mio_conn()? {
                empty if !empty => continue,
                _ => break Ok(false),
            }
        }
    }

    // Return empty
    fn mio_conn(&mut self) -> Result<bool> {
        use std::io;

        let (server, cluster) = match &self.inner {
            Inner::Main(RunLoop { server, cluster, .. }) => (server, cluster),
            _ => unreachable!(),
        };

        let (conn, addr) = match server.as_ref().unwrap().accept() {
            Ok((conn, addr)) => (conn, addr),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(true),
            Err(err) => {
                error!("{} accept failed {}", self.pp(), err);
                err!(IOError, try: Err(err))?
            }
        };

        let hs = Handshake {
            conn: Some(conn),
            addr,
            cluster: cluster.to_tx(),
            retries: 0,
        };
        let _thrd = Thread::spawn_sync("handshake", 1, hs);

        Ok(false)
    }

    // Return (empty, disconnected)
    fn mio_chan(&mut self, rx: &Rx<Request, Result<Response>>) -> Result<(bool, bool)> {
        use crate::thread::pending_requests;
        use Request::*;

        let (qs, empty, disconnected) = pending_requests(&rx, BATCH_SIZE);
        debug!("{} processing {} requests ...", self.pp(), qs.len());
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

    fn handle_close(&mut self, _req: Request) -> Result<Response> {
        use std::mem;

        info!("{} close ...", self.pp());

        let RunLoop { server, cluster, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let cluster = mem::replace(cluster, Box::new(Cluster::default()));
        mem::drop(cluster);
        mem::drop(server.take());

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
    conn: Option<mio::net::TcpStream>,
    addr: net::SocketAddr,
    cluster: Cluster,
    retries: usize,
}

impl Threadable for Handshake {
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Result<Self> {
        use crate::{v5::Packet, MAX_SOCKET_RETRY};

        info!("new connection {}", self.addr);

        let mut packetr = v5::PacketRead::new();
        let (conn, addr) = (self.conn.take().unwrap(), self.addr);

        let pkt_connect = loop {
            packetr = match packetr.read(&conn) {
                Ok((pr, true)) if self.retries < MAX_SOCKET_RETRY => {
                    self.retries += 1;
                    pr
                }
                Ok((_pr, true)) => {
                    err!(IOError, desc: "handshake fail after {} retries", self.retries)?
                }
                Ok((pr, false)) => match pr.parse() {
                    Ok(Packet::Connect(pkt_connect)) => break pkt_connect,
                    Ok(pkt) => {
                        // SPEC: After a Network Connection is established by a Client to
                        // a Server, the first packet sent from the Client to the
                        // Server MUST be a CONNECT packet [MQTT-3.1.0-1].
                        err!(
                            IOError,
                            desc: "unexpect {:?} on new connection",
                            pkt.to_packet_type()
                        )?
                    }
                    Err(err) => {
                        err!(IOError, desc: "{} handshake parse failed {}", addr, err)?
                    }
                },
                Err(err) => err!(IOError, desc: "{} handshake failed {}", addr, err)?,
            };
        };

        err!(IPCFail, try: self.cluster.connect(conn, addr, pkt_connect))?;

        Ok(self)
    }
}
