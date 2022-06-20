use log::{debug, error, info};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{Cluster, Config};
use crate::{Error, ErrorKind, Result};

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
    Handle(Thread<Listener, Request, Result<Response>>),
    Tx(Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// Tx-handle to send messages to cluster.
    cluster: Box<Cluster>,
}

impl Default for Listener {
    fn default() -> Listener {
        let config = Config::default();
        Listener {
            name: "--listener--".to_string(),
            port: config.port.unwrap(),
            chan_size: config.listener_chan_size.unwrap(),
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
            Inner::Init => debug!("Listener::Init, {:?} drop ...", self.name),
            Inner::Handle(_) => {
                error!("Listener::Handle, {:?} invalid drop ...", self.name);
                panic!("Listener::Handle, {:?} invalid drop ...", self.name);
            }
            Inner::Tx(_tx) => info!("Listener::Tx {:?} drop ...", self.name),
            Inner::Main(_run_loop) => info!("Listener::Main {:?} drop ...", self.name),
        }
    }
}

impl Listener {
    /// Create a listener from configuration. Listener shall be in `Init` state, to start
    /// the listener thread call [Listener::spawn].
    pub fn from_config(config: Config) -> Result<Listener> {
        let l = Listener::default();
        let val = Listener {
            name: format!("{}-listener", config.name),
            port: config.port.unwrap_or(l.port),
            chan_size: config.listener_chan_size.unwrap_or(l.chan_size),
            config: config.clone(),
            inner: Inner::Init,
        };

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster) -> Result<Listener> {
        info!(
            "Starting listener {:?} port:{} chan_size:{} ...",
            self.name, self.port, self.chan_size
        );

        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "listener can be spawned only in init-state ")?;
        }

        let listener = Listener {
            name: self.name.clone(),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop { cluster: Box::new(cluster) }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, listener);

        let listener = Listener {
            name: self.name.clone(),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };

        Ok(listener)
    }

    pub fn to_tx(&self) -> Self {
        info!("Listener::to_tx {:?} cloning tx ...", self.name);

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        Listener {
            name: self.name.to_string(),
            port: self.port,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
    }

    pub fn close_wait(mut self) -> Result<Listener> {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::Close)??;
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

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Result<Self> {
        use crate::thread::pending_msg;
        use Request::*;

        loop {
            let (qs, disconnected) = pending_msg(&rx, 16); // TODO: no magic num
            for q in qs.into_iter() {
                match q {
                    (Close, Some(tx)) => {
                        err!(IPCFail, try: tx.send(self.handle_close()))?
                    }
                    (_, _) => unreachable!(),
                }
            }

            if disconnected {
                break;
            }
        }

        Ok(self)
    }
}

impl Listener {
    fn handle_close(&mut self) -> Result<Response> {
        use std::mem;

        info!("Listener::close, {:?}", self.name);

        let RunLoop { cluster } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };
        let cluster = mem::replace(cluster, Box::new(Cluster::default()));

        mem::drop(cluster);

        Ok(Response::Ok)
    }
}
