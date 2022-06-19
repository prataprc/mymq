use log::{debug, error, info};

use std::net;

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::Cluster;
use crate::{Error, ErrorKind, Result};

pub struct Listener {
    /// Address:Port to listen for new incoming connections.
    pub address: net::SocketAddr,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Thread<Listener, Request, Result<Response>>),
    Tx(Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    cluster: Box<Cluster>,
}

impl Default for Listener {
    fn default() -> Listener {
        use crate::default_listen_address4;

        Listener {
            address: default_listen_address4(None),
            inner: Inner::Init,
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("Listener::Init, {:?} drop ...", self.address),
            Inner::Handle(_) => {
                error!("Listener::Handle, {:?} invalid drop ...", self.address);
                panic!("Listener::Handle, {:?} invalid drop ...", self.address);
            }
            Inner::Tx(_tx) => info!("Listener::Tx {:?} drop ...", self.address),
            Inner::Main(_run_loop) => info!("Listener::Main {:?} drop ...", self.address),
        }
    }
}

impl Listener {
    pub fn spawn(self, address: net::SocketAddr, cluster: Cluster) -> Result<Listener> {
        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "listener can be spawned only in init-state ")?;
        }

        let listener = Listener {
            address: self.address,
            inner: Inner::Main(RunLoop { cluster: Box::new(cluster) }),
        };
        let thrd = {
            let name = self.address.to_string();
            Thread::spawn_sync(&name, 16, listener) // TODO: no magic number
        };

        let listener = Listener { address, inner: Inner::Handle(thrd) };

        Ok(listener)
    }

    pub fn to_tx(&self) -> Self {
        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        Listener { address: self.address, inner }
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

        let RunLoop { cluster } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };
        let cluster = mem::replace(cluster, Box::new(Cluster::default()));

        info!("Listener::close, {:?} dropping cluster {:?}", self.address, cluster.name);
        mem::drop(cluster);

        Ok(Response::Ok)
    }
}
