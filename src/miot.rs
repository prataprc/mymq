use log::{debug, error, info};

use std::{collections::BTreeMap, net};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{ClientID, Config, Shard};
use crate::{Error, ErrorKind, Result};

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
    Handle(Thread<Miot, Request, Result<Response>>),
    Tx(Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    shard: Box<Shard>,
    rsocks: BTreeMap<ClientID, net::TcpStream>,
    wsocks: BTreeMap<ClientID, net::TcpStream>,
}

impl Default for Miot {
    fn default() -> Miot {
        let config = Config::default();
        Miot {
            name: format!("{}-miot", config.name),
            chan_size: config.miot_chan_size.unwrap(),
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
            Inner::Init => debug!("Miot::Init, {:?} drop ...", self.name),
            Inner::Handle(_) => {
                error!("Miot::Handle, {:?} invalid drop ...", self.name);
                panic!("Miot::Handle, {:?} invalid drop ...", self.name);
            }
            Inner::Tx(_tx) => info!("Miot::Tx {:?} drop ...", self.name),
            Inner::Main(_run_loop) => info!("Miot::Main {:?} drop ...", self.name),
        }
    }
}

impl Miot {
    /// Create a miot thread from configuration. Miot shall be in `Init` state, to start
    /// the miot thread call [Miot::spawn].
    pub fn from_config(config: Config) -> Result<Miot> {
        let m = Miot::default();
        let val = Miot {
            name: format!("{}-miot", config.name),
            chan_size: config.miot_chan_size.unwrap_or(m.chan_size),
            config: config.clone(),
            inner: Inner::Init,
        };

        Ok(val)
    }

    pub fn spawn(self, shard: Shard) -> Result<Miot> {
        info!("Starting miot {:?} chan_size:{} ...", self.name, self.chan_size);

        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "miot can be spawned only in init-state ")?;
        }

        let miot = Miot {
            name: self.name.clone(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                shard: Box::new(shard),
                rsocks: BTreeMap::default(),
                wsocks: BTreeMap::default(),
            }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, miot);

        let val = Miot {
            name: self.name.clone(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };

        Ok(val)
    }

    pub fn to_tx(&self) -> Self {
        info!("Miot::to_tx {:?} cloning tx ...", self.name);

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        Miot {
            name: self.name.clone(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
    }

    pub fn close_wait(mut self) -> Result<Miot> {
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

impl Threadable for Miot {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Result<Self> {
        use crate::thread::pending_msg;
        use Request::*;

        loop {
            let (qs, disconnected) = pending_msg(&rx, self.chan_size);
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

impl Miot {
    fn handle_close(&mut self) -> Result<Response> {
        use std::mem;

        let RunLoop { shard, rsocks, wsocks } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };
        let shard = mem::replace(shard, Box::new(Shard::default()));
        let rsocks = mem::replace(rsocks, BTreeMap::default());
        let wsocks = mem::replace(wsocks, BTreeMap::default());
        info!(
            "Miot::close, {:?} rconns:{:?} wconns:{:?}",
            self.name,
            rsocks.len(),
            wsocks.len(),
        );

        assert_eq!(rsocks.len(), wsocks.len()); // TODO: feature gate this
        for (id, s) in rsocks.into_iter() {
            info!(
                "Miot::close, {:?} read-connection {:?} for client-id {:?}",
                self.name,
                s.peer_addr()?,
                id
            );
        }
        for (id, s) in wsocks.into_iter() {
            info!(
                "Miot::close, {:?} write-connection {:?} for client-id {:?}",
                self.name,
                s.peer_addr()?,
                id
            );
        }

        mem::drop(shard);

        Ok(Response::Ok)
    }
}
