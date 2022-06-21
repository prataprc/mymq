use log::{debug, error, info};
use uuid::Uuid;

use std::collections::BTreeMap;

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{ClientID, Cluster, Config, Miot, Session, Shardable};
use crate::{Error, ErrorKind, Result};

pub struct Shard {
    /// Human readable name for shard.
    pub name: String,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,
    /// Input channel size for the shard's thread.
    pub chan_size: usize,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Thread<Shard, Request, Result<Response>>),
    Tx(Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    cluster: Box<Cluster>,
    sessions: BTreeMap<ClientID, Session>,
    miot: Miot,
}

impl Default for Shard {
    fn default() -> Shard {
        use crate::CHANNEL_SIZE;

        let config = Config::default();
        Shard {
            name: format!("{}-shard", config.name),
            uuid: Uuid::new_v4(),
            chan_size: CHANNEL_SIZE,
            config,
            inner: Inner::Init,
        }
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
            Inner::Init => debug!("Shard::Init, {:?}, drop ...", self.name),
            Inner::Handle(_) => {
                error!("Shard::Handle, {:?} invalid drop ...", self.name);
                panic!("Shard::Handle, {:?} nvalid drop ...", self.name);
            }
            Inner::Tx(_tx) => info!("Shard::Tx {:?} drop ...", self.name),
            Inner::Main(_run_loop) => info!("Shard::Main {:?} drop ...", self.name),
        }
    }
}

impl Shard {
    pub fn from_config(config: Config) -> Result<Shard> {
        let s = Shard::default();
        let val = Shard {
            name: format!("{}-shard", config.name),
            uuid: s.uuid,
            chan_size: s.chan_size,
            config: config.clone(),
            inner: Inner::Init,
        };

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster) -> Result<Shard> {
        info!(
            "Starting shard {:?} uuid:{} chan_size:{} ...",
            self.name, self.uuid, self.chan_size
        );

        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "shard can be spawned only in init-state ")?;
        }

        let shard = Shard {
            name: self.name.clone(),
            uuid: self.uuid,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                cluster: Box::new(cluster),
                sessions: BTreeMap::default(),
                miot: Miot::default(),
            }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, shard);

        let shard = Shard {
            name: self.name.clone(),
            uuid: self.uuid,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        {
            let miot = Miot::from_config(self.config.clone())?.spawn(shard.to_tx())?;
            match &shard.inner {
                Inner::Handle(thrd) => {
                    thrd.request(Request::SetMiot(miot))??;
                }
                _ => unreachable!(),
            }
        }

        Ok(shard)
    }

    pub fn to_tx(&self) -> Self {
        info!("Shard::to_tx {:?} cloning tx ...", self.name);

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        Shard {
            name: self.name.clone(),
            uuid: self.uuid,
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
    }

    pub fn close_wait(mut self) -> Result<Shard> {
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
    SetMiot(Miot),
    Close,
}

pub enum Response {
    Ok,
}

impl Threadable for Shard {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Result<Self> {
        use crate::thread::pending_requests;
        use Request::*;

        loop {
            let (qs, _empty, disconnected) = pending_requests(&rx, self.chan_size);
            for q in qs.into_iter() {
                match q {
                    (SetMiot(miot_handle), Some(tx)) => {
                        err!(IPCFail, try: tx.send(self.handle_set_miot(miot_handle)))?
                    }
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

impl Shard {
    fn handle_set_miot(&mut self, miot_handle: Miot) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        run_loop.miot = miot_handle;
        Ok(Response::Ok)
    }

    fn handle_close(&mut self) -> Result<Response> {
        use std::mem;

        let RunLoop { cluster, sessions, miot } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        info!("Shard::close, {:?} sessions:{} ...", self.name, sessions.len());

        let cluster = mem::replace(cluster, Box::new(Cluster::default()));
        *miot = mem::replace(miot, Miot::default()).close_wait()?;

        for (_, sess) in mem::replace(sessions, BTreeMap::default()).into_iter() {
            mem::drop(sess);
        }

        mem::drop(cluster);
        Ok(Response::Ok)
    }
}
