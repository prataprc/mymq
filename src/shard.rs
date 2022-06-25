use log::{debug, error, info};
use uuid::Uuid;

use std::{collections::BTreeMap, net};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{v5, ClientID, Cluster, Config, Miot, Session, Shardable};
use crate::{Error, ErrorKind, Result};

pub struct Shard {
    /// Human readable name for shard.
    pub name: String,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,
    /// Active count of sessions maintained by this shard.
    pub n_sessions: usize, // used by Cluster.
    prefix: String,
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
    /// Cluster::Tx to communicate back to cluster.
    /// Shall be dropped after close_wait call, when the thread returns, will point
    /// to Inner::Init
    cluster: Box<Cluster>,
    /// Collection of sessions and corresponding clients managed by this shard.
    /// Shall be dropped after close_wait call, when the thread returns, will be empty.
    sessions: BTreeMap<ClientID, Session>,
    /// Inner::Handle to corresponding miot-thread.
    /// Shall be dropped after close_wait call, when the thread returns, will point
    /// to Inner::Init
    miot: Miot,
}

impl Default for Shard {
    fn default() -> Shard {
        let config = Config::default();
        let mut def = Shard {
            name: format!("{}-shard-init", config.name),
            uuid: Uuid::new_v4(),
            n_sessions: Default::default(),
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
            Inner::Handle(_) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Tx(_tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Shard {
    pub fn from_config(config: Config) -> Result<Shard> {
        let def = Shard::default();
        let val = Shard {
            name: def.name.clone(),
            uuid: def.uuid,
            n_sessions: Default::default(),
            prefix: def.prefix.clone(),
            config: config.clone(),
            inner: Inner::Init,
        };

        Ok(val)
    }

    pub fn spawn(self, cluster: Cluster) -> Result<Shard> {
        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "shard can be spawned only in init-state ")?;
        }

        let shard = Shard {
            name: format!("{}-shard-main", self.config.name),
            uuid: self.uuid,
            n_sessions: Default::default(),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                cluster: Box::new(cluster),
                sessions: BTreeMap::default(),
                miot: Miot::default(),
            }),
        };
        let thrd = Thread::spawn(&self.name, shard);

        let shard = Shard {
            name: format!("{}-shard-handle", self.config.name),
            uuid: self.uuid,
            n_sessions: Default::default(),
            prefix: self.prefix.clone(),
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
        info!("{} cloning tx ...", self.prefix);

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        Shard {
            name: format!("{}-shard-tx", self.config.name),
            uuid: self.uuid,
            n_sessions: Default::default(),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        }
    }
}

// calls to interfacw with cluster-thread.
impl Shard {
    pub fn add_session(
        &self,
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    ) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::AddSession { conn, addr, pkt })??;
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn failed_connection(&self, id: ClientID, ps: Vec<v5::Packet>) -> Result<()> {
        match &self.inner {
            Inner::Tx(tx) => {
                tx.post(Request::FailedConnection { client_id: id, packets: ps })?
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn restart_miot(&self) -> Result<()> {
        match &self.inner {
            Inner::Tx(tx) => tx.post(Request::RestartChild { name: "miot" })?,
            _ => unreachable!(),
        };

        Ok(())
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
    AddSession {
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    },
    FailedConnection {
        client_id: ClientID,
        packets: Vec<v5::Packet>,
    },
    RestartChild {
        name: &'static str,
    },
    Close,
}

pub enum Response {
    Ok,
}

impl Threadable for Shard {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Self {
        use crate::{thread::pending_requests, REQ_CHANNEL_SIZE};
        use Request::*;

        info!("{} uuid:{} ...", self.prefix, self.uuid);

        let mut closed = false;
        loop {
            let (mut qs, _empty, disconnected) = pending_requests(&rx, REQ_CHANNEL_SIZE);
            if closed {
                info!("{} skipping {} requests closed:{}", self.prefix, qs.len(), closed);
                qs.drain(..);
            } else {
                debug!("{} process {} requests closed:{}", self.prefix, qs.len(), closed);
            }

            for q in qs.into_iter() {
                let res = match q {
                    (SetMiot(miot_handle), Some(tx)) => {
                        tx.send(self.handle_set_miot(miot_handle))
                    }
                    (q @ AddSession { .. }, Some(tx)) => {
                        tx.send(self.handle_add_session(q))
                    }
                    (FailedConnection { client_id: _, packets: _ }, None) => todo!(),
                    (RestartChild { name: "miot" }, None) => todo!(),
                    (Close, Some(tx)) => {
                        closed = true;
                        tx.send(self.handle_close())
                    }

                    (_, _) => unreachable!(),
                };
                match res {
                    Ok(()) if closed => break,
                    Ok(()) => (),
                    Err(err) => {
                        let msg = format!("fatal error, {}", err.to_string());
                        // TODO
                        // allow_panic!(self.as_app_tx().send(msg));
                        break;
                    }
                }
            }

            if disconnected {
                break;
            }
        }

        info!("{} thread normal exit...", self.prefix);
        self
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

    fn handle_add_session(&mut self, _req: Request) -> Result<Response> {
        todo!()
    }

    fn handle_close(&mut self) -> Result<Response> {
        use std::mem;

        let RunLoop { cluster, sessions, miot } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        info!("{} sessions:{} ...", self.prefix, sessions.len());

        let cluster = mem::replace(cluster, Box::new(Cluster::default()));
        *miot = mem::replace(miot, Miot::default()).close_wait()?;

        for (_, sess) in mem::replace(sessions, BTreeMap::default()).into_iter() {
            sess.close()?; // session is dropped here.
        }

        mem::drop(cluster);
        Ok(Response::Ok)
    }
}

impl Shard {
    fn prefix(&self) -> String {
        format!("{}", self.name)
    }

    fn as_cluster(&self) -> &Cluster {
        match &self.inner {
            Inner::Main(RunLoop { cluster, .. }) => cluster,
            _ => unreachable!(),
        }
    }
}
