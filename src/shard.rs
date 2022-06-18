use log::{error, info};
use uuid::Uuid;

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::Result;
use crate::{Session, Shardable};

pub struct Shard {
    /// Human readable name for shard.
    pub name: String,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,
    /// Input channel size for the shard's thread.
    pub chan_size: usize,
    inner: Inner,
}

pub enum Inner {
    Init,
    Handle(Thread<Shard, Request, Result<Response>>),
    Tx(Tx<Request, Result<Response>>),
    Main(Main),
}

pub struct Main {
    tx: Option<Tx<Request, Result<Response>>>,
    sessions: Vec<Session>,
}

impl Default for Shard {
    fn default() -> Shard {
        Shard {
            name: String::default(),
            uuid: Uuid::new_v4(),
            chan_size: Self::CHANNEL_SIZE,
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

        match &mut self.inner {
            Inner::Init => error!("Shard::drop, invalid state Inner::Init"),
            Inner::Handle(handle) => match handle.close_wait() {
                Ok(_shard) => (),
                Err(err) => error!("Shard::drop, thread result: {}", err),
            },
            Inner::Tx(tx) => mem::drop(tx),
            Inner::Main(run_loop) => {
                info!("Shard::drop, there are {} sessions", run_loop.sessions.len());
            }
        }
    }
}

impl Shard {
    pub const CHANNEL_SIZE: usize = 1024; // TODO: is this enough?

    pub fn spawn(self) -> Result<Shard> {
        let run_loop = Shard {
            name: self.name.clone(),
            uuid: self.uuid,
            chan_size: self.chan_size,
            inner: Inner::Main(Main { tx: None, sessions: Vec::default() }),
        };
        let handle = Thread::spawn_sync(&self.name, self.chan_size, run_loop);
        let tx = handle.to_tx();

        handle.post(Request::SetTx(tx.clone()))?;

        let val = Shard {
            name: self.name.clone(),
            uuid: self.uuid,
            chan_size: self.chan_size,
            inner: Inner::Handle(handle),
        };

        Ok(val)
    }

    pub fn to_tx(&self) -> Self {
        match &self.inner {
            Inner::Main(Main { tx, .. }) => Shard {
                name: self.name.clone(),
                uuid: self.uuid,
                chan_size: self.chan_size,
                inner: Inner::Tx(tx.as_ref().unwrap().clone()),
            },
            Inner::Handle(thrd) => Shard {
                name: self.name.clone(),
                uuid: self.uuid,
                chan_size: self.chan_size,
                inner: Inner::Tx(thrd.to_tx()),
            },
            _ => unreachable!(),
        }
    }
}

pub enum Request {
    SetTx(Tx<Request, Result<Response>>),
}

pub enum Response {
    Ok,
}

impl Threadable for Shard {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Result<Self> {
        use crate::thread::pending_msg;
        use Request::*;

        loop {
            let (qs, disconnected) = pending_msg(&rx, self.chan_size);
            for q in qs.into_iter() {
                match q {
                    (SetTx(newtx), None) => match &mut self.inner {
                        Inner::Main(Main { tx, .. }) => *tx = Some(newtx),
                        _ => unreachable!(),
                    },
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
