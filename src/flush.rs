use log::{debug, error, info, trace, warn};

use std::{thread, time};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{queue, v5, AppTx, Config, SLEEP_10MS};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;

pub struct Flusher {
    pub name: String,
    prefix: String,
    config: Config,

    inner: Inner,
}

pub enum Inner {
    Init,
    // Held by Cluster
    Handle(Thread<Flusher, Request, Result<Response>>),
    // Held by each shard.
    Tx(Tx<Request, Result<Response>>),
    // Thread
    Main(RunLoop),
    // Held by Cluser, replacing both Handle and Main.
    Close(FinState),
}

pub struct RunLoop {
    /// Back channel to communicate with application.
    app_tx: AppTx,
}

pub struct FinState;

impl Default for Flusher {
    fn default() -> Flusher {
        let config = Config::default();
        let mut def = Flusher {
            name: format!("{}-flush-init", config.name),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_thrd) => info!("{} drop ...", self.prefix),
            Inner::Tx(_tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => info!("{} drop ...", self.prefix),
        }
    }
}

impl Flusher {
    /// Create a flusher from configuration. Flusher shall be in `Init` state. To start
    /// this flusher thread call [Flusher::spawn].
    pub fn from_config(config: Config) -> Result<Flusher> {
        let mut val = Flusher {
            name: format!("{}-flush-init", config.name),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, app_tx: AppTx) -> Result<Flusher> {
        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "flusher can be spawned only in init-state ")?;
        }

        let mut flush = Flusher {
            name: format!("{}-flush-main", self.config.name),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop { app_tx }),
        };
        flush.prefix = flush.prefix();
        let thrd = Thread::spawn(&self.prefix, flush);

        let mut flush = Flusher {
            name: format!("{}-flush-handle", self.config.name),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        flush.prefix = flush.prefix();

        Ok(flush)
    }

    pub fn to_tx(&self) -> Self {
        trace!("{} cloning tx ...", self.prefix);

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };

        let mut flush = Flusher {
            name: format!("{}-flush-tx", self.config.name),
            prefix: String::default(),
            config: self.config.clone(),
            inner,
        };
        flush.prefix = self.prefix();
        flush
    }
}

pub enum Request {
    FlushConnection {
        socket: queue::Socket,
        err: Option<Error>,
    },
    Close,
}

pub enum Response {
    Ok,
}

pub struct FlushConnectionArgs {
    pub socket: queue::Socket,
    pub err: Option<Error>,
}

// calls to interface with flusher-thread.
impl Flusher {
    pub fn flush_connection(&self, args: FlushConnectionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => thrd.request(Request::FlushConnection {
                socket: args.socket,
                err: args.err,
            })??,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn close_wait(mut self) -> Flusher {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::Close).ok();
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

impl Threadable for Flusher {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::{thread::get_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let flush_timeout = self.config.mqtt_flush_timeout();
        info!("{}, spawn thread flush_timeout:{} ...", self.prefix, flush_timeout);

        loop {
            let (mut qs, disconnected) = get_requests(&rx, CONTROL_CHAN_SIZE);

            if matches!(&self.inner, Inner::Close(_)) {
                info!("{} skipping {} requests closed:true", self.prefix, qs.len());
                qs.drain(..);
            } else {
                debug!("{} process {} requests closed:false", self.prefix, qs.len());
            }

            for q in qs.into_iter() {
                match q {
                    (q @ FlushConnection { .. }, Some(tx)) => {
                        let resp = self.handle_flush_connection(q);
                        allow_panic!(&self, tx.send(Ok(resp)));
                    }
                    (q @ Close, Some(tx)) => {
                        allow_panic!(&self, tx.send(Ok(self.handle_close(q))));
                    }
                    (_, _) => unreachable!(),
                }
            }

            match disconnected {
                true => break,
                false => (),
            };
        }

        self.handle_close(Request::Close);

        info!("{}, thread exit ...", self.prefix);

        self
    }
}

impl Flusher {
    fn handle_flush_connection(&self, req: Request) -> Response {
        use crate::miot::{rx_packets, Miot};
        use crate::packet::send_disconnect;

        let now = time::Instant::now();
        let max_size = self.config.mqtt_max_packet_size();
        let flush_timeout = self.config.mqtt_flush_timeout();

        let (mut socket, conn_err) = match req {
            Request::FlushConnection { socket, err } => (socket, err),
            _ => unreachable!(),
        };
        let prefix = format!("{}:{}:{}", self.prefix, socket.addr, *socket.client_id);

        info!("{} flush connection at {:?}", prefix, now);
        match rx_packets(&socket.wt.miot_rx, usize::MAX) {
            (qs, _empty, false) => {
                socket.wt.packets.extend_from_slice(&qs);
                error!("{} unexpected fatality, usize::MAX exhausted", prefix);
            }
            (qs, _empty, true) => {
                socket.wt.packets.extend_from_slice(&qs);
                info!("{} flush last batch, qs:{}", prefix, qs.len());
            }
        }

        let timeout = now + time::Duration::from_secs(flush_timeout as u64);
        loop {
            match Miot::flush_packets(&prefix, &self.config, &mut socket) {
                Ok(true /*would_block*/) if time::Instant::now() > timeout => {
                    error!("{} give up flush_packets after {:?}", prefix, timeout);
                    break;
                }

                Ok(true) => thread::sleep(SLEEP_10MS),
                Ok(false) => break,
                Err(err) if err.kind() == ErrorKind::Disconnected => {
                    warn!("{} stop flush, socket disconnected {}", prefix, err);
                    break;
                }
                Err(err) => unreachable!("unexpected err: {}", err),
            }
        }

        let timeout = now + time::Duration::from_secs(flush_timeout as u64);
        let code = match conn_err {
            Some(err) => v5::DisconnReasonCode::try_from(err.code() as u8).unwrap(),
            None => v5::DisconnReasonCode::NormalDisconnect,
        };
        send_disconnect(&prefix, code, &mut socket.conn, timeout, max_size).ok();

        Response::Ok
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        use std::mem;

        match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(_run_loop) => {
                info!("{} closed ...", self.prefix);

                let _init = mem::replace(&mut self.inner, Inner::Close(FinState));
                Response::Ok
            }
            Inner::Close(_) => Response::Ok,
            _ => unreachable!(),
        }
    }
}

impl Flusher {
    fn prefix(&self) -> String {
        format!("{}:flush", self.name)
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}
