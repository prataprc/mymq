use log::{debug, error, info, trace};

use std::{thread, time};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{queue, AppTx, Config, SLEEP_10MS};
use crate::{Error, ErrorKind, ReasonCode, Result};

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
}

pub struct RunLoop {
    /// Back channel to communicate with application.
    app_tx: AppTx,
    /// thread is already closed.
    closed: bool,
}

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

impl Flusher {
    /// Create a flusher from configuration. Flusher shall be in `Init` state. To start
    /// this flusher thread call [Flusher::spawn].
    pub fn from_config(config: Config) -> Result<Flusher> {
        let def = Flusher::default();
        let mut val = Flusher {
            name: def.name.clone(),
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
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop { app_tx, closed: false }),
        };
        flush.prefix = flush.prefix();
        let thrd = Thread::spawn(&self.prefix, flush);

        let mut flush = Flusher {
            name: format!("{}-flush-handle", self.config.name),
            prefix: self.prefix.clone(),
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
            prefix: self.prefix.clone(),
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

// calls to interfacw with cluster-thread.
impl Flusher {
    pub fn flush_connection_err(&self, socket: queue::Socket, err: Error) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::FlushConnection { socket, err: Some(err) })??
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn flush_connection(&self, socket: queue::Socket) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::FlushConnection { socket, err: None })??
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn close_wait(mut self) -> Result<Flusher> {
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

impl Threadable for Flusher {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx) -> Self {
        use crate::{thread::get_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let flush_timeout = self.config.mqtt_flush_timeout();

        info!("{}, spawn thread flush_timeout:{} ...", self.prefix, flush_timeout);

        'outer: loop {
            let (qs, disconnected) = get_requests(&rx, CONTROL_CHAN_SIZE);

            for q in qs.into_iter() {
                match q {
                    (q @ FlushConnection { .. }, Some(tx)) => {
                        err!(IPCFail, try: tx.send(Ok(self.handle_flush_connection(q))))
                            .ok();
                    }
                    (q @ Close, Some(tx)) => {
                        err!(IPCFail, try: tx.send(Ok(self.handle_close(q)))).ok();
                        break 'outer;
                    }
                    (_, _) => unreachable!(),
                }
            }

            if disconnected {
                break;
            };
        }

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
        let msg_batch_size = self.config.mqtt_msg_batch_size() as usize;

        let (mut socket, conn_err) = match req {
            Request::FlushConnection { socket, err } => (socket, err),
            _ => unreachable!(),
        };
        let prefix = format!("{}:{}:{}", self.prefix, socket.addr, *socket.client_id);

        info!("{} flush connection at {:?}", prefix, now);

        let timeout = now + time::Duration::from_secs(flush_timeout as u64);
        loop {
            match Miot::send_upstream(&prefix, &mut socket) {
                Ok(true /*would_block*/) if time::Instant::now() > timeout => break,
                Ok(true) => thread::sleep(SLEEP_10MS),
                Ok(false) => break,
                Err(err) if err.kind() == ErrorKind::RxClosed => break,
                Err(err) => unreachable!("{}", err),
            }
        }
        debug!("{} flushed packets upstream, newer ones shall be ignored", prefix);

        loop {
            match Miot::flush_packets(&prefix, &self.config, &mut socket) {
                Ok(true /*would_block*/) if time::Instant::now() > timeout => break,
                Ok(true) => thread::sleep(SLEEP_10MS),
                Ok(false) => match rx_packets(&socket.wt.miot_rx, msg_batch_size) {
                    (qs, _empty, false) => {
                        socket.wt.packets.extend_from_slice(&qs);
                        trace!("{} flush packets from upstream", prefix);
                    }
                    (qs, _empty, true) => {
                        socket.wt.packets.extend_from_slice(&qs);
                        info!("{} flush last batch, upstream finished", prefix);
                        break;
                    }
                },
                Err(err) if err.kind() == ErrorKind::Disconnected => {
                    error!("{} stop flush, socket disconnected {}", prefix, err);
                    break;
                }
                Err(err) => unreachable!("unexpected err: {}", err),
            }
        }

        let timeout = now + time::Duration::from_secs(flush_timeout as u64);
        let code = conn_err.as_ref().map(|err| err.code()).unwrap_or(ReasonCode::Success);
        send_disconnect(&prefix, timeout, max_size, code, &mut socket.conn).ok();

        Response::Ok
    }

    fn handle_close(&mut self, _req: Request) -> Response {
        let RunLoop { closed, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        if *closed == false {
            info!("{} closed ...", self.prefix);
            *closed = true;
        }
        Response::Ok
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
