//! Module `thread` implement a generic multi-threading pattern.
//!
//! It is inspired from gen-server model from Erlang, where by, every thread is
//! expected to hold onto its own state, and handle all inter-thread communication
//! via channels and message queues.

use log::warn;

use std::sync::{mpsc, Arc};
use std::thread;

use crate::broker::QueueStatus;
use crate::{Error, ErrorKind, Result};

pub type QueueReq<Q, R> = QueueStatus<(Q, Option<mpsc::Sender<R>>)>;

/// Trait can be used to spawn a type's instance as a thread.
///
/// Our threading model follows Erlang's OTP philosopy. There is no way for threads to
/// communicate with rest of the system by sharing memory. Other threads can communicate
/// using the channels carrying types `Req` for requests and `Resp` for response.
pub trait Threadable: Sized {
    /// Request type that the implementing type can handle.
    type Req;
    /// Response type that the implementing type shall return.
    type Resp;

    /// Entry point for the thread.
    fn main_loop(self, rx: Rx<Self::Req, Self::Resp>) -> Self;
}

/// IPC type, that enumerates as either [mpsc::Sender] or, [mpsc::SyncSender] channel.
///
/// The clone behavior is similar to [mpsc::Sender] or, [mpsc::SyncSender].
pub enum Tx<Q, R = ()> {
    N(mpsc::Sender<(Q, Option<mpsc::Sender<R>>)>, Option<Arc<mio::Waker>>),
    S(mpsc::SyncSender<(Q, Option<mpsc::Sender<R>>)>, Option<Arc<mio::Waker>>),
}

impl<Q, R> Clone for Tx<Q, R> {
    fn clone(&self) -> Self {
        match self {
            Tx::N(tx, waker) => Tx::N(tx.clone(), waker.as_ref().map(Arc::clone)),
            Tx::S(tx, waker) => Tx::S(tx.clone(), waker.as_ref().map(Arc::clone)),
        }
    }
}

impl<Q, R> Tx<Q, R> {
    /// Post a message to thread and don't wait for response.
    pub fn post(&self, msg: Q) -> Result<()>
    where
        Q: 'static + Send,
        R: 'static + Send,
    {
        match self {
            Tx::N(tx, waker) => {
                err!(IPCFail, try: tx.send((msg, None)))?;
                match waker {
                    Some(waker) => err!(IOError, try: waker.wake()),
                    None => Ok(()),
                }
            }
            Tx::S(tx, waker) => {
                err!(IPCFail, try: tx.send((msg, None)))?;
                match waker {
                    Some(waker) => err!(IOError, try: waker.wake()),
                    None => Ok(()),
                }
            }
        }
    }

    /// Send a request message to thread and wait for a response.
    pub fn request(&self, request: Q) -> Result<R>
    where
        Q: 'static + Send,
        R: 'static + Send,
    {
        let (stx, srx) = mpsc::channel();
        match self {
            Tx::N(tx, waker) => {
                err!(IPCFail, try: tx.send((request, Some(stx))))?;
                match waker {
                    Some(waker) => err!(IOError, try: waker.wake())?,
                    None => (),
                }
            }
            Tx::S(tx, waker) => {
                err!(IPCFail, try: tx.send((request, Some(stx))))?;
                match waker {
                    Some(waker) => err!(IOError, try: waker.wake())?,
                    None => (),
                }
            }
        }

        err!(IPCFail, try: srx.recv())
    }

    /// Send a request message to thread and caller can receive on other end of
    /// `resp_tx`.
    pub fn request_with(&self, request: Q, resp_tx: mpsc::Sender<R>) -> Result<()>
    where
        Q: 'static + Send,
        R: 'static + Send,
    {
        match self {
            Tx::N(tx, waker) => {
                err!(IPCFail, try: tx.send((request, Some(resp_tx))))?;
                match waker {
                    Some(waker) => err!(IOError, try: waker.wake()),
                    None => Ok(()),
                }
            }
            Tx::S(tx, waker) => {
                err!(IPCFail, try: tx.send((request, Some(resp_tx))))?;
                match waker {
                    Some(waker) => err!(IOError, try: waker.wake()),
                    None => Ok(()),
                }
            }
        }
    }
}

/// IPC-Type alias for parametrised [mpsc::Receiver]. Shall be passed to the
/// thread's main loop.
///
/// Refer to [Thread::spawn] for details.
pub type Rx<Q, R = ()> = mpsc::Receiver<(Q, Option<mpsc::Sender<R>>)>;

/// Thread type, providing gen-server pattern to do multi-threading.
///
/// Parametrized over
/// * **Q**: Request type.
/// * **R**: Optional, response type.
///
/// NOTE: When a thread value is dropped, it is made sure that there are no dangling
/// thread routines. To achieve this following requirements need to be satisfied:
///
/// * All `tx` and its close must be closed.
/// * The thread's main loop should handle _disconnect_ signal on its [Rx] channel.
/// * Call `close_wait()` on this thread instance, for a clean exit.
pub struct Thread<T, Q, R = ()>
where
    T: 'static + Send + Threadable<Req = Q, Resp = R>,
    Q: 'static + Send,
    R: 'static + Send,
{
    name: String,
    handle: Option<thread::JoinHandle<T>>,
    tx: Option<Tx<Q, R>>,
}

impl<T, Q, R> Drop for Thread<T, Q, R>
where
    T: 'static + Send + Threadable<Req = Q, Resp = R>,
    Q: 'static + Send,
    R: 'static + Send,
{
    fn drop(&mut self) {
        use std::panic;

        if self.handle.is_some() || self.tx.is_some() {
            panic!("call close_wait() before dropping thread {:?}", self.name);
        }
    }
}

impl<T, Q, R> Thread<T, Q, R>
where
    T: 'static + Send + Threadable<Req = Q, Resp = R>,
    Q: 'static + Send,
    R: 'static + Send,
{
    /// Create a new Thread instance, using asynchronous channel with infinite buffer.
    ///
    /// `T` Threads context, when thread is spawned take ownership and calls `main_loop`
    pub fn spawn(name: &str, thrd: T) -> Thread<T, Q, R> {
        let (tx, rx) = mpsc::channel();
        Thread {
            name: name.to_string(),
            handle: Some(thread::spawn(move || thrd.main_loop(rx))),
            tx: Some(Tx::N(tx, None)),
        }
    }

    /// Create a new Thread instance, using synchronous channel with finite buffer.
    pub fn spawn_sync(name: &str, chan_size: usize, thrd: T) -> Thread<T, Q, R> {
        let (tx, rx) = mpsc::sync_channel(chan_size);

        Thread {
            name: name.to_string(),
            handle: Some(thread::spawn(move || thrd.main_loop(rx))),
            tx: Some(Tx::S(tx, None)),
        }
    }

    /// Set a waker on this thread, so that for every post/request calls the thred
    /// is woken up
    pub fn set_waker(&mut self, waker: Arc<mio::Waker>) {
        self.tx = match self.tx.take() {
            Some(Tx::N(tx, None)) => Some(Tx::N(tx, Some(Arc::clone(&waker)))),
            Some(Tx::S(tx, None)) => Some(Tx::S(tx, Some(Arc::clone(&waker)))),
            _ => unreachable!(),
        };
    }

    /// Return a clone of tx channel.
    pub fn to_tx(&self) -> Tx<Q, R> {
        self.tx.clone().unwrap()
    }

    /// Return name of this thread.
    pub fn to_name(&self) -> String {
        self.name.to_string()
    }

    /// Must way to exit/shutdown the thread. Note that all [Tx] clones of this
    /// thread must also be dropped for this call to return.
    ///
    /// Even otherwise, when Thread value goes out of scope its drop implementation
    /// shall call this method to exit the thread, except that any errors are ignored.
    pub fn close_wait(mut self) -> T {
        use std::{mem, panic};

        mem::drop(self.tx.take());

        let handle = self.handle.take().unwrap();
        match handle.join() {
            Ok(thread_val) => thread_val,
            Err(err) => panic::resume_unwind(err),
        }
    }

    /// If thread does not need to join back with its parent, then parent thread can
    /// call drop() instead of close_wait().
    pub fn drop(mut self) {
        std::mem::drop(self.tx.take());
        let _handle = self.handle.take().unwrap();
    }
}

impl<T, Q, R> Thread<T, Q, R>
where
    T: 'static + Send + Threadable<Req = Q, Resp = R>,
    Q: 'static + Send,
    R: 'static + Send,
{
    /// Post a message to thread and don't wait for response.
    pub fn post(&self, msg: Q) -> Result<()> {
        match &self.tx {
            Some(tx) => tx.post(msg),
            None => unreachable!(),
        }
    }

    /// Send a request message to thread and wait for a response.
    pub fn request(&self, request: Q) -> Result<R> {
        match &self.tx {
            Some(tx) => tx.request(request),
            None => unreachable!(),
        }
    }

    /// Send a request message to thread and caller can receive on other end of
    /// `resp_tx`.
    pub fn request_with(&self, request: Q, resp_tx: mpsc::Sender<R>) -> Result<()> {
        match &self.tx {
            Some(tx) => tx.request_with(request, resp_tx),
            None => unreachable!(),
        }
    }
}

/// Uses non-blocking `try_recv`. For blocking read, use get_requests.
pub fn pending_requests<Q, R>(prefix: &str, rx: &Rx<Q, R>, max: usize) -> QueueReq<Q, R> {
    let mut reqs = Vec::with_capacity(max);
    loop {
        match rx.try_recv() {
            Ok(req) if reqs.len() < max => reqs.push(req),
            Ok(req) => {
                reqs.push(req);
                break QueueReq::Ok(reqs);
            }
            Err(mpsc::TryRecvError::Empty) => break QueueReq::Block(reqs),
            Err(mpsc::TryRecvError::Disconnected) => {
                warn!("{} req-channel disconnected ...", prefix);
                break QueueReq::Disconnected(reqs);
            }
        }
    }
}

/// Return (requests, disconnected), uses blocking `recv`. For nond-blocking version
/// use pending_requests.
pub fn get_requests<Q, R>(prefix: &str, rx: &Rx<Q, R>, max: usize) -> QueueReq<Q, R> {
    let mut reqs = Vec::with_capacity(max);
    let req = match rx.recv() {
        Ok(req) if reqs.len() < max => req,
        Ok(req) => req,
        Err(mpsc::RecvError) => {
            warn!("{} req-channel disconnected ...", prefix);
            return QueueReq::Disconnected(reqs);
        }
    };
    reqs.push(req);

    let mut status = pending_requests(prefix, rx, max);
    reqs.extend(status.take_values().into_iter());

    QueueReq::Ok(reqs)
}
