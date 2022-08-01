use std::{mem, sync::Arc};

use crate::broker::Message;

/// Consensus for shard-state.
pub enum Consensus {
    Local { out_back_log: Vec<Message> },
}

impl Consensus {
    pub fn new_local() -> Self {
        Consensus::Local { out_back_log: Vec::default() }
    }

    /// Replicate a new set of `msgs` to attached replica nodes.
    pub fn replicate(&mut self, msgs: Vec<Message>) -> Vec<Message> {
        match self {
            Consensus::Local { waker, out_back_log } => {
                waker.wake().unwrap(); // TODO: handle this error.
                mem::replace(out_back_log, msgs)
            }
        }
    }
}
