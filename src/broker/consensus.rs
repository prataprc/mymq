use std::mem;

use crate::broker::Message;

/// Consensus for shard-state.
pub enum Consensus {
    Local { out_back_log: Vec<Message> },
}

impl Default for Consensus {
    fn default() -> Consensus {
        Consensus::Local { out_back_log: Vec::default() }
    }
}

impl Consensus {
    /// Replicate `msgs` in the consensus loop
    pub fn replicate_msgs(&mut self, msgs: Vec<Message>) {
        match self {
            Consensus::Local { out_back_log } if out_back_log.len() == 0 => {
                let _empty = mem::replace(out_back_log, msgs);
            }
            Consensus::Local { out_back_log } => out_back_log.extend(msgs.into_iter()),
        }
    }

    /// Obtain messages that has been successfully replicated
    pub fn replicated_msgs(&mut self) -> Vec<Message> {
        match self {
            Consensus::Local { out_back_log } => mem::replace(out_back_log, Vec::new()),
        }
    }
}
