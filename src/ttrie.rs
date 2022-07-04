use std::sync::{atomic::AtomicPtr, Arc};

use crate::Result;
use crate::TopicFilter;

// <TopicFilter, (ClientID, shard_id)
pub struct TopicTrie {
    inner: Arc<AtomicPtr<Inner>>,
}

struct Inner {
    count: usize, // number of entries in the trie.
}

impl Default for TopicTrie {
    fn default() -> TopicTrie {
        let inner = Box::new(Inner { count: usize::default() });
        TopicTrie { inner: Arc::new(AtomicPtr::new(Box::leak(inner))) }
    }
}

impl TopicTrie {
    pub fn clone(&self) -> TopicTrie {
        todo!()
    }
}

impl TopicTrie {
    pub fn unsubscribe(&self, _tfilter: &TopicFilter) -> Result<()> {
        todo!()
    }
}
