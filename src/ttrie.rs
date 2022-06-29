use std::sync::{atomic::AtomicPtr, Arc};

pub struct TopicTrie {
    inner: Arc<AtomicPtr<Inner>>,
}

struct Inner {
    count: usize, // number of entries in the trie.
}

impl TopicTrie {
    pub fn new() -> TopicTrie {
        let inner = Box::new(Inner { count: usize::default() });
        TopicTrie { inner: Arc::new(AtomicPtr::new(Box::leak(inner))) }
    }

    pub fn clone(&self) -> TopicTrie {
        todo!()
    }
}
