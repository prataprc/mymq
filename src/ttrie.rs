use std::marker::PhantomData;
use std::sync::{atomic::AtomicPtr, Arc};

use crate::Result;

// K = TopicName or TopicFilter, indexes (ClientID, shard_id)
pub struct TopicTrie<K, V> {
    inner: Arc<AtomicPtr<Inner<K, V>>>,
}

struct Inner<K, V> {
    count: usize, // number of entries in the trie.
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K, V> Default for TopicTrie<K, V>
where
    K: Default,
{
    fn default() -> TopicTrie<K, V> {
        let inner = Box::new(Inner {
            count: usize::default(),
            _key: PhantomData,
            _val: PhantomData,
        });
        TopicTrie { inner: Arc::new(AtomicPtr::new(Box::leak(inner))) }
    }
}

impl<K, V> TopicTrie<K, V> {
    pub fn clone(&self) -> TopicTrie<K, V> {
        todo!()
    }
}

impl<K, V> TopicTrie<K, V> {
    pub fn subscribe(&self, _key: &K) -> Result<()> {
        todo!()
    }

    pub fn unsubscribe(&self, _key: &K) -> Result<()> {
        todo!()
    }
}
