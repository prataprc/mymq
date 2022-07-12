use std::{borrow::Borrow, sync::Arc};

use crate::{v5, v5::Subscription, IterTopicPath, Spinlock};

/// Trie for managing subscriptions.
pub struct SubscribedTrie {
    inner: Arc<Spinlock<Arc<Inner<Subscription>>>>,
}

struct Inner<V> {
    stats: Stats,
    root: Arc<Node<V>>,
}

impl Default for SubscribedTrie {
    fn default() -> SubscribedTrie {
        let inner = Inner {
            stats: Stats::default(),
            root: Arc::new(Node::<Subscription>::Root { children: Vec::default() }),
        };
        SubscribedTrie { inner: Arc::new(Spinlock::new(Arc::new(inner))) }
    }
}

impl SubscribedTrie {
    pub fn clone(&self) -> SubscribedTrie {
        SubscribedTrie { inner: Arc::clone(&self.inner) }
    }
}

impl SubscribedTrie {
    pub fn subscribe<'b, K>(&self, key: &'b K, value: Subscription)
    where
        K: IterTopicPath<'b>,
    {
        self.do_subscribe(key, value)
    }

    pub fn unsubscribe<'a, K>(&self, key: &'a K, value: &Subscription)
    where
        K: IterTopicPath<'a>,
    {
        self.do_unsubscribe(key, value);
    }

    pub fn match_key<'b, K>(&self, key: &'b K) -> Vec<Subscription>
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        stats.lookups = stats.lookups.saturating_add(1);

        let matches = match Node::match_topic(root.as_ref(), in_levels) {
            Some(vals) => {
                stats.hits = stats.hits.saturating_add(1);
                vals
            }
            None => Vec::new(),
        };

        let inner = Inner { stats, root: Arc::clone(&root) };
        *self.inner.write() = Arc::new(inner);

        matches
    }
}

impl SubscribedTrie {
    fn do_subscribe<'b, K>(&self, key: &'b K, value: Subscription)
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        let (root, first, repeat) = root.sub(in_levels, value);
        if first {
            stats.count = stats.count.saturating_add(1);
        }
        if repeat {
            stats.repeat = stats.repeat.saturating_add(1);
        }

        let inner = Inner { stats, root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);
    }

    pub fn do_unsubscribe<'a, K>(&self, key: &'a K, value: &Subscription)
    where
        K: IterTopicPath<'a>,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        let (root, last, missing) = root.unsub(in_levels, value);
        let root = root.unwrap();

        if last {
            stats.count = stats.count.saturating_sub(1);
        }
        if missing {
            stats.missing = stats.missing.saturating_sub(1);
        }

        let inner = Inner { stats, root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);
    }
}

/// Trie for managing retain messages.
pub struct RetainedTrie {
    inner: Arc<Spinlock<Arc<Inner<v5::Publish>>>>,
}

impl Default for RetainedTrie {
    fn default() -> RetainedTrie {
        let inner = Inner {
            stats: Stats::default(),
            root: Arc::new(Node::<v5::Publish>::Root { children: Vec::default() }),
        };
        RetainedTrie { inner: Arc::new(Spinlock::new(Arc::new(inner))) }
    }
}

impl RetainedTrie {
    pub fn clone(&self) -> RetainedTrie {
        RetainedTrie { inner: Arc::clone(&self.inner) }
    }
}

impl RetainedTrie {
    pub fn set<'b, K>(&self, key: &'b K, value: v5::Publish)
    where
        K: IterTopicPath<'b>,
    {
        self.do_set(key, value)
    }

    pub fn remove<'a, K>(&self, key: &'a K)
    where
        K: IterTopicPath<'a>,
    {
        self.do_remove(key)
    }

    pub fn match_key<'b, K>(&self, key: &'b K) -> Option<v5::Publish>
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        stats.lookups = stats.lookups.saturating_add(1);

        let res = match Node::match_topic(root.as_ref(), in_levels) {
            Some(mut vals) => {
                assert!(vals.len() == 1);
                stats.hits = stats.hits.saturating_add(1);
                Some(vals.remove(0))
            }
            None => None,
        };

        let inner = Inner { stats, root: Arc::clone(&root) };
        *self.inner.write() = Arc::new(inner);

        res
    }
}

impl RetainedTrie {
    fn do_set<'b, K>(&self, key: &'b K, value: v5::Publish)
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        let (root, first) = root.set(in_levels, value);
        if first {
            stats.count = stats.count.saturating_add(1);
        }

        let inner = Inner { stats, root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);
    }

    pub fn do_remove<'a, K>(&self, key: &'a K)
    where
        K: IterTopicPath<'a>,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        let (root, missing) = root.remove(in_levels);
        let root = root.unwrap();

        if missing {
            stats.missing = stats.missing.saturating_sub(1);
        } else {
            stats.count = stats.count.saturating_sub(1);
        }

        let inner = Inner { stats, root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);
    }
}

enum Node<V> {
    Root {
        children: Vec<Arc<Node<V>>>, // sorted list of nodes
    },
    Child {
        name: String,                // can be zero-length, also the sort key for node.
        children: Vec<Arc<Node<V>>>, // sorted list of nodes
        values: Vec<V>,              // TODO: should we make this Arc<V>
    },
}

impl<V> Node<V> {
    fn new_node(name: String) -> Node<V> {
        Node::<V>::Child {
            name,
            children: Vec::default(),
            values: Vec::default(),
        }
    }

    fn cow_clone(&self) -> Node<V>
    where
        V: Clone,
    {
        match self {
            Node::Root { children } => Node::Root { children: children.clone() },
            Node::Child { name, children, values } => Node::Child {
                name: name.clone(),
                children: children.clone(),
                values: values.to_vec(),
            },
        }
    }

    fn as_name(&self) -> &str {
        match self {
            Node::Child { name, .. } => name.as_str(),
            _ => unreachable!(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Node::Child { children, values, .. } => {
                children.len() == 0 && values.len() == 0
            }
            Node::Root { .. } => false,
        }
    }

    // return (first, repeat)
    // `first` is whether this is the first time a topic is subscribed.
    fn insert_value(&mut self, value: V) -> (bool, bool)
    where
        V: Ord,
    {
        match self {
            Node::Child { values, .. } if values.len() == 0 => {
                values.push(value);
                (true, false)
            }
            Node::Child { values, .. } => match values.binary_search(&value) {
                Err(off) => {
                    values.insert(off, value);
                    (false, false)
                }
                Ok(_off) => (false, true),
            },
            _ => unreachable!(),
        }
    }

    // return (first,)
    // `first` is whether this is the first time a topic is subscribed.
    fn replace_value(&mut self, value: V) -> bool {
        match self {
            Node::Child { values, .. } if values.len() == 0 => {
                *values = vec![value];
                true
            }
            Node::Child { values, .. } => {
                *values = vec![value];
                false
            }
            _ => unreachable!(),
        }
    }

    // return (last, missing)
    // `last` is the last value for this topic.
    fn remove_value(&mut self, value: &V) -> (bool, bool)
    where
        V: Ord,
    {
        match self {
            Node::Child { values, .. } if values.len() == 0 => unreachable!(),
            Node::Child { values, .. } if values.len() == 1 => {
                match &values[0] == value {
                    true => {
                        values.remove(0);
                        (true, false)
                    }
                    false => (false, true),
                }
            }
            Node::Child { values, .. } => match values.binary_search(value) {
                Ok(off) => {
                    values.remove(off);
                    (false, false)
                }
                Err(_off) => (false, true),
            },
            Node::Root { .. } => unreachable!(),
        }
    }

    // return (missing,)
    fn remove_topic(&mut self) -> bool {
        match self {
            Node::Child { values, .. } if values.len() == 1 => {
                values.remove(0);
                false
            }
            Node::Child { .. } => unreachable!(),
            Node::Root { .. } => unreachable!(),
        }
    }

    // return (root, first, repeat)
    fn sub<'a, K>(&self, mut in_levels: K, value: V) -> (Node<V>, bool, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone + Ord,
    {
        let mut cow_node = self.cow_clone();

        match in_levels.next() {
            Some(in_level) => {
                let children = match &mut cow_node {
                    Node::Root { children } => children,
                    Node::Child { children, .. } => children,
                };
                let r = match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        (child.sub(in_levels, value), off)
                    }
                    Err(off) => {
                        let child = Arc::new(Node::<V>::new_node(in_level.to_string()));
                        (child.sub(in_levels, value), off)
                    }
                };
                let ((child, first, repeat), off) = r;
                children.insert(off, Arc::new(child));
                (cow_node, first, repeat)
            }
            None => {
                let (first, repeat) = cow_node.insert_value(value);
                (cow_node, first, repeat)
            }
        }
    }

    // return (root, last, missing)
    fn unsub<'a, K>(&self, mut inl: K, val: &V) -> (Option<Node<V>>, bool, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone + Ord,
    {
        let mut cow_node = self.cow_clone();

        match inl.next() {
            Some(in_level) => {
                let (is_root, children) = match &mut cow_node {
                    Node::Root { children } => (true, children),
                    Node::Child { children, .. } => (false, children),
                };
                match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        match child.unsub(inl, val) {
                            (Some(child), last, miss) => {
                                children.insert(off, Arc::new(child));
                                (Some(cow_node), last, miss)
                            }
                            (None, last, mi) if is_root => (Some(cow_node), last, mi),
                            (None, last, mi) if cow_node.is_empty() => (None, last, mi),
                            (None, last, mi) => (Some(cow_node), last, mi),
                        }
                    }
                    Err(_off) => (Some(cow_node), false, true),
                }
            }
            None => match cow_node.remove_value(val) {
                (last, miss) if cow_node.is_empty() => (None, last, miss),
                (last, miss) => (Some(cow_node), last, miss),
            },
        }
    }

    // return (root, first)
    fn set<'a, K>(&self, mut in_levels: K, value: V) -> (Node<V>, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone,
    {
        let mut cow_node = self.cow_clone();

        match in_levels.next() {
            Some(in_level) => {
                let children = match &mut cow_node {
                    Node::Root { children } => children,
                    Node::Child { children, .. } => children,
                };
                let r = match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        (child.set(in_levels, value), off)
                    }
                    Err(off) => {
                        let child = Arc::new(Node::<V>::new_node(in_level.to_string()));
                        (child.set(in_levels, value), off)
                    }
                };
                let ((child, first), off) = r;
                children.insert(off, Arc::new(child));
                (cow_node, first)
            }
            None => {
                let first = cow_node.replace_value(value);
                (cow_node, first)
            }
        }
    }

    // return (root, missing)
    fn remove<'a, K>(&self, mut inl: K) -> (Option<Node<V>>, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone,
    {
        let mut cow_node = self.cow_clone();

        match inl.next() {
            Some(in_level) => {
                let (is_root, children) = match &mut cow_node {
                    Node::Root { children } => (true, children),
                    Node::Child { children, .. } => (false, children),
                };
                match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        match child.remove(inl) {
                            (Some(child), missing) => {
                                children.insert(off, Arc::new(child));
                                (Some(cow_node), missing)
                            }
                            (None, missing) if is_root => (Some(cow_node), missing),
                            (None, missing) if cow_node.is_empty() => (None, missing),
                            (None, missing) => (Some(cow_node), missing),
                        }
                    }
                    Err(_off) => (Some(cow_node), true),
                }
            }
            None => match cow_node.remove_topic() {
                missing if cow_node.is_empty() => (None, missing),
                missing => (Some(cow_node), missing),
            },
        }
    }

    fn match_topic<'a, I>(&self, mut in_levels: I) -> Option<Vec<V>>
    where
        I: Iterator<Item = &'a str> + Clone,
        V: Clone,
    {
        let in_level = in_levels.next();

        if in_level == None {
            match self {
                Node::Child { values, .. } => return Some(values.to_vec()),
                Node::Root { .. } => return None,
            }
        }

        let in_level = in_level.unwrap();

        let children = match self {
            Node::Root { children } => children.iter(),
            Node::Child { children, .. } => children.iter(),
        };

        let mut acc = vec![];
        for child in children {
            match match_level(in_level, child.as_name()) {
                (_slevel, true) => {
                    if let Node::Child { values, .. } = child.borrow() {
                        acc.extend(values.to_vec().into_iter());
                    }
                }
                (true, _mlevel) => (),
                (false, false) if acc.len() == 0 => return None,
                (false, false) => return Some(acc),
            }
            match Node::match_topic(child, in_levels.clone()) {
                Some(values) => acc.extend(values.into_iter()),
                None => (),
            }
        }

        Some(acc)
    }
}

// (level_match, multi_level_match)
// input key must have be already validated !!
fn match_level(in_lvl: &str, trie_level: &str) -> (bool, bool) {
    match (in_lvl, trie_level) {
        ("#", _) => (true, true),
        (_, "#") => (true, true),
        ("+", _) => (true, false),
        (_, "+") => (true, false),
        (in_lvl, trie_level) if compare_level(in_lvl, trie_level) => (true, false),
        (_, _) => (false, false),
    }
}

fn compare_level(in_level: &str, trie_level: &str) -> bool {
    if in_level == trie_level {
        true
    } else {
        false
    }
}

#[derive(Clone, Copy, Default)]
pub struct Stats {
    // number of topics in the trie.
    pub count: usize,
    // number repeated inserts of same topic.
    pub repeat: usize,
    // number of missing topics removed.
    pub missing: usize,
    // total number of matches
    pub lookups: usize,
    // number of hits
    pub hits: usize,
}
