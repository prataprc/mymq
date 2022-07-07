use std::{borrow::Borrow, sync::Arc};

use crate::{Error, ErrorKind, Result};
use crate::{IterTopicPath, Spinlock};

// K = TopicFilter or TopicName, values Vec<(ClientID, shard_id)> or Vec<retain-message>
pub struct TopicTrie<V> {
    inner: Arc<Spinlock<Arc<Inner<V>>>>,
}

struct Inner<V> {
    stats: Stats,
    root: Arc<Node<V>>,
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

impl<V> Default for TopicTrie<V> {
    fn default() -> TopicTrie<V> {
        let inner = Inner {
            stats: Stats::default(),
            root: Arc::new(Node::<V>::Root { children: Vec::default() }),
        };
        TopicTrie { inner: Arc::new(Spinlock::new(Arc::new(inner))) }
    }
}

impl<V> TopicTrie<V> {
    pub fn clone(&self) -> TopicTrie<V> {
        todo!()
    }
}

impl<V> TopicTrie<V> {
    pub fn subscribe<'b, K>(&mut self, key: &'b K, value: V) -> Result<()>
    where
        K: IterTopicPath<'b>,
        V: Clone + Ord,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        let (root, first, r) = Node::sub(root, in_levels, value);
        if first {
            stats.count = stats.count.saturating_add(1);
        }
        if r {
            stats.repeat = stats.repeat.saturating_add(1);
        }

        let inner = Inner { stats, root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);

        Ok(())
    }

    pub fn unsubscribe<'a, K>(&mut self, key: &'a K, value: &V) -> Result<()>
    where
        K: IterTopicPath<'a>,
        V: Clone + Ord,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        let (root, last, m) = Node::unsub(root, in_levels, value);
        let root = root.unwrap();

        if last {
            stats.count = stats.count.saturating_sub(1);
        }
        if m {
            stats.missing = stats.missing.saturating_sub(1);
        }

        let inner = Inner { stats, root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);

        Ok(())
    }

    pub fn match_key<'b, K>(&mut self, key: &'b K) -> Result<Option<Vec<V>>>
    where
        K: IterTopicPath<'b>,
        V: Clone,
    {
        let in_levels = key.iter_topic_path();

        let (mut stats, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.stats, Arc::clone(&inner.root))
        };

        stats.lookups = stats.lookups.saturating_add(1);

        let res = match Node::match_topic(root.as_ref(), in_levels)? {
            Some(vals) => {
                stats.hits = stats.hits.saturating_add(1);
                Ok(Some(vals))
            }
            None => Ok(None),
        };

        let inner = Inner { stats, root: Arc::clone(&root) };
        *self.inner.write() = Arc::new(inner);

        res
    }
}

enum Node<V> {
    Root {
        children: Vec<Arc<Node<V>>>, // sorted list of nodes
    },
    Child {
        name: String,                // can be zero-length, also the sort key for node.
        children: Vec<Arc<Node<V>>>, // sorted list of nodes
        values: Vec<V>,              // TODO: should be make this Arc<V>
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

    // return (root, first, repeat)
    fn sub<'a, K>(node: Arc<Node<V>>, mut in_levels: K, value: V) -> (Node<V>, bool, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone + Ord,
    {
        let mut cow_node = node.as_ref().cow_clone();

        match in_levels.next() {
            Some(in_level) => {
                let children = match &mut cow_node {
                    Node::Root { children } => children,
                    Node::Child { children, .. } => children,
                };
                let r = match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        (Node::sub(child, in_levels, value), off)
                    }
                    Err(off) => {
                        let child = Arc::new(Node::<V>::new_node(in_level.to_string()));
                        (Node::sub(child, in_levels, value), off)
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
    fn unsub<'a, K>(
        node: Arc<Node<V>>,
        mut inl: K,
        val: &V,
    ) -> (Option<Node<V>>, bool, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone + Ord,
    {
        let mut cow_node = node.as_ref().cow_clone();

        match inl.next() {
            Some(in_level) => {
                let (is_root, children) = match &mut cow_node {
                    Node::Root { children } => (true, children),
                    Node::Child { children, .. } => (false, children),
                };
                match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        match Node::unsub(child, inl, val) {
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

    fn match_topic<'a, I>(node: &Node<V>, mut in_levels: I) -> Result<Option<Vec<V>>>
    where
        I: Iterator<Item = &'a str> + Clone,
        V: Clone,
    {
        let in_level = in_levels.next();

        if in_level == None {
            match node {
                Node::Child { values, .. } => return Ok(Some(values.to_vec())),
                Node::Root { .. } => return Ok(None),
            }
        }

        let in_level = in_level.unwrap();

        let children = match node {
            Node::Root { children } => children.iter(),
            Node::Child { children, .. } => children.iter(),
        };

        let mut acc = vec![];
        for child in children {
            match match_level(in_level, child.as_name())? {
                (_slevel, true) => {
                    if let Node::Child { values, .. } = child.borrow() {
                        acc.extend(values.to_vec().into_iter());
                    }
                }
                (true, _mlevel) => (),
                (false, false) if acc.len() == 0 => return Ok(None),
                (false, false) => return Ok(Some(acc)),
            }
            match Node::match_topic(child, in_levels.clone())? {
                Some(values) => acc.extend(values.into_iter()),
                None => (),
            }
        }

        Ok(Some(acc))
    }
}

// (level_match, multi_level_match)
fn match_level(in_lvl: &str, trie_level: &str) -> Result<(bool, bool)> {
    match (in_lvl, trie_level) {
        ("#", _) => Ok((true, true)),
        (_, "#") => Ok((true, true)),
        ("+", _) => Ok((true, false)),
        (_, "+") => Ok((true, false)),
        (in_lvl, trie_level) if compare_level(in_lvl, trie_level)? => Ok((true, false)),
        (_, _) => Ok((false, false)),
    }
}

fn compare_level(in_level: &str, trie_level: &str) -> Result<bool> {
    // TODO: If inputs are already validated we may not need wildcard checks
    if in_level.chars().any(|ch| ch == '#' || ch == '+') {
        err!(InvalidInput, desc: "wildcards cannot mix with chars inp:{}", in_level)
    } else if in_level.chars().any(|ch| ch == '#' || ch == '+') {
        err!(InvalidInput, desc: "wildcards cannot mix with chars trie:{}", trie_level)
    } else if in_level == trie_level {
        Ok(true)
    } else {
        Ok(false)
    }
}
