use std::marker::PhantomData;
use std::sync::{atomic::AtomicPtr, Arc};

use crate::{IterTopicPath, Result};

// K = TopicName or TopicFilter, indexes (ClientID, shard_id)
pub struct TopicTrie<V> {
    inner: Arc<Spinlock<Arc<Inner<V>>>>,
}

struct Inner<V> {
    count: usize, // number of entries in the trie.
    root: Arc<Node<V>>,
}

impl<V> Default for TopicTrie<V> {
    fn default() -> TopicTrie<V> {
        let inner = Inner {
            count: usize::default(),
            root: Node::Root { children: Vec::default() },
        };
        TopicTrie { inner }
    }
}

impl<V> TopicTrie<V> {
    pub fn clone(&self) -> TopicTrie<V> {
        todo!()
    }
}

impl<V> TopicTrie<V> {
    pub fn subscribe<K: where IterTopicPath>(&self, key: &K, value: V) -> Result<()> {
        let mut in_levels = key.iter_topic_path();

        let (count, root) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.count, Arc::clone(&inner.root))
        };

        root = Node::subscribe(node, in_levels, value);

        let inner = Inner { root, count }
        *self.inner.write() = Arc::new(inner);

        Ok(())
    }

    pub fn unsubscribe(&self, _key: &K) -> Result<()> {
        todo!()
    }
}

enum Node<V> {
    Root {
        children: Vec<Arc<Node>>, // sorted list of nodes
    },
    Child {
        name: String, // can be zero-length, also the sort key for node.
        children: Vec<Arc<Node>>,// sorted list of nodes
        values: Vec<V>, // TODO: should be make this Arc<V>
    },
}

impl<V> Node<V> {
    fn new_node(name: String) -> Node {
        Node::Child { name, children: Vec::default(), values: Vec::default() }
    }

    fn cow_clone(&self) -> Node {
        match self {
            Node::Root { children } => Node::Root { children: children.clone() }
            Node::Child { name, children, values } => Node::Child {
                name: name.clone(),
                children: children.clone(),
                values: values.clone(),
            }
        }
    }

    fn as_name(&self) -> &str {
        match self {
            Node::Child { name, .. } => name.as_str(),
            _ => unreachable!(),
        }
    }

    fn find_child(&self, in_level: &str) -> Option<&Arc<Node>> {
        let children = match self {
            Node::Root { children } => children,
            Node::Child { children, .. } => children,
        };
        match children.binary_search_by_key(in_level, |n| n.as_name()) {
            Ok(off) => Some(&children[off]),
            Err(_) => None,
        }
    }

    fn insert_node(&mut self, node: Node) {
        let children = match self {
            Node::Root { children } => children,
            Node::Child { children, .. } => children,
        };
        let name = node.as_name();
        let off = match children.binary_search_by_key(name, |n| n.as_name()) {
            Ok(off) => off,
            Err(off) => off,
        };
        children.insert(off, Arc::new(node))
    }

    fn insert_value(&mut self, value: V) {
        match self {
            Node::Child { values, .. } => value.push(value),
            _ => unreachable!(),
        }
    }

    fn subscribe<'a>(mut node: Arc<Node>, mut in_levels: I, value: V) -> Node
    where
        I: Iterator<Item = &'a str>,
        V: Clone,
    {
        let mut cow_node = node.cow_node();

        match in_level.next() {
            Some(in_level) => match node.find_child(in_level) {
                    Some(child_node) => {
                        cow_node.insert_node(
                            Node::subscribe(Arc::clone(child_node), in_levels, value)
                        );
                    }
                    None => {
                        cow_node.insert_node(
                            Node::subscribe(
                                Arc::new(Node::new_node(in_level.to_string())),
                                in_levels,
                                value
                            );
                        );
                    }
                }
            }
            None => cow_node.insert_value(value),
        };
        cow_node
    }
}


// (level_match, multi_level_match)
fn match_level(in_level: &str, trie_level: &str) -> Result<(bool, bool)> {
    match (in_level, trie_level) => {
        ("#", _) => Ok((true, true)),
        (_, "#") => Ok((true, true)),
        ("+", _) => Ok((true, false)),
        (_, "+") => Ok((true, false)),
        (in_level, trie_level) if compare_level(in_level, trie_level)? => {
            Ok((true, false))
        }
    }
}

fn compare_level(in_level: &str, trie_level: &str) -> Result<bool> {
    if in_level.chars().map(|ch| ch == '#' || ch == '+') {
        err!(InvalidInput, desc: "wildcards cannot mix with chars inp:{}", in_level)
    } else if in_level.chars().map(|ch| ch == '#' || ch == '+') {
        err!(InvalidInput, desc: "wildcards cannot mix with chars trie:{}", trie_level)
    } else if in_level == out_level {
        Ok(true)
    } else {
        Ok(false)
    }
}

