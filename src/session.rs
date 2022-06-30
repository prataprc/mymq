use std::collections::{BTreeMap, VecDeque};
use std::net;

use crate::{queue, v5, ClientID, Config, TopicFilter, TopicTrie};

// TODO A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is
// set to 0.
// TODO Revisit 2.2.1 Packet Identifier

pub struct SessionArgs {
    pub addr: net::SocketAddr,
    pub client_id: ClientID,
    pub miot_tx: queue::QueueTx,
    pub miot_rx: queue::QueueRx,
    pub topic_filters: TopicTrie,
}

pub struct Session {
    /// Remote socket address.
    addr: net::SocketAddr,
    /// Outbound channel to Miot thread.
    miot_tx: queue::QueueTx,
    /// Inbound channel from Miot thread.
    miot_rx: queue::QueueRx,
    /// A clone of this Tx channel will be added to every shard in this node. Other
    /// sessions can send messages to this session.
    tx: queue::QueueTx,
    /// Inbound channel from other sessions.
    rx: queue::QueueRx,
    config: Config,

    state: SessionState,

    // a clone of Cluster::topic_filters
    topic_filters: TopicTrie,
    // inbound queue from client.
    cinp: queue::ClientInp,
    // A Periodic Message::LocalAck shall be sent to other sessions. Every incoming
    // PUBLISH from a local-session will be indexed with its cliend_id and
    // Message::Packet::seqno. Only the latest seqno shall be indexed here, and once
    // the ack is sent back, entry shall be removed from this index.
    timestamp: BTreeMap<ClientID, u64>,
}

/// SessionState is a candidate for synchronisation with replicas.
struct SessionState {
    /// Client's ClientID that created this session.
    client_id: ClientID,
    /// List of topic-filters subscribed by this client, when ever SUBSCRIBE/UNSUBSCRIBE
    /// messages are committed here, [Cluster::topic_filter] will also be updated.
    subscriptions: Vec<Subscription>,
    /// Manages out-bound messages to client.
    cout: queue::ClientOut,
}

struct Subscription {
    subscription_id: u32,
    topic_filter: TopicFilter,
}

impl Session {
    pub fn start(args: SessionArgs, config: Config, _pkt: v5::Connect) -> Session {
        use crate::MSG_CHANNEL_SIZE;

        let cinp = queue::ClientInp {
            seqno: 0,
            index: BTreeMap::default(),
            timestamp: BTreeMap::default(),
        };
        let cout = queue::ClientOut {
            seqno: 0,
            index: BTreeMap::default(),
            packet_ids: VecDeque::default(),
        };
        let state = SessionState {
            client_id: args.client_id,
            subscriptions: Vec::default(),
            cout,
        };

        let (tx, rx) = queue::queue_channel(MSG_CHANNEL_SIZE);
        Session {
            addr: args.addr,
            miot_tx: args.miot_tx,
            miot_rx: args.miot_rx,
            tx,
            rx,
            config,

            state,

            topic_filters: args.topic_filters,
            cinp,
            timestamp: BTreeMap::default(),
        }
    }

    pub fn to_subscribed_tx(&self) -> queue::QueueTx {
        self.tx.clone()
    }

    pub fn close(mut self) -> Self {
        use std::mem;

        let (tx, rx) = queue::queue_channel(1);
        mem::drop(mem::replace(&mut self.miot_tx, tx));
        mem::drop(mem::replace(&mut self.miot_rx, rx));

        self
    }
}
