use std::net;

use crate::{queue, v5, ClientID, Config};

pub struct SessionArgs {
    pub addr: net::SocketAddr,
    pub client_id: ClientID,
    pub miot_tx: queue::QueueTx,
    pub miot_rx: queue::QueueRx,
}

pub struct Session {
    /// Remote socket address.
    addr: net::SocketAddr,
    /// Client's ClientID that created this session.
    client_id: ClientID,
    /// Outbound channel to Miot thread.
    miot_tx: queue::QueueTx,
    /// Inbound channel from Miot thread.
    miot_rx: queue::QueueRx,
    /// A clone of this Tx channel will be added to every shard in this node. Other
    /// sessions can send messages to this session.
    tx: queue::QueueTx,
    /// Inbound channel from other sessions.
    rx: queue::QueueRx,

    /// Subscribe ack timeout. Refer [Config::subscribe_ack_timeout].
    subscribe_ack_timeout: Option<u32>,
    /// Publish ack timeout.  [Config::publish_ack_timeout].
    publish_ack_timeout: Option<u32>,
    config: Config,
}

impl Session {
    pub fn from_args(args: SessionArgs, config: Config, _pkt: v5::Connect) -> Session {
        use crate::MSG_CHANNEL_SIZE;

        let (tx, rx) = queue::queue_channel(MSG_CHANNEL_SIZE);
        Session {
            addr: args.addr,
            client_id: args.client_id,
            miot_tx: args.miot_tx,
            miot_rx: args.miot_rx,
            tx,
            rx,

            subscribe_ack_timeout: config.subscribe_ack_timeout,
            publish_ack_timeout: config.publish_ack_timeout,
            config,
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
