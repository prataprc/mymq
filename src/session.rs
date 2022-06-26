use std::net;

use crate::{queue, v5, ClientID};

pub struct SessionArgs {
    pub addr: net::SocketAddr,
    pub client_id: ClientID,
    pub miot_tx: queue::QueueTx,
    pub miot_rx: queue::QueueRx,
}

pub struct Session {
    addr: net::SocketAddr,
    client_id: ClientID,
    miot_tx: queue::QueueTx,
    miot_rx: queue::QueueRx,
    tx: queue::QueueTx,
    rx: queue::QueueRx,
}

impl Session {
    pub fn from_args(args: SessionArgs, _pkt: v5::Connect) -> Session {
        use crate::MSG_CHANNEL_SIZE;

        let (tx, rx) = queue::queue_channel(MSG_CHANNEL_SIZE);
        Session {
            addr: args.addr,
            client_id: args.client_id,
            miot_tx: args.miot_tx,
            miot_rx: args.miot_rx,
            tx,
            rx,
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

    pub fn do_mqtt_packets(&self, _packets: Vec<v5::Packet>) {
        todo!()
    }
}
