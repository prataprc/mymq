use std::collections::{BTreeMap, VecDeque};
use std::net;

use crate::{queue, v5, ClientID, Config, Shard, TopicFilter};
use crate::{Error, ErrorKind, ReasonCode, Result};

// TODO A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is
// set to 0.
// TODO Revisit 2.2.1 Packet Identifier

pub struct SessionArgs {
    pub addr: net::SocketAddr,
    pub client_id: ClientID,
    pub miot_tx: queue::PktTx,
    pub session_rx: queue::PktRx,
}

pub struct Session {
    /// Remote socket address.
    addr: net::SocketAddr,
    /// Outbound channel to Miot thread.
    miot_tx: queue::PktTx,
    /// Inbound channel from Miot thread.
    session_rx: queue::PktRx,
    prefix: String,
    config: Config,

    state: SessionState,

    // inbound queue from client.
    cinp: queue::ClientInp,
    // A Periodic Message::LocalAck shall be sent to other sessions. Every incoming
    // PUBLISH from a local-session will be indexed with its cliend_id and
    // Message::Packet::seqno. Only the latest seqno shall be indexed here, and once
    // the ack is sent back, entry shall be removed from this index.
    timestamp: BTreeMap<ClientID, (u32, u64)>,
}

/// SessionState is a candidate for consensus with replicas.
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

        Session {
            addr: args.addr,
            miot_tx: args.miot_tx,
            session_rx: args.session_rx,
            prefix: format!("session:{}", args.addr),
            config,

            state,

            cinp,
            timestamp: BTreeMap::default(),
        }
    }

    // Disconnected
    pub fn route(&mut self, shard: &Shard) -> Result<()> {
        use crate::miot::rx_packets;

        match rx_packets(&self.session_rx, self.config.mqtt_msg_batch_size() as usize) {
            (pkts, _empty, true) => {
                self.route_packets(shard, pkts)?;
                err!(Disconnected, desc: "{} downstream disconnect", self.prefix)
            }
            (pkts, _empty, _disconnected) => self.route_packets(shard, pkts),
        }
    }

    fn route_packets(&self, shard: &Shard, pkts: Vec<v5::Packet>) -> Result<()> {
        for pkt in pkts.into_iter() {
            self.route_packet(shard, pkt)?
        }

        Ok(())
    }

    fn route_packet(&self, shard: &Shard, pkt: v5::Packet) -> Result<()> {
        match self.check_protocol_error(&pkt) {
            Some((resp, res)) => {
                tx_pkt_miot!(self, shard, resp)?;
                res
            }
            None => Ok(()),
        }?;

        Ok(())
    }

    fn check_protocol_error(&self, pkt: &v5::Packet) -> Option<(v5::Packet, Result<()>)> {
        match pkt {
            v5::Packet::Connect(_) => {
                let resp = v5::Packet::ConnAck(v5::ConnAck::from_reason_code(
                    v5::ConnackReasonCode::ProtocolError,
                ));
                let res = err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{} duplicate packet error",
                    self.prefix
                );
                Some((resp, res))
            }
            v5::Packet::ConnAck(_)
            | v5::Packet::SubAck(_)
            | v5::Packet::UnsubAck(_)
            | v5::Packet::PingResp => {
                let resp = v5::Packet::ConnAck(v5::ConnAck::from_reason_code(
                    v5::ConnackReasonCode::ProtocolError,
                ));
                let res = err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{} packet type {:?} not expected from client",
                    self.prefix,
                    pkt.to_packet_type()
                );
                Some((resp, res))
            }
            _ => None,
        }
    }

    pub fn close(mut self) -> Self {
        use std::mem;

        let (tx, rx) = queue::pkt_channel(1); // DUMMY channels
        mem::drop(mem::replace(&mut self.miot_tx, tx));
        mem::drop(mem::replace(&mut self.session_rx, rx));

        self
    }
}
