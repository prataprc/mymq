pub struct Shard::ActiveLoop {                      Session::Active {                                               pub enum Message {
    sessions: BTreeMap<ClientID, Session>,              topic_aliases: BTreeMap<u16, TopicName>,                        ClientAck { packet: v5::Packet },
    inp_seqno: InpSeqno,                                subscriptions: BTreeMap<TopicFilter, v5::Subscription>,         Packet {
    shard_back_log: BTreeMap<u32, Vec<Message>>,                                                                            out_seqno: OutSeqno,
    index: BTreeMap<InpSeqno, Message>,                 inp_qos12: Vec<PacketID>,                                           packet_id: Option<PacketID>,
    ack_timestamps: Vec<Timestamp>,                                                                                         publish: v5::Publish,
                                                        qos0_back_log: Vec<Message>,                                    },
    shard_queues: BTreeMap<u32, Shard>,                 out_acks: Vec<Message>,                                         Index {
    topic_filters: SubscribedTrie,                                                                                          src_client_id: ClientID,
    retained_messages: RetainedTrie,                    qos12_unacks: BTreeMap<PacketID, Message>,                          packet_id: PacketID,
}                                                       next_packet_id: PacketID,                                       },
 pub struct Shard::ReplicaLoop {                        out_seqno: OutSeqno,                                            Routed {
     sessions: BTreeMap<ClientID, Session>,             back_log: BTreeMap<OutSeqno, Message>,                              src_shard_id: u32,
 }                                                  },                                                                      client_id: ClientID,
                                                    Session::Replica {                                                      inp_seqno: InpSeqno,
                                                        out_seqno: OutSeqno,                                                out_seqno: OutSeqno,
                                                        back_log: BTreeMap<OutSeqno, Message>,                              publish: v5::Publish,
                                                    },                                                                      ack_needed: bool,
                                                    Session::Reconnect {                                                },
                                                        topic_aliases: BTreeMap<u16, TopicName>,                        LocalAck {
                                                        subscriptions: BTreeMap<TopicFilter, v5::Subscription>,             shard_id: u32,
                                                                                                                            last_acked: InpSeqno,
                                                        inp_qos12: Vec<PacketID>,                                       },
                                                                                                                    }
                                                        next_packet_id: PacketID,
                                                        out_seqno: OutSeqno,
                                                    },




























