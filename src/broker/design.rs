//! ### Design overview
//!
//! ```ignore
//!
//!   *---------*  Handle   *----------*        *-----------*
//!   | Cluster |---------->| Listener |        | Handshake |
//!   *---------*           *----------*        *-----------*
//!     ^  |  ^                    |                   |
//!     |  |  |                    |Tx                 |Tx
//!     |  |  +--------------------+-------------------+
//!     |  |
//!     |  |     Handle     *--------* Tx
//!     |  +--------------->| Ticker |---------+
//!     |  |                *--------*         |
//!     |  |                                   |
//!     |  |     Handle     *---------*        |
//!     |  +--------------->| Flusher |<---+   |
//!     |  |                *---------*    |   |
//!     |  |                               |   |
//!     |  |     Handle     *-------*  Tx  |   |
//!     |  +--------------->|       |------+   |
//!     |                   | Shard |<---------+    *------*
//!     |                Tx |       |  Handle       |      |------->|
//!     +-------------------|       |-------------->| Miot |        | Socket
//!                 +------>|       |<--------------|      |<-------|
//!                 |       |       |            Tx |      |
//!                 |       |       | PktTx         |      |
//!                 |       |       |-------------->|      |
//!                 |       |       |<--------------|      |
//!                 |       *-------*         PktTx *------*
//!                 |           ^
//!                 |   MsgTx   |
//!                 +-----------+
//!
//! ```
//!
//! * A `Cluster` shall be made up of nodes, capable of hosting one or more shards.
//! * Number of shards are fixed when creating a cluster, and cannot change there after.
//! * Number of shards must be power of 2.
//! * Cluster, Node are uniquely identified by uuid. While, shard is uniquely identified
//!   by its `shard_id`. For N shards, their `shard_id` shall range from 0..N
//! * MQTT is a state-ful protocol, hence on the broker side, there shall be one `Session`
//!   for each client, identified by ClientID.
//! * Given the ClientID and number of shards, mapping of `Session` to `Shard` is
//!   computed with ZERO knowledge.
//! * There should atleast be as many shards as the CPU cores in a node.
//! * There shall be `one-master` and `zero-or-more-replicas` for each shard.
//!
//! **Threading model**
//!
//! * Any type implementing `Threadable` can be spawned as a thread.
//! * At present, following types implement `Threadable`.
//!   * `Cluster`, that manages all the other threads.
//!   * `Listener`, MQTT listener, binds to MQTT port and accepts new connection
//!   * `Handshake`, takes care of converting a connection into session.
//!   * `Flush`, shutsdown a connection
//!   * `Ticker`, periodic timer that will wake other threads, like Shard.
//!   * `Shard`, is the work-horse of MQTT protocol, manages one or more Session.
//!   * `Miot`, takes care of serializing and de-serializing MQTT packets and
//!     write/read from MQTT sockets.
//! * Other than Shard and Miot thread, all the other types are singleton, that is,
//!   there is only one instance of it.
//! * There can be one or more Shard instances. It is the scope of the rebalancer to
//!   decide which node shall host the shards.
//! * For each Shard instance there shall be a Miot instance paired together.
//! * Except `Handshake` thread, all the other threads are spawned during the system-boot
//!   and remains active until `close_wait` is called.
//! * `Handshake` thread is spawned for every new connection, once handshake with the
//!   client has completed, it is handed over to the Shard.
//!
//! **Thread-state**
//!
//! * When a thread-type is instantiated, it is the `Init` state.
//! * Subsequently, spawning the thread will create two variants of the type.
//!   * A variant that shall be in the `Main` state and executes the `main` loop
//!     for the thread.
//!   * A variant that shall be in the `Handle` state and shall be owned by the
//!     parent-thread, `Handle` can be used to communicate with the `Main`.
//! * When `close_wait` is called on the thread, this call can be made only by the
//!   parent-thread holding the `Handle` variant, and calling this will fold both
//!   `Handle` variant and `Main` variant into `Close` variant, which again will be held
//!   by the parent-thread.
//! * Other than this, there can be one or more  `Tx` Or `MsgTx` variants. These variants
//!   are owned by other threads, to communicate with `Main`.
//!
//! **Queue**
//!
//! There are three types of queues.
//!
//! * `control-queue`, un-bounded mpsc-channel. The `Rx` of the channel is held by the
//!   `Main` loop of the thread.
//! * `packet-queue`, bounded mpsc-channel. There will a pair of these queues, one for
//!   for sending packets and another for receiving packets, welded between each
//!   `Shard` and its `Miot` pair. So for N shards there will be `2*N` packet-queues.
//! * `message-queue`, bounded mpsc-channel. There will one queue welded to each Shard.
//!   Other shards/sessions can route messages via this queue. So for N shards there
//!   shall be N message-queues.
//!
//! Note that `Packet` are plain-vanilla MQTT packets. `Message` are wrapper around
//! Packets and other meta-data that gets exchanged between shards/sessions.
//!
//! **Topology**
//!
//! Topology is distribution of shard's master and replicas across the cluster. For each
//! shard, there shall be a [Topology] discription. After every rebalance, whether
//! gracefull or fail-over, a new Topology shall be created that involves minimum
//! shard migration.
//!
//! **Rebalancer**
//!
//! * Shard to node mapping is open-ended, Rebalancer can experiment with several
//!   algorithms, but the end result shall be that:
//!   * Shards, both master and its replicas, shall be distributed evenly across nodes.
//!   * All shards hosted by the same node, shall have same master/replica topology.
//! * Assign session to shard, computed only using ClientID and num_shards.
//! * Assign a node for master shard.
//! * Assign ZERO or more node for replica shards.
//!
//! **Shard-Migration**
//!
//! * Migration of master shard from one node to another.
//! * Migration of replica shard from one node not another.
//! * Demotion of master shard as replica-shard.
//! * Promotion of replica-shard as master-shard.
