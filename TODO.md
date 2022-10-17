* Binary-mymqd, cleanup and document in lib.rs
* Cleanup src/broker/chash.rs,src/broker/crc.rs
* Create a script to calculate the bare-minimum `rustc` version required.
* Create a script to list unstable features required by this crate and depended crates.
* While mymqd boots,
  * Log version and SHA1.
  * Log system resources like CPU, Memory (Total, Free), Net-Interfaces,
    Disks (Count, Total, Free).
* Review .ok() .unwrap() `app_fatal!()`, panic!() and unreachable!() calls.
* Review assert macro calls.
* Review `as` type-casting for numbers.
* Review code for `#[allow(dead_code)]`.
* Validate and document all thread handles, cluster, listener, flusher, shard, miot.
* Handle retain-messages in Will, Publish, Subscribe scenarios, `retain_available`.
* Take the list of DisconnReasonCode on the server side and figure out places where
  this error can happen.
* Section 4.7.3: The topic resource MAY be either predefined in the Server by an
  administrator or it MAY be dynamically created by the Server when it receives the
  first subscription or an Application Message with that Topic Name. The Server MAY also
  use a security component to authorize particular actions on the topic resource for a
  given Client.
* Create a Cheatsheet for MQTT protocol.

### Retain message

* When going multi-node, retain messages for topics must be distributed between Cluster
  instances in participating nodes. In other words, `set_retain_topic` and
  `reset_retain_topic` API must be part of cluster consensus loop.

### Will message

* Will QoS, Retain
* Will topic, payload, properties
* Will Delay Interval
* Will properties
  * Payload Format Indicator
  * Message Expiry Interval
  * Content Type
  * Response Topic
  * Correlation Data
  * User Property
* ReasonCode, Disconnect with Will Message

### Security

* Basic authentication using username and password
* Enhanced authentication
* Re-authentication
* Section 4 and 5 covers some details.

### Test cases

* If both Client and Server set Receive Maximum to 1, they make sure that no more than
  one message is “in-flight” at any one time. In this case no QoS 1 message will be
  received after any later one even on re-connection. For example a subscriber might
  receive them in the order 1,2,3,3,4 but not 1,2,3,2,3,4. Refer to section 4.9 Flow
  Control for details of how the Receive Maximum is used.
