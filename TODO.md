* Contains both "broker" and "client" code.
* Provide feature-gating to compile this source base for
  * Both "broker" and "client"
  * Only "client", this will reduce the library foot-print and make for fast-compilation.
* Binary-mqttd, cleanup and document in lib.rs
* Cleanup src/broker/chash.rs,src/broker/crc.rs
* Create a script to calculate the bare-minimum `rustc` version required.
* Create a script to list unstable features required by this crate and depended crates.
* While mqttd boots,
  * Log version and SHA1.
  * Log system resources like CPU, Memory (Total, Free), Net-Interfaces,
    Disks (Count, Total, Free).
* Review .ok() .unwrap() `allow_panic!()`, panic!() and unreachable!() calls.
* Review assert macro calls.
* Review `as` type-casting for numbers.
* Review code for `#[allow(dead_code)]`.
* Validate and document all thread handles, cluster, listener, flusher, shard, miot.
* Handle retain-messages in Will, Publish, Subscribe scenarios, `retain_available`.
