> How IoT devices - say for e.g. sensors or managed ?

IoT devices are expected to be embedded devices, though not restricted to that. They typically run an RTOS, and have a network stack to communicate with internet.
It is also possible that micro sensors may be driven by simple firmware and communicate through CAN/Serial-bus/Infrared/Bluetooth to a more powerful device in close proximity, which in turn should consolidate and relay the message over the internet to remote brokers.

> May be it is one time installation and then they authenticate with a centralized entity using some protocol/medium - will they run forever like that?
> How security is resolved in IoT?

Between broker and remote devices the security can be handled at the following points.

(a) Using pre assigned IP address, that is server's address and clients address will be configured on the device side and broker side respectively.
(b) SSL security, should give socket level security between communicating parties.
(c) Typically we can use assymetic security keys (like public/private key) during initial connection establishment and later switch to temporarily generated symmetric key which is easy on computation.
(d) Every device _must_ be identified based on unique ClientID. ClientID shall be generated and configured at the device manufactuing time.

On top of all this MQTT provides AUTH negotiation protocol, which we can use for additional authentication and also for authorization purposes. The specification is open-ended so we can
use our imagination.

> Can each communication/packet from an IoT device be authenticated and authorized?

Yes using asymmetic or symmetric encryption, which can happen at socket level or at application-message level.

> May be an inbuilding setup will have a constant source of power and the IoT devices are not constrained?

Yes that is true. When a target market is going to impose too many variables in a system, then the market will eventually fragment. This will open up oppurtunities for small players. 

> Still it is like an litter of inventory to maintain with health check. Closest that comes to my mind is Aircraft and it's sensors. Its goes through rigorous process and diligent maintenance before every flight.
> What is the process followed in IoT to ensure faultless operation - for e.g. automate access control to a building or lift operation?

IoT seem to be very broadbased. Right now I am focusing on the broker protocol. I am sure all this can be handled by the application. My guess is, eventually we will end up with hundreds of applications, if not thousands, to deal with developement/deployment/conguration/running/maintanence/analysis of IoT systems.

