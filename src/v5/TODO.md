### System

* One or remote nodes shall continuously connect and disconnect with broker. We can
  increase the concurrency of this operation from 1, 2, 4, 8, 16, 32, 64, 128

### Sesson-level

* `FixedHeader.remaining_len` must be validated with
   `ConnectProperties.maximum_pkt_size`.
* Test case to detect fresh packet-identifier.
* first socket read for fixed-header can wait indefinitely, but the next read
  for `remaining_len` must timeout within a stipulated period.
* If a Server receives a CONNECT packet containing a Will Message with the Will
* Retain set to 1, and it does not support retained messages, the Server MUST
  reject the connection request. It SHOULD send CONNACK with Reason Code
  0x9A (Retain not supported) and then it MUST close the Network Connection.
* A Client receiving Retain Available set to 0 from the Server MUST NOT send a
  PUBLISH packet with the RETAIN flag set to 1. If the Server receives such a
  packet, this is a Protocol Error. The Server SHOULD send a DISCONNECT with
  Reason Code of 0x9A (Retain not supported) as described in section 4.13.
* The Client MUST NOT send packets exceeding Maximum Packet Size to the Server.
  If a Server receives a packet whose size exceeds this limit, this is a Protocol
  Error, the Server uses DISCONNECT with Reason Code 0x95 (Packet too large), as
  described in section 4.13.
* If the Server receives a SUBSCRIBE packet containing a Wildcard Subscription
  and it does not support Wildcard Subscriptions, this is a Protocol Error. The
  Server uses DISCONNECT with Reason Code 0xA2 (Wildcard Subscriptions not
  supported) as described in section 4.13.
* If the Server receives a SUBSCRIBE packet containing Subscription Identifier
  and it does not support Subscription Identifiers, this is a Protocol Error.
  The Server uses DISCONNECT with Reason Code of 0xA1 (Subscription Identifiers
  not supported) as described in section 4.13.
* If the Server receives a SUBSCRIBE packet containing Shared Subscriptions and
  it does not support Shared Subscriptions, this is a Protocol Error. The Server
  uses DISCONNECT with Reason Code 0x9E (Shared Subscriptions not supported) as
  described in section 4.13.
* If the Client sends a Request Response Information with a value 1, it is
  OPTIONAL for the Server to send the Response Information in the CONNACK.
* The Server uses a Server Reference in either a CONNACK or DISCONNECT packet
  with Reason code of 0x9C (Use another server) or Reason Code 0x9D (Server moved)
  as described in section 4.13.
* If a Server sends a CONNACK packet containing a Reason code of 128 or greater
  it MUST then close the Network Connection

#### Connect

* After a network connection is established CONNECT must be the first packet.
* The Server MUST process a second CONNECT packet sent from a Client as a
  Protocol Error and close the Network Connection
* Payload with missing client identifier.
* Test case for server unavailable.
* Test case for server busy.
* The Server MUST validate that the reserved flag in the CONNECT packet is set to 0
* The Server MAY validate that the Will Message is of the format indicated,
  and if it is not send a CONNACK with the Reason Code of
  0x99 (Payload format invalid) as described in section 4.13
* The Server MUST NOT send packets exceeding Maximum Packet Size in CONNECT msg.
  * Where a Packet is too large to send, the Server MUST discard it without sending
    it and then behave as if it had completed sending that Application Message.
  * In the case of a Shared Subscription where the message is too large to send
    to one or more of the Clients but other Clients can receive it, the Server
    can choose either discard the message without sending the message to any of
    the Clients, or to send the message to one of the Clients that can receive it.
* Protocol confirmance for Request Response Information.
* Protocol confirmance for Request Problem Information.
* Extended authentication Section 4.12
* If a Client sets an Authentication Method in the CONNECT, the Client MUST NOT
  send any packets other than AUTH or DISCONNECT packets until it has received
  a CONNACK packet

### Publish

* The DUP flag MUST be set to 0 for all QoS 0 messages
* Test case to detect validity of duplicate message, that there should be
  no duplicate message for QoS0. Duplicate message for QoS1 is allowed but not
  after sender has received PUBACK. Duplicate message of QoS2 is allowed but not
  after PUBREL.
* The receiver of an MQTT Control Packet that contains the DUP flag set to 1
  cannot assume that it has seen an earlier copy of this packet.
* A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set
  to 0.
* If the Server included a Maximum QoS in its CONNACK response to a Client and
  it receives a PUBLISH packet with a QoS greater than this, then it uses
  DISCONNECT with Reason Code 0x9B (QoS not supported).
* If the Payload contains zero bytes it is processed normally by the Server but
  any retained message with the same topic name MUST be removed and any future
  subscribers for the topic will not receive a retained message.
* A retained message with a Payload containing zero bytes MUST NOT be stored
  as a retained message on the Server.
* If the Server included Retain Available in its CONNACK response to a Client
  with its value set to 0 and it receives a PUBLISH packet with the RETAIN flag
  is set to 1, then it uses the DISCONNECT Reason Code of 0x9A (Retain not
  supported).
* what should be RETAIN flag when broker forwards a PUBLISH message.
* what should be RETAIN flag when broker forwards a retain message for matching
  new subscriptions.
* The Topic Name in a PUBLISH packet sent by a Server to a subscribing Client
  MUST match the Subscription’s Topic Filter according to the matching process
  defined in section 4.7 [MQTT-3.3.2-3]. However, as the Server is permitted
  to map the Topic Name to another name, it might not be the same as the
  Topic Name in the original PUBLISH packet.
* It is a Protocol Error if the Topic Name is zero length and there is no
  Topic Alias.
* Test cases to include packet-identifier for QoS0 messages.
* If the Message Expiry Interval has passed and the Server has not managed to
  start onward delivery to a matching subscriber, then it MUST delete the copy
  of the message for that subscriber
