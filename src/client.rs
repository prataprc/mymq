use crate::v5;

//! Clients can send CONNECT, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP,
//! SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT, AUTH packets
//!
//! Clients can recieve CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP,
//! SUBACK, UNSUBACK, PINGRESP, DISCONNECT, AUTH packets
struct ClientBuilder {
    pub client_id: Option<ClientID>,
    pub addr: net::SocketAddr,

    pub clean_start: bool,
    pub will_qos: Option<v5::QoS>,
    pub will_retain: Option<v5::QoS>,
    pub will_topic: Option<TopicName>,
    pub will_properties: Option<v5::WillProperties>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,

    pub keep_alive: u16,

    version: MqttProtocol,
}
