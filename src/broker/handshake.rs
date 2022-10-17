use log::info;

use std::net;

#[allow(unused_imports)]
use crate::broker::Cluster;

use crate::broker::thread::{Rx, Threadable};
use crate::broker::{ClusterAPI, Config};
use crate::{Protocol, ToJson};

/// Type handles incoming connection.
///
/// Complete the handshake by sending the appropriate CONNACK packet. This type is
/// threadable and spawned for every incoming connection, once the handshake is
/// completed, connection is handed over to the [Cluster].
pub struct Handshake<C>
where
    C: ClusterAPI,
{
    pub prefix: String,
    pub raddr: net::SocketAddr,
    pub config: Config,

    pub proto: Protocol,
    pub cluster: C,
    pub conn: Option<mio::net::TcpStream>,
}

impl<C> ToJson for Handshake<C>
where
    C: ClusterAPI,
{
    fn to_config_json(&self) -> String {
        format!("{{ {:?}: {:?} }}", "name", self.config.name)
    }

    fn to_stats_json(&self) -> String {
        "{{}}".to_string()
    }
}

impl<C> Threadable for Handshake<C>
where
    C: ClusterAPI,
{
    type Req = ();
    type Resp = ();

    fn main_loop(mut self, _rx: Rx<(), ()>) -> Self {
        info!("{} config:{} start handshake ...", self.prefix, self.to_config_json());
        let conn = self.conn.take().unwrap();

        if let Ok(socket) = self.proto.handshake(&self.prefix, conn) {
            self.cluster.add_connection(socket);
        }

        self
    }
}
