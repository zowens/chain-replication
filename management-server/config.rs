use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct Config {
    pub server_addr: SocketAddr,
}
