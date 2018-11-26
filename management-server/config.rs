use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct Config {
    pub server_addr: SocketAddr,

    #[serde(default)]
    pub failure_detection: FailureDetection,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct FailureDetection {
    /// Number of seconds to ask the node to repoll.
    pub poll_duration_seconds: u64,

    /// Number of seconds over poll_duration_seconds in which
    /// a connection is considered failed.
    pub failed_poll_duration_seconds: u64,

    /// Maximum number of nodes that are permitted to be considered
    /// failing at once. This prevents the management server from
    /// thrashing on the cluster membership. Value must be in the range `[0, 100)`
    pub percent_maximum_failed_nodes: u32,
}

impl Default for FailureDetection {
    fn default() -> FailureDetection {
        FailureDetection {
            poll_duration_seconds: 3,
            failed_poll_duration_seconds: 3,
            percent_maximum_failed_nodes: 50,
        }
    }
}
