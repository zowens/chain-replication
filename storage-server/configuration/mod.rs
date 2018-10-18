use config::Config;
use futures::{Async, Future, Poll};
use grpcio;
use grpcio::{ChannelBuilder, EnvBuilder};
use protocol;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

pub use protocol::NodeStatus;
const DEFAULT_WAIT_DURATION: Duration = Duration::from_secs(5);

pub struct NodeConfiguration<'a> {
    config: RwLockReadGuard<'a, protocol::NodeConfiguration>,
}

impl<'a> NodeConfiguration<'a> {
    pub fn is_head(&self) -> bool {
        self.config.role == protocol::NodeRole::HEAD
    }

    pub fn is_tail(&self) -> bool {
        self.config.role == protocol::NodeRole::TAIL
    }

    pub fn upstream_addr(&self) -> Option<SocketAddr> {
        self.config
            .upstream_node
            .as_ref()
            .and_then(|n| n.replication_address.as_str().to_socket_addrs().ok())
            .and_then(|sock_addrs| sock_addrs.into_iter().next())
    }

    pub fn status(&self) -> NodeStatus {
        self.config.get_node_status()
    }

    pub fn wait_duration(&self) -> Duration {
        if self.config.has_poll_wait() {
            let poll = self.config.get_poll_wait();
            Duration::new(poll.seconds as u64, poll.nanos as u32)
        } else {
            DEFAULT_WAIT_DURATION
        }
    }
}

#[derive(Clone)]
pub struct NodeManager {
    current_config: Arc<RwLock<protocol::NodeConfiguration>>,
    client: protocol::ConfigurationClient,
}

fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<NodeManager>();
}

impl NodeManager {
    fn new(
        config: protocol::NodeConfiguration,
        client: protocol::ConfigurationClient,
    ) -> NodeManager {
        NodeManager {
            current_config: Arc::new(RwLock::new(config)),
            client,
        }
    }

    pub fn current(&self) -> NodeConfiguration {
        NodeConfiguration {
            config: self.current_config.read().unwrap(),
        }
    }

    pub fn repoll(&self) -> NodeConfigFuture {
        let node_id = {
            let config = self.current_config.read().unwrap();
            config.get_node().get_id()
        };
        let mut poll_req = protocol::PollRequest::new();
        poll_req.set_node_id(node_id);

        NodeConfigFuture {
            current_config: self.current_config.clone(),
            future: self.client.poll_async(&poll_req).unwrap(),
        }
    }
}

pub struct NodeConfigFuture {
    current_config: Arc<RwLock<protocol::NodeConfiguration>>,
    future: grpcio::ClientUnaryReceiver<protocol::NodeConfiguration>,
}

impl Future for NodeConfigFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        match self.future.poll() {
            Ok(Async::Ready(config)) => {
                let mut v = self.current_config.write().unwrap();
                *v = config;
                Ok(Async::Ready(()))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                error!("Error polling for configuration: {}", e);
                Err(())
            }
        }
    }
}

pub struct ClusterJoin {
    future: grpcio::ClientUnaryReceiver<protocol::NodeConfiguration>,
    client: protocol::ConfigurationClient,
}

impl ClusterJoin {
    pub fn new(config: &Config) -> ClusterJoin {
        let cb = ChannelBuilder::new(Arc::new(EnvBuilder::new().build()))
            .default_compression_algorithm(grpcio::CompressionAlgorithms::Gzip)
            .max_concurrent_stream(100)
            .http2_bdp_probe(true);
        let conn = cb.connect(&config.management.management_server_addr);
        let client = protocol::ConfigurationClient::new(conn);

        let mut join = protocol::JoinRequest::new();
        join.set_replication_address(config.replication.server_addr.to_string());
        join.set_client_address(config.frontend.server_addr.to_string());
        let join_req = client.join_async(&join).unwrap();
        ClusterJoin {
            future: join_req,
            client,
        }
    }
}

impl Future for ClusterJoin {
    type Item = NodeManager;
    type Error = ();

    fn poll(&mut self) -> Poll<NodeManager, ()> {
        match self.future.poll() {
            Ok(Async::Ready(config)) => {
                info!(
                    "Joined cluster. id={}, role={:?}, status={:?}, upstream_node={:?}",
                    config.get_node().get_id(),
                    config.get_role(),
                    config.get_node_status(),
                    config
                        .upstream_node
                        .as_ref()
                        .map(|v| v.replication_address.as_str())
                );
                Ok(Async::Ready(NodeManager::new(config, self.client.clone())))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                error!("Error joinging cluster: {:?}", e);
                Err(())
            }
        }
    }
}
