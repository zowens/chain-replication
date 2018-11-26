use config::Config;
use futures::{Async, Future, Poll};
use grpcio;
use grpcio::{ChannelBuilder, EnvBuilder, Error as GrpcError, RpcStatus, RpcStatusCode};
use protocol;
use rand::{thread_rng, Rng};
use retry::{Retry, RetryBehavior, RetryPoll};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

const DEFAULT_WAIT_DURATION: Duration = Duration::from_secs(5);

pub struct NodeConfiguration<'a> {
    config: RwLockReadGuard<'a, protocol::NodeConfiguration>,
}

impl<'a> NodeConfiguration<'a> {
    pub fn is_head(&self) -> bool {
        assert!(self.config.quorum > 0);
        assert!(self.config.has_self_node());

        let id = self.config.get_self_node().id;
        self.config.active_chain.len() >= self.config.quorum as usize
            && id == self.config.active_chain.iter().next().unwrap().id
    }

    pub fn is_tail(&self) -> bool {
        assert!(self.config.quorum > 0);
        assert!(self.config.has_self_node());

        let id = self.config.get_self_node().id;
        self.config.active_chain.len() >= self.config.quorum as usize
            && id == self.config.active_chain.iter().last().unwrap().id
    }

    pub fn upstream_addr(&self) -> Option<SocketAddr> {
        assert!(self.config.has_self_node());
        let id = self.config.get_self_node().id;
        let index = self
            .config
            .active_chain
            .iter()
            .enumerate()
            .filter_map(|(ind, n)| if n.id == id { Some(ind) } else { None })
            .next();

        match index {
            Some(0) => None,
            None => None,
            Some(i) => {
                assert!(i > 0);
                let upstream_node = &self.config.active_chain[i - 1];
                upstream_node
                    .replication_address
                    .as_str()
                    .to_socket_addrs()
                    .ok()
                    .and_then(|sock_addrs| sock_addrs.into_iter().next())
            }
        }
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
            config.get_self_node().get_id()
        };
        let mut poll_req = protocol::PollRequest::new();
        poll_req.set_node_id(node_id);
        let poll_fut = self.client.poll_async(&poll_req).unwrap();
        NodeConfigFuture {
            current_config: self.current_config.clone(),
            client: self.client.clone(),
            request: poll_req,
            future: RetryBehavior::forever().retry(poll_fut),
        }
    }
}

pub struct NodeConfigFuture {
    client: protocol::ConfigurationClient,
    request: protocol::PollRequest,
    current_config: Arc<RwLock<protocol::NodeConfiguration>>,
    future: Retry<grpcio::ClientUnaryReceiver<protocol::NodeConfiguration>>,
}

impl Future for NodeConfigFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            match self.future.poll_next() {
                RetryPoll::FinalResult(config) => {
                    let mut v = self.current_config.write().unwrap();
                    *v = config;
                    return Ok(Async::Ready(()));
                }
                RetryPoll::AsyncNotReady => return Ok(Async::NotReady),
                RetryPoll::Retry(e) => {
                    error!("Error polling for configuration, retrying: {}", e);
                    let fut = self.client.poll_async(&self.request).unwrap();
                    self.future.set_future(fut);
                }
                RetryPoll::FinalError(e) => {
                    error!("Error polling for configuration: {}", e);
                    return Err(());
                }
            }
        }
    }
}

pub struct ClusterJoin {
    join_req: protocol::JoinRequest,
    future: Retry<grpcio::ClientUnaryReceiver<protocol::NodeConfiguration>>,
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

        let mut join_req = protocol::JoinRequest::new();
        join_req.set_replication_address(config.replication.server_addr.to_string());
        join_req.set_client_address(config.frontend.server_addr.to_string());
        join_req.set_node_id(thread_rng().gen());
        let join_fut = client.join_async(&join_req).unwrap();
        ClusterJoin {
            join_req,
            future: RetryBehavior::forever()
                .max_backoff(Duration::from_secs(4))
                .initial_backoff(Duration::from_millis(500))
                .disable_jitter()
                .retry(join_fut),
            client,
        }
    }
}

impl Future for ClusterJoin {
    type Item = NodeManager;
    type Error = ();

    fn poll(&mut self) -> Poll<NodeManager, ()> {
        loop {
            match self.future.poll_next() {
                RetryPoll::FinalResult(config) => {
                    info!("Joined cluster. id={}", config.get_self_node().get_id());
                    return Ok(Async::Ready(NodeManager::new(config, self.client.clone())));
                }
                RetryPoll::FinalError(e) => {
                    error!("Error joining cluster {}", e);
                    return Err(());
                }
                RetryPoll::AsyncNotReady => return Ok(Async::NotReady),
                RetryPoll::Retry(GrpcError::RpcFailure(RpcStatus {
                    status: RpcStatusCode::AlreadyExists,
                    ..
                })) => {
                    error!("Duplicate node ID, trying with another");
                    self.join_req.set_node_id(thread_rng().gen());
                    let fut = self.client.join_async(&self.join_req).unwrap();
                    self.future.set_future(fut);
                }
                RetryPoll::Retry(e) => {
                    warn!("Error joining cluster, retrying: {}", e);
                    let fut = self.client.join_async(&self.join_req).unwrap();
                    self.future.set_future(fut);
                }
            }
        }
    }
}
