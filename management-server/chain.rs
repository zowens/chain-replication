use protobuf::well_known_types::Duration;
use protocol::{
    ClientConfiguration, JoinRequest, NodeConfiguration, NodeRole, NodeStatus, PollRequest,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

const POLL_DURATION_SECONDS: i64 = 3;

#[derive(Clone, Debug)]
struct NodeState {
    id: u64,
    replication_address: String,
    client_address: String,
}

#[derive(PartialEq, Debug)]
pub enum PollError {
    NoNodeFound,
}

#[derive(Clone)]
pub struct Chain {
    servers: Arc<RwLock<Vec<NodeState>>>,
    id_gen: Arc<AtomicUsize>,
}

fn map_node_configuration(
    state: &NodeState,
    upstream: Option<&NodeState>,
    downstream: Option<&NodeState>,
) -> NodeConfiguration {
    let mut config = NodeConfiguration::new();

    match upstream {
        Some(srv_state) => {
            let mut node = config.mut_upstream_node();
            node.set_id(srv_state.id);
            node.set_replication_address(srv_state.replication_address.clone());
        }
        None => {
            config.clear_upstream_node();
        }
    }

    match (upstream.is_some(), downstream.is_some()) {
        (true, true) => {
            config.set_role(NodeRole::INNER);
            config.set_node_status(NodeStatus::ACTIVE);
        }
        (true, false) => {
            config.set_role(NodeRole::TAIL);
            config.set_node_status(NodeStatus::ACTIVE);
        }
        (false, true) => {
            config.set_role(NodeRole::HEAD);
            config.set_node_status(NodeStatus::ACTIVE);
        }
        (false, false) => {
            config.set_role(NodeRole::HEAD);
            config.set_node_status(NodeStatus::WAITING);
        }
    }

    {
        let node = config.mut_node();
        node.set_id(state.id);
        node.set_replication_address(state.replication_address.clone());
    }

    let mut poll_duration = Duration::new();
    poll_duration.set_seconds(POLL_DURATION_SECONDS);
    config.set_poll_wait(poll_duration);

    config
}

impl Chain {
    pub fn new() -> Chain {
        Chain {
            servers: Arc::new(RwLock::new(Vec::new())),
            id_gen: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn join(&self, req: JoinRequest) -> NodeConfiguration {
        let id = self.id_gen.fetch_add(1, Ordering::SeqCst) as u64;
        let mut servers = self.servers.write().unwrap();
        let server = NodeState {
            replication_address: req.replication_address.clone(),
            client_address: req.client_address,
            id,
        };
        let config = {
            let upstream_server = servers.last();
            map_node_configuration(&server, upstream_server, None)
        };
        info!("Server joined {:?}", server);
        servers.push(server);
        config
    }

    pub fn poll(&self, req: PollRequest) -> Result<NodeConfiguration, PollError> {
        let servers = self.servers.read().unwrap();
        debug!("Server poll {}", req.get_node_id());

        let id = req.get_node_id();
        let server_index = servers
            .iter()
            .enumerate()
            .find_map(|(index, state)| if state.id == id { Some(index) } else { None })
            .ok_or_else(|| PollError::NoNodeFound)?;

        let upstream_server = if server_index == 0 {
            None
        } else {
            servers.get(server_index - 1)
        };

        let downstream_server = servers.get(server_index + 1);

        Ok(map_node_configuration(
            &servers[server_index],
            upstream_server,
            downstream_server,
        ))
    }

    pub fn snapshot(&self) -> ClientConfiguration {
        trace!("Snapshot called");
        let servers = self.servers.read().unwrap();
        let head = servers.first();
        let tail = servers.last();

        let mut config = ClientConfiguration::new();

        match (head, tail) {
            (Some(head), Some(tail)) if head.id != tail.id => {
                // TODO: do we want this constraint? at least 2 nodes?
                {
                    let mut hd = config.mut_head_node();
                    hd.set_id(head.id);
                    hd.set_client_address(head.client_address.clone());
                }

                {
                    let mut tl = config.mut_tail_node();
                    tl.set_id(tail.id);
                    tl.set_client_address(tail.client_address.clone());
                }
            }
            _ => {}
        };
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join() {
        let chain = Chain::new();

        // join first node
        let config = chain.join(join_req("a"));
        assert_eq!(NodeRole::HEAD, config.get_role());
        assert_eq!(NodeStatus::WAITING, config.get_node_status());
        assert!(!config.has_upstream_node());
        assert!(config.has_node());
        assert!(config.has_poll_wait());
        assert_eq!(0, config.get_node().get_id());
        assert_eq!("a", config.get_node().get_replication_address());

        // join second node
        let config = chain.join(join_req("b"));
        assert_eq!(NodeRole::TAIL, config.get_role());
        assert_eq!(NodeStatus::ACTIVE, config.get_node_status());
        assert!(config.has_upstream_node());
        assert_eq!("a", config.get_upstream_node().get_replication_address());
        assert_eq!(0, config.get_upstream_node().get_id());
        assert!(config.has_node());
        assert!(config.has_poll_wait());
        assert_eq!(1, config.get_node().get_id());
        assert_eq!("b", config.get_node().get_replication_address());
    }

    #[test]
    fn test_poll_unknown_node() {
        let chain = Chain::new();
        assert_eq!(
            Some(PollError::NoNodeFound),
            chain.poll(poll_req(123)).err()
        );
    }

    #[test]
    fn test_poll_nodes() {
        let chain = Chain::new();
        // join 3 nodes
        let a = chain.join(join_req("a"));
        assert_eq!(0, a.get_node().get_id());
        let b = chain.join(join_req("b"));
        assert_eq!(1, b.get_node().get_id());
        let c = chain.join(join_req("c"));
        assert_eq!(2, c.get_node().get_id());

        {
            let c_poll = chain.poll(poll_req(2)).unwrap();
            assert_eq!(NodeRole::TAIL, c_poll.get_role());
            assert_eq!(NodeStatus::ACTIVE, c_poll.get_node_status());
            assert!(c_poll.has_upstream_node());
            assert_eq!("b", c_poll.get_upstream_node().get_replication_address());
            assert_eq!(1, c_poll.get_upstream_node().get_id());
            assert!(c_poll.has_poll_wait());
            assert!(c_poll.has_node());
            assert_eq!(2, c_poll.get_node().get_id());
            assert_eq!("c", c_poll.get_node().get_replication_address());
        }

        {
            let b_poll = chain.poll(poll_req(1)).unwrap();
            assert_eq!(NodeRole::INNER, b_poll.get_role());
            assert_eq!(NodeStatus::ACTIVE, b_poll.get_node_status());
            assert!(b_poll.has_upstream_node());
            assert_eq!("a", b_poll.get_upstream_node().get_replication_address());
            assert_eq!(0, b_poll.get_upstream_node().get_id());
            assert!(b_poll.has_poll_wait());
            assert!(b_poll.has_node());
            assert_eq!(1, b_poll.get_node().get_id());
            assert_eq!("b", b_poll.get_node().get_replication_address());
        }

        {
            let a_poll = chain.poll(poll_req(0)).unwrap();
            assert_eq!(NodeRole::HEAD, a_poll.get_role());
            assert_eq!(NodeStatus::ACTIVE, a_poll.get_node_status());
            assert!(!a_poll.has_upstream_node());
            assert!(a_poll.has_poll_wait());
            assert!(a_poll.has_node());
            assert_eq!(0, a_poll.get_node().get_id());
            assert_eq!("a", a_poll.get_node().get_replication_address());
        }
    }

    #[test]
    fn test_snapshot() {
        let chain = Chain::new();

        // no head/tail
        let snapshot = chain.snapshot();
        assert!(!snapshot.has_head_node());
        assert!(!snapshot.has_tail_node());

        chain.join(join_req("a"));

        // still not head/tail
        let snapshot = chain.snapshot();
        assert!(!snapshot.has_head_node());
        assert!(!snapshot.has_tail_node());

        chain.join(join_req("b"));
        chain.join(join_req("c"));

        // configuration = a -> b -> c
        let snapshot = chain.snapshot();
        assert!(snapshot.has_head_node());
        assert_eq!(0, snapshot.get_head_node().get_id());
        assert_eq!("a", snapshot.get_head_node().get_client_address());
        assert!(snapshot.has_tail_node());
        assert_eq!(2, snapshot.get_tail_node().get_id());
        assert_eq!("c", snapshot.get_tail_node().get_client_address());
    }

    fn join_req(id: &str) -> JoinRequest {
        let mut req = JoinRequest::new();
        req.set_replication_address(id.to_string());
        req.set_client_address(id.to_string());
        req
    }

    fn poll_req(id: u64) -> PollRequest {
        let mut req = PollRequest::new();
        req.set_node_id(id);
        req
    }
}
