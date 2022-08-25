use crate::communication::{MessageLimit, NodeProtocol};
use crate::configuration::Cluster;
use crate::storage::Storage;
use crate::{Buffer, Entry, Serializable, Slot};
use futures::select;
use futures::{FutureExt, StreamExt};
use std::marker::PhantomData;
use thiserror::Error;

// TODO: make this configurable
const DEFAULT_MESSAGE_LIMIT: usize = 100;

#[derive(Error, Debug)]
pub enum ReplicationError<E: Entry, N: NodeProtocol, S: Storage<E>> {
    #[error("error communicating with upstream node")]
    NodeError(N::Error),
    #[error("storage error during replication")]
    StorageError(S::Error),
    #[error("unable to parse message from upstream node")]
    ParseError,
}

/// Controller for pull-based replication
pub struct Replication<E: Entry, S: Storage<E>, N: NodeProtocol, C: Cluster<Node = N::Node>> {
    self_node: C::Node,
    storage: S,
    node_protocol: N,
    config: C,
    _e: PhantomData<E>,
}

impl<E: Entry, S: Storage<E>, N: NodeProtocol, C: Cluster<Node = N::Node>> Replication<E, S, N, C> {
    pub fn new(
        self_node: C::Node,
        storage: S,
        node_protocol: N,
        config: C,
    ) -> Replication<E, S, N, C> {
        Replication {
            self_node,
            storage,
            node_protocol,
            config,
            _e: PhantomData,
        }
    }

    /// Runs the chain replication cycle for a single upstream. If the
    /// upstream is changed, the future is dropped
    async fn replication_loop(&mut self, node: &C::Node) -> Result<(), ReplicationError<E, N, S>> {
        // first we need to figure out what slot is the latest
        let mut latest_slot = self
            .storage
            .latest_slot()
            .await
            .map_err(ReplicationError::StorageError)?;

        loop {
            // fetch the messages!
            let mut msgs = self
                .node_protocol
                .fetch(node, latest_slot, MessageLimit(DEFAULT_MESSAGE_LIMIT))
                .await
                .map_err(ReplicationError::NodeError)?;
            // TODO: what type should we use as the conduit?
            let buf =
                S::Output::deserialize(&mut msgs).map_err(|_| ReplicationError::ParseError)?;
            // TODO: make sure we're appending the right slow
            latest_slot = match buf.slots().last() {
                Some(v) => Some(v),
                None => continue,
            };
            self.storage
                .append_from_buffer(buf)
                .await
                .map_err(ReplicationError::StorageError)?;
        }
    }

    /// Run the replication loop, looking for changes to the configuration and adjusting.
    pub async fn run(&mut self) {
        // lookup upstream node
        let mut config_change = self.config.change_notification().fuse();
        let mut upstream = self.config.upstream_from(&self.self_node);
        loop {
            if let Some(upstream_node) = &upstream {
                select! {
                    _ = config_change.next().fuse() => {
                        upstream = self.config.upstream_from(&self.self_node);
                    },
                    // TODO: handle error/retry cycle with backoff
                    _ = self.replication_loop(upstream_node).fuse() => {}
                }
            } else {
                config_change.next().await;
                upstream = self.config.upstream_from(&self.self_node);
            }
        }
    }
}
/*
#[cfg(test)]
mod tests {
    use executor::run;


}*/
