use crate::{
    communication::{MessageLimit, NodeProtocol},
    configuration::Cluster,
    storage::Storage,
    Buffer, Entry, Serializable,
};
use futures::{
    future::{select, Either},
    pin_mut, select, FutureExt, StreamExt, TryFutureExt,
};
use std::marker::PhantomData;
use thiserror::Error;

// TODO: make this configurable
const DEFAULT_MESSAGE_LIMIT: usize = 100;

#[derive(Error, Debug)]
pub enum ReplicationError<N, S> {
    #[error("error communicating with upstream node")]
    NodeError(N),
    #[error("storage error during replication")]
    StorageError(S),
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
        Replication { self_node, storage, node_protocol, config, _e: PhantomData }
    }

    /// Runs the chain replication cycle for a single upstream. If the
    /// upstream is changed, the future is dropped
    async fn replication_loop(
        &mut self,
        upstream_node: &C::Node,
    ) -> Result<(), ReplicationError<N::Error, S::Error>> {
        // first we need to figure out what slot is the latest
        let mut request_slot = self
            .storage
            .latest_slot()
            .await
            .map(|v| v.map(|v| v + 1))
            .map_err(ReplicationError::StorageError)?;

        // run the first round of fetch, then fetches happen concurrently after
        let mut replication_msgs = self
            .node_protocol
            .fetch(upstream_node, request_slot, MessageLimit(DEFAULT_MESSAGE_LIMIT))
            .map_err(ReplicationError::NodeError)
            .await?;

        loop {
            let buf = S::Buffer::deserialize(&mut replication_msgs)
                .map_err(|_| ReplicationError::ParseError)?;

            // TODO: make sure we're appending the right slot
            request_slot = match buf.slots().last() {
                Some(v) => Some(v + 1),
                None => continue,
            };

            // kick off another fetch while we write
            let fetch = self
                .node_protocol
                .fetch(upstream_node, request_slot, MessageLimit(DEFAULT_MESSAGE_LIMIT))
                .map_err(ReplicationError::NodeError);

            let store =
                self.storage.append_from_buffer(buf).map_err(ReplicationError::StorageError);
            pin_mut!(fetch);
            pin_mut!(store);
            match select(fetch, store).await {
                Either::Left((replication_res, store_fut)) => {
                    store_fut.await?;
                    replication_msgs = replication_res?;
                }
                Either::Right((store_res, replication_fut)) => {
                    store_res?;
                    replication_msgs = replication_fut.await?;
                }
            }
        }
    }

    /// Run the replication loop, looking for changes to the configuration and
    /// adjusting.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::Storage, test_infrastructure::*, Serializable};
    use futures::executor::LocalPool;
    use std::{future::Future, pin::Pin, task::Poll};

    #[test]
    fn replication_loop_start() {
        let storage = SimpleStorage::default();
        let protocol = SimpleProtocol::new(vec![
            SimpleBuffer::new(0, vec![SimpleEntry(42), SimpleEntry(-42)]).serialize(),
        ]);
        let cluster = SimpleCluster::new(vec![SimpleNode(0), SimpleNode(1), SimpleNode(2)]);
        let mut replication = Replication::new(SimpleNode(1), storage, protocol, cluster);

        {
            let mut replication_loop = Box::pin(replication.replication_loop(&SimpleNode(0)));
            let waker = futures::task::noop_waker_ref();
            let mut cx = std::task::Context::from_waker(waker);
            let run_one = Pin::new(&mut replication_loop).poll(&mut cx);
            assert!(run_one.is_pending());
        }

        // assert replication made the right fetch calls
        let fetches = &replication.node_protocol.fetches;
        assert_eq!(2, fetches.len());
        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: None,
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[0].clone()
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(2),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[1].clone()
        );

        // assert we have the right entries
        let entries = &replication.storage.entries;
        assert_eq!(2, entries.len());
        assert_eq!(SimpleEntry(42), entries[0]);
        assert_eq!(SimpleEntry(-42), entries[1]);
    }

    #[test]
    fn replication_loop_start_with_existing_entries() {
        let mut storage = SimpleStorage::default();
        storage.append(SimpleEntry(100)).run_until_blocked().unwrap();
        storage.append(SimpleEntry(9)).run_until_blocked().unwrap();
        storage.append(SimpleEntry(-10)).run_until_blocked().unwrap();

        let protocol = SimpleProtocol::new(vec![
            SimpleBuffer::new(3, vec![SimpleEntry(42), SimpleEntry(-42)]).serialize(),
        ]);
        let cluster = SimpleCluster::new(vec![SimpleNode(0), SimpleNode(1), SimpleNode(2)]);
        let mut replication = Replication::new(SimpleNode(1), storage, protocol, cluster);

        {
            let mut replication_loop = Box::pin(replication.replication_loop(&SimpleNode(0)));
            let waker = futures::task::noop_waker_ref();
            let mut cx = std::task::Context::from_waker(waker);
            let run_one = Pin::new(&mut replication_loop).poll(&mut cx);
            assert!(run_one.is_pending());
        }

        // assert replication made the right fetch calls
        let fetches = &replication.node_protocol.fetches;
        assert_eq!(2, fetches.len());
        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(3),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[0].clone()
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(5),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[1].clone()
        );

        // assert we have the right entries
        let entries = &replication.storage.entries;
        assert_eq!(5, entries.len());
        assert_eq!(SimpleEntry(100), entries[0]);
        assert_eq!(SimpleEntry(9), entries[1]);
        assert_eq!(SimpleEntry(-10), entries[2]);
        assert_eq!(SimpleEntry(42), entries[3]);
        assert_eq!(SimpleEntry(-42), entries[4]);
    }

    #[test]
    fn replication_loop_fetch_while_writing() {
        let storage = SimpleStorage::default();
        let protocol = SimpleProtocol::new(vec![
            SimpleBuffer::new(0, vec![SimpleEntry(42), SimpleEntry(-42)]).serialize(),
            SimpleBuffer::new(2, vec![SimpleEntry(120)]).serialize(),
            SimpleBuffer::new(3, vec![SimpleEntry(360), SimpleEntry(-66)]).serialize(),
        ]);
        let cluster = SimpleCluster::new(vec![SimpleNode(0), SimpleNode(1), SimpleNode(2)]);
        let mut replication = Replication::new(SimpleNode(1), storage, protocol, cluster);

        {
            let mut replication_loop = Box::pin(replication.replication_loop(&SimpleNode(0)));
            let waker = futures::task::noop_waker_ref();
            let mut cx = std::task::Context::from_waker(waker);
            let run_one = Pin::new(&mut replication_loop).poll(&mut cx);
            assert!(run_one.is_pending());
        }

        // assert replication made the right fetch calls
        let fetches = &replication.node_protocol.fetches;
        assert_eq!(4, fetches.len());
        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: None,
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[0].clone()
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(2),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[1].clone()
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(3),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[2].clone()
        );
        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(5),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[3].clone()
        );

        // assert we have the right entries
        let entries = &replication.storage.entries;
        assert_eq!(5, entries.len());
        assert_eq!(SimpleEntry(42), entries[0]);
        assert_eq!(SimpleEntry(-42), entries[1]);
        assert_eq!(SimpleEntry(120), entries[2]);
        assert_eq!(SimpleEntry(360), entries[3]);
        assert_eq!(SimpleEntry(-66), entries[4]);
    }

    #[test]
    fn run_notices_cluster_change() {
        let storage = SimpleStorage::default();
        let protocol = SimpleProtocol::new(vec![
            SimpleBuffer::new(0, vec![SimpleEntry(42), SimpleEntry(-42)]).serialize(),
        ]);
        let cluster = SimpleCluster::new(vec![SimpleNode(0), SimpleNode(1), SimpleNode(2)]);
        let mut replication = Replication::new(SimpleNode(2), storage, protocol, cluster.clone());

        {
            let mut replication_loop = Box::pin(replication.run());
            let waker = futures::task::noop_waker_ref();
            let mut cx = std::task::Context::from_waker(waker);
            assert!(Pin::new(&mut replication_loop).poll(&mut cx).is_pending());

            cluster.remove_node(1);

            assert!(Pin::new(&mut replication_loop).poll(&mut cx).is_pending());
        }

        let fetches = &replication.node_protocol.fetches;
        assert_eq!(3, fetches.len());
        assert_eq!(
            Fetch {
                for_node: SimpleNode(1),
                starting_slot: None,
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[0].clone()
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(1),
                starting_slot: Some(2),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[1].clone()
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(0),
                starting_slot: Some(2),
                message_limit: MessageLimit(DEFAULT_MESSAGE_LIMIT)
            },
            fetches[2].clone()
        );
    }
}
