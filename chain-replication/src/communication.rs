use crate::{configuration::Node, Slot};
use bytes::Buf;
use futures::stream::Stream;
use std::future::Future;

/// Maximum number of messages
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MessageLimit(pub usize);

pub trait NodeProtocol {
    type Node: Node;
    type Error: std::fmt::Debug + Send + Sync + 'static;
    type FetchBuffer: Buf;
    type FetchFuture: Future<Output = Result<Self::FetchBuffer, Self::Error>>;

    /// Fetches entries from another node
    fn fetch(
        &mut self,
        node: &Self::Node,
        starting_slot: Option<Slot>,
        message_limit: MessageLimit,
    ) -> Self::FetchFuture;
}

pub trait ClientProtocol {
    type Node;
    type Client;
    type MessageToken;

    type ClientError: std::fmt::Debug + Send + Sync + 'static;
    type AppendFuture: Future<Output = Result<Self::MessageToken, Self::ClientError>>;
    type LatestSlotFuture: Future<Output = Result<Option<Slot>, Self::ClientError>>;
    type ReplyStream: Stream<Item = Result<Self::MessageToken, Self::ClientError>>;

    /// Requests the head node to append a message to the chain.
    fn append<B: Buf>(
        &mut self,
        node: Self::Node,
        client: Self::Client,
        entry: B,
    ) -> Self::AppendFuture;

    /// Fetches the latest slot number from any node
    fn latest_slot(&mut self, node: Self::Node) -> Self::LatestSlotFuture;

    fn replies(&mut self, client: Self::Client) -> Self::ReplyStream;
}

pub trait Handler {
    type Error: std::fmt::Debug + Send + Sync + 'static;
    type FetchBuffer: Buf;
    type Client;
    type MessageToken;
    type FetchFuture: Future<Output = Result<Self::FetchBuffer, Self::Error>>;
    type AppendFuture: Future<Output = Result<Self::MessageToken, Self::Error>>;
    type LatestSlotFuture: Future<Output = Result<Option<Slot>, Self::Error>>;
    type ReplyStream: Stream<Item = Result<Self::MessageToken, Self::Error>>;

    /// Fetches entries from current node
    fn fetch(
        &mut self,
        starting_slot: Option<Slot>,
        message_limit: MessageLimit,
    ) -> Self::FetchFuture;

    /// Requests the head node to append a message to the chain.
    fn append<B: Buf>(
        &mut self,
        client: Self::Client,
        entry: B,
    ) -> Self::AppendFuture;

    /// Fetches the latest slot number from any node
    fn latest_slot(&mut self) -> Self::LatestSlotFuture;

    fn replies(&mut self) -> Self::ReplyStream;
}
