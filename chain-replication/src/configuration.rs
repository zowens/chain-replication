use futures::stream::Stream;
use std::marker::Unpin;

pub type NodeId = u64;

pub trait Node {
    /// The node identifier within the cluster
    fn id(&self) -> NodeId;
}

pub enum Role {
    Head,
    Inner,
    Tail,
}

pub trait Cluster {
    type Node: Node;

    type ChangeStream: Stream<Item = ()> + Unpin;

    /// Looks up the node information by node Id
    fn node_info(&self, id: NodeId) -> Option<Self::Node>;

    /// The number of nodes in the cluster
    fn nodes(&self) -> usize;

    /// Looks up the head node
    fn head_node(&self) -> Option<Self::Node>;

    /// Looks up the tail node
    fn tail_node(&self) -> Option<Self::Node>;

    /// Describes which node is up the chain from a particular node.
    ///
    /// Head nodes do not have an upstream.
    fn upstream_from(&self, node: &Self::Node) -> Option<Self::Node>;

    /// Looks up the role for a particular
    fn current_role(&self, node: &Self::Node) -> Option<Role>;

    /// Creates a listener for changes to the configuration
    fn change_notification(&mut self) -> Self::ChangeStream;
}
