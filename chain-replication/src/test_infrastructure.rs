use crate::{
    communication::{MessageLimit, NodeProtocol},
    configuration::{Cluster, Node, NodeId, Role},
    storage::Storage,
    Buffer, Entry, Serializable, Slot,
};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core::ops::Range;
use futures::{
    channel::mpsc,
    executor::LocalPool,
    future::{err, ok, pending, Ready},
    task::{LocalSpawn, LocalSpawnExt},
};
use std::{
    borrow::Borrow, boxed::Box, cell::RefCell, collections::VecDeque, future::Future,
    marker::PhantomData, rc::Rc,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimpleEntry(i64);

impl Serializable for SimpleEntry {
    type Output = Bytes;

    fn deserialize<B: Buf>(b: &mut B) -> Result<SimpleEntry, ()> {
        Ok(SimpleEntry(b.get_i64_le()))
    }

    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i64_le(self.0);
        buf.freeze()
    }
}

impl Entry for SimpleEntry {}

#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub struct SimpleNode(u64);
impl Node for SimpleNode {
    fn id(&self) -> NodeId {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Fetch {
    pub for_node: SimpleNode,
    pub starting_slot: Option<Slot>,
    pub message_limit: MessageLimit,
}

pub struct SimpleProtocol {
    pub fetches: Vec<Fetch>,
    fetch_answers: VecDeque<Bytes>,
}

impl SimpleProtocol {
    fn new<I: Into<VecDeque<Bytes>>>(fetch_answers: I) -> SimpleProtocol {
        SimpleProtocol { fetches: Vec::new(), fetch_answers: fetch_answers.into() }
    }
}

impl NodeProtocol for SimpleProtocol {
    type Node = SimpleNode;
    type Error = ();
    type FetchBuffer = Bytes;
    type FetchFuture = Box<dyn Future<Output = Result<Bytes, ()>> + Unpin>;

    fn fetch(
        &mut self,
        node: &SimpleNode,
        starting_slot: Option<Slot>,
        message_limit: MessageLimit,
    ) -> Self::FetchFuture {
        self.fetches.push(Fetch { for_node: *node, starting_slot, message_limit });
        match self.fetch_answers.pop_front() {
            Some(bytes) => Box::new(ok(bytes)),
            None => Box::new(pending()),
        }
    }
}

struct SimpleCluster {
    nodes: Vec<SimpleNode>,
    receiver: Option<mpsc::Receiver<()>>,
    sender: mpsc::Sender<()>,
}

impl SimpleCluster {
    pub fn new(nodes: Vec<SimpleNode>) -> SimpleCluster {
        let (sender, receiver) = mpsc::channel(1);
        SimpleCluster { nodes, receiver: Some(receiver), sender }
    }

    pub fn add_node(&mut self, node: SimpleNode) {
        self.nodes.push(node);
        self.sender.try_send(());
    }

    pub fn remove_node(&mut self, id: NodeId) {
        if let Some(i) = self.nodes.iter().position(|n| n.0 == id) {
            self.nodes.remove(i);
            self.sender.try_send(());
        }
    }
}

impl Cluster for SimpleCluster {
    type Node = SimpleNode;
    type ChangeStream = mpsc::Receiver<()>;

    fn node_info(&self, id: NodeId) -> Option<SimpleNode> {
        self.nodes.iter().find(|n| n.0 == id).cloned()
    }

    fn nodes(&self) -> usize {
        return self.nodes.len();
    }

    fn head_node(&self) -> Option<SimpleNode> {
        self.nodes.first().cloned()
    }

    fn tail_node(&self) -> Option<SimpleNode> {
        self.nodes.last().cloned()
    }

    fn upstream_from(&self, node: &SimpleNode) -> Option<SimpleNode> {
        let mut upstream: Option<usize> = None;
        for i in 0..self.nodes.len() {
            if self.nodes[i].0 == node.0 {
                return upstream.map(|i| self.nodes[i].clone());
            }
            upstream = Some(i);
        }
        return None;
    }

    fn current_role(&self, node: &SimpleNode) -> Option<Role> {
        for i in 0..self.nodes.len() {
            // not the node you're looking for
            if self.nodes[i].0 != node.0 {
                continue;
            }

            if i == 0 {
                return Some(Role::Head);
            }
            if i == self.nodes.len() - 1 {
                return Some(Role::Tail);
            }
            return Some(Role::Inner);
        }
        return None;
    }

    fn change_notification(&mut self) -> Self::ChangeStream {
        return self.receiver.take().unwrap();
    }
}

/// Buffer with an extremely simple encoding:
///
/// * u64_le (length of bytes after this length)
/// * u64_le (starting_slot)
/// * u64_le (count)
/// * for each entry:
///    - u64_le (length in bytes`)
///    - bytes[length]
pub struct SimpleBuffer<E: Entry>(Bytes, PhantomData<E>);

impl<E: Entry> SimpleBuffer<E> {
    pub fn new<I: IntoIterator>(starting_slot: Slot, i: I) -> SimpleBuffer<E>
    where
        I::Item: Borrow<E>,
    {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u64_le(0);
        buf.put_u64_le(starting_slot);
        buf.put_u64_le(0);

        // add the entries to the buffer
        let mut count = 0u64;
        for e in i.into_iter() {
            count += 1;
            let serialized = e.borrow().serialize();
            let bytes = serialized.as_ref();
            buf.reserve(8 + bytes.len());
            buf.put_u64_le(bytes.len() as u64);
            buf.put(bytes);
        }

        // apply the header and merge the header and body back together
        let len = buf.len() as u64;
        LittleEndian::write_u64(&mut buf[0..8], len);
        LittleEndian::write_u64(&mut buf[16..24], count);

        SimpleBuffer(buf.freeze(), PhantomData)
    }

    pub fn iter(&self) -> BufferIter<E> {
        let mut bytes = self.0.clone();
        // ignore length
        bytes.get_u64_le();

        let starting_slot = bytes.get_u64_le();
        let count = bytes.get_u64_le();
        BufferIter {
            encoded: bytes,
            slot: starting_slot,
            end_slot: starting_slot + count,
            _e: PhantomData,
        }
    }
}

impl<E: Entry> Serializable for SimpleBuffer<E> {
    type Output = Bytes;

    fn deserialize<B: Buf>(b: &mut B) -> Result<Self, ()> {
        // TODO: check bounds
        let len = b.get_u64_le();
        let mut buf = BytesMut::with_capacity((len as usize) + 8);
        buf.put_u64_le(len);
        buf.put(b.take(len as usize));
        Ok(SimpleBuffer(buf.freeze(), PhantomData))
    }

    fn serialize(&self) -> Bytes {
        self.0.clone()
    }
}

impl<E: Entry> Buffer<E> for SimpleBuffer<E> {
    fn slots(&self) -> Range<Slot> {
        let mut view = self.0.slice(8..24);
        let starting_slot = view.get_u64_le();
        let count = view.get_u64_le();
        starting_slot..starting_slot + count
    }
}

pub struct BufferIter<E: Entry> {
    encoded: Bytes,
    slot: Slot,
    end_slot: Slot,
    _e: PhantomData<E>,
}

impl<E: Entry> Iterator for BufferIter<E> {
    type Item = (Slot, E);
    fn next(&mut self) -> Option<Self::Item> {
        if self.slot >= self.end_slot {
            return None;
        }

        let slot = self.slot;
        self.slot += 1;

        let size = self.encoded.get_u64_le();
        let mut bytes = self.encoded.split_to(size as usize);
        let val = E::deserialize(&mut bytes).unwrap();
        Some((slot, val))
    }
}

#[derive(Eq, PartialEq, Error, Debug)]
pub enum StorageError {
    #[error("The slot {given:?} is not the expected value of {expected:?}")]
    MisalignedSlot { given: Slot, expected: Slot },
}

pub struct SimpleStorage<E: Entry> {
    pub entries: Vec<E>,
    _e: PhantomData<E>,
}

impl<E: Entry> Default for SimpleStorage<E> {
    fn default() -> SimpleStorage<E> {
        SimpleStorage { entries: Vec::new(), _e: PhantomData }
    }
}

impl<E: Entry> Storage<E> for SimpleStorage<E> {
    type Buffer = SimpleBuffer<E>;
    type Error = StorageError;
    type LatestSlotFuture = Ready<Result<Option<Slot>, StorageError>>;
    type AppendFuture = Ready<Result<Slot, StorageError>>;
    type AppendBufferFuture = Ready<Result<(), StorageError>>;
    type OperationsFuture = Ready<Result<Option<Self::Buffer>, StorageError>>;

    fn latest_slot(&self) -> Self::LatestSlotFuture {
        match self.entries.len() {
            0 => ok(None),
            l => ok(Some((l as u64) - 1)),
        }
    }

    fn append(&mut self, entry: E) -> Self::AppendFuture {
        let slot = self.entries.len() as u64;
        self.entries.push(entry);
        ok(slot)
    }

    fn append_from_buffer(&mut self, operations: Self::Buffer) -> Self::AppendBufferFuture {
        // make sure we have the right
        let expected = self.entries.len() as Slot;
        if operations.slots().start != expected {
            err(StorageError::MisalignedSlot { given: operations.slots().start, expected })
        } else {
            self.entries.extend(operations.iter().map(|(_, e)| e));
            ok(())
        }
    }

    fn operations(&self, starting_offset: Slot, max_entries: u64) -> Self::OperationsFuture {
        if starting_offset >= self.entries.len() as u64 {
            ok(None)
        } else {
            let iter =
                self.entries.iter().skip(starting_offset as usize).take(max_entries as usize);
            ok(Some(SimpleBuffer::new(starting_offset, iter)))
        }
    }
}

fn run_until_blocked<F: Future + 'static>(f: F) -> Option<F::Output> {
    let mut pool = LocalPool::new();
    let output = Rc::new(RefCell::new(None));
    let output_clone = output.clone();
    pool.spawner()
        .spawn_local(async move {
            let v = f.await;
            output_clone.replace(Some(v));
            ()
        })
        .expect("Unexpected spawn error");
    pool.run_until_stalled();
    output.replace(None)
}

pub trait TestFutureExt: Future {
    fn run_until_blocked(self) -> Option<Self::Output>
    where
        Self: Sized + 'static,
    {
        run_until_blocked(self)
    }
}

impl<T: ?Sized> TestFutureExt for T where T: Future {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Buffer;

    #[test]
    fn buffer_empty() {
        let buf = SimpleBuffer::new::<Vec<SimpleEntry>>(10, vec![]);
        assert_eq!(10..10, buf.slots());
        assert_eq!(0, buf.count());
        assert!(!buf.iter().next().is_some());
    }

    #[test]
    fn buffer_with_entries() {
        let buf = SimpleBuffer::new(10, vec![SimpleEntry(-5), SimpleEntry(15), SimpleEntry(99)]);
        assert_eq!(10..13, buf.slots());
        assert_eq!(3, buf.count());
        assert_eq!(72, buf.0.len());
        let mut iter = buf.iter();

        assert_eq!(Some((10, SimpleEntry(-5))), iter.next());
        assert_eq!(Some((11, SimpleEntry(15))), iter.next());
        assert_eq!(Some((12, SimpleEntry(99))), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn cluster_view() {
        let cluster = SimpleCluster::new(vec![SimpleNode(0), SimpleNode(1), SimpleNode(2)]);
        assert_eq!(3, cluster.nodes());
        assert_eq!(Some(SimpleNode(0)), cluster.head_node());
        assert_eq!(Some(SimpleNode(2)), cluster.tail_node());
        assert_eq!(Some(SimpleNode(2)), cluster.tail_node());
        assert_eq!(Some(SimpleNode(0)), cluster.node_info(0));
        assert_eq!(Some(SimpleNode(1)), cluster.node_info(1));
        assert_eq!(Some(SimpleNode(2)), cluster.node_info(2));

        assert_eq!(None, cluster.upstream_from(&SimpleNode(0)));
        assert_eq!(Some(SimpleNode(0)), cluster.upstream_from(&SimpleNode(1)));
        assert_eq!(Some(SimpleNode(1)), cluster.upstream_from(&SimpleNode(2)));

        // non-existant node
        assert_eq!(None, cluster.node_info(3));
        assert_eq!(None, cluster.upstream_from(&SimpleNode(3)));
    }

    #[test]
    fn protocol_test() {
        let mut protocol = SimpleProtocol::new(vec![
            Bytes::from_static(&[0xff, 0xee, 0x00]),
            Bytes::from_static(&[0x00, 0x11, 0x12]),
            Bytes::from_static(&[0xea, 0xeb, 0xec, 0xed, 0xee, 0xef]),
        ]);

        let fetch1 = protocol
            .fetch(&SimpleNode(1), None, MessageLimit(100))
            .run_until_blocked()
            .unwrap()
            .unwrap();
        assert_eq!(Bytes::from_static(&[0xff, 0xee, 0x00]), fetch1);

        let fetch2 = protocol
            .fetch(&SimpleNode(1), Some(12), MessageLimit(1))
            .run_until_blocked()
            .unwrap()
            .unwrap();
        assert_eq!(Bytes::from_static(&[0x00, 0x11, 0x12]), fetch2);

        let fetch3 = protocol
            .fetch(&SimpleNode(2), Some(8), MessageLimit(5))
            .run_until_blocked()
            .unwrap()
            .unwrap();
        assert_eq!(Bytes::from_static(&[0xea, 0xeb, 0xec, 0xed, 0xee, 0xef]), fetch3);

        assert_eq!(3, protocol.fetches.len());

        assert_eq!(
            Fetch {
                for_node: SimpleNode(1),
                starting_slot: None,
                message_limit: MessageLimit(100)
            },
            protocol.fetches[0]
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(1),
                starting_slot: Some(12),
                message_limit: MessageLimit(1)
            },
            protocol.fetches[1]
        );

        assert_eq!(
            Fetch {
                for_node: SimpleNode(2),
                starting_slot: Some(8),
                message_limit: MessageLimit(5)
            },
            protocol.fetches[2]
        );
    }

    #[test]
    fn storage() {
        let mut storage = SimpleStorage::<SimpleEntry>::default();

        assert_eq!(Some(Ok(None)), storage.latest_slot().run_until_blocked());

        let append_res = storage.append(SimpleEntry(-100)).run_until_blocked();
        assert_eq!(Some(Ok(0)), append_res);

        assert_eq!(Some(Ok(Some(0))), storage.latest_slot().run_until_blocked());

        // aligned buffer
        let append_buf = SimpleBuffer::new(1, vec![SimpleEntry(1), SimpleEntry(2), SimpleEntry(5)]);
        let append_buf_res = storage.append_from_buffer(append_buf).run_until_blocked();
        assert_eq!(Some(Ok(())), append_buf_res);
        assert_eq!(Some(Ok(Some(3))), storage.latest_slot().run_until_blocked());

        // try a misaligned buffer
        let append_buf = SimpleBuffer::new(8, vec![SimpleEntry(100)]);
        let append_buf_res = storage.append_from_buffer(append_buf).run_until_blocked();
        assert_eq!(
            Some(Err(StorageError::MisalignedSlot { given: 8, expected: 4 })),
            append_buf_res
        );

        let ops = storage.operations(0, 2).run_until_blocked().unwrap().unwrap().unwrap();
        assert_eq!(0..2, ops.slots());
        assert_eq!(
            vec![(0, SimpleEntry(-100)), (1, SimpleEntry(1))],
            ops.iter().collect::<Vec<_>>()
        );

        let ops = storage.operations(2, 10).run_until_blocked().unwrap().unwrap().unwrap();
        assert_eq!(2..4, ops.slots());
        assert_eq!(vec![(2, SimpleEntry(2)), (3, SimpleEntry(5))], ops.iter().collect::<Vec<_>>());

        let ops = storage.operations(4, 10).run_until_blocked().unwrap().unwrap();
        assert!(ops.is_none());

        let ops = storage.operations(100, 10).run_until_blocked().unwrap().unwrap();
        assert!(ops.is_none());
    }
}
