use crate::{Buffer, Entry, Key, KeyedEntry, Slot};
use std::{error::Error, fmt::Debug, future::Future};

/// Abstraction for persistent storage for chain replicaiton. The Storage must
/// be able to linearly request updates, append operations, and lookup entries
/// keyed with a certain key.
pub trait Storage<E: Entry> {
    // TODO: do we need this?
    type Buffer: Buffer<E>;
    type Error: Error + Debug + 'static;
    type LatestSlotFuture: Future<Output = Result<Option<Slot>, Self::Error>>;
    type AppendFuture: Future<Output = Result<Slot, Self::Error>>;
    type AppendBufferFuture: Future<Output = Result<(), Self::Error>>;
    type OperationsFuture: Future<Output = Result<Option<Self::Buffer>, Self::Error>>;

    /// Grabs the latest slot number of applied operations.
    fn latest_slot(&self) -> Self::LatestSlotFuture;

    /// Adds operations into the storage.
    fn append(&mut self, entry: E) -> Self::AppendFuture;

    /// Appends to the storage from another node with slot numbers within the
    /// buffer.
    fn append_from_buffer(&mut self, operations: Self::Buffer) -> Self::AppendBufferFuture;

    // TODO: Option<Buffer> is strange for operations

    /// Queries for log entries starting at a given slot.
    fn operations(&self, starting_offset: Slot, max_entries: u64) -> Self::OperationsFuture;
}

/// Abstraction for persistent storage for chain replicaiton. The Storage must
/// be able to linearly request updates, append operations, and lookup entries
/// keyed with a certain key.
pub trait KeyedStorage<K: Key, E: KeyedEntry<Key = K>>: Storage<E> {
    type EntryFuture: Future<Output = Result<Option<(Slot, E)>, Self::Error>>;
    /// Grabs the latest slot and value for a key.
    fn entry(&self, key: K) -> Self::EntryFuture;
}
