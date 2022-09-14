use bytes::Buf;
use std::{cmp::Eq, hash::Hash, ops::Range};

mod communication;
mod configuration;
mod replication;
mod storage;
#[cfg(test)]
mod test_infrastructure;

/// The operation number
pub type Slot = u64;

/// Value is an abstract trait used for serializaiton and deserialization.
pub trait Serializable: Sized {
    type Output: AsRef<[u8]>;

    /// Deserializes the value
    fn deserialize<B: Buf>(b: &mut B) -> Result<Self, ()>;

    /// Serializes the underlying value into a byte array. Implementations
    /// may choose to be zero copy.
    fn serialize(&self) -> Self::Output;
}

pub trait Entry: Serializable {}

/// The logical object key. Chain replication is optimized
/// for multiple values existing in the same logical storage, segmented
/// by `Key`.
pub trait Key: Eq + Hash {}

/// An Entry is an object within the system.
pub trait KeyedEntry: Entry {
    type Key: Key;

    /// The key for the entry
    fn key(&self) -> Self::Key;

    /// Applies an operation to an existing entry
    fn merge(&mut self, op: Self);
}

/// Holder of entries
pub trait Buffer<E: Entry>: Serializable {
    fn slots(&self) -> Range<Slot>;
    fn count(&self) -> usize {
        let range = self.slots();
        (range.end - range.start) as usize
    }
}
