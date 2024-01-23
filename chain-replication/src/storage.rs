use crate::{Buffer, Entry, Key, KeyedEntry, Slot};
use std::{error::Error, fmt::Debug, future::Future};
use std::marker::PhantomData;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::IoSlice;

/// Abstraction for persistent storage for chain replicaiton. The Storage must
/// be able to linearly request updates, append operations, and lookup entries
/// keyed with a certain key.
pub trait Storage<E: Entry> {
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





/// Storage implementation that buffers entries in memory while replication is waiting.
pub struct BufferedStorage<E: Entry, S: Storage<E>> {
    storage: S,
    _e: PhantomData<E>,
}

struct ChainedBuf<B: Buf> {
    buffers: VecDeque<B>,
}

impl<B: Buf> Buf for ChainedBuf<B> {

    fn remaining(&self) -> usize {
        self.buffers.iter().map(|b| b.remaining()).sum()
    }

    fn chunk(&self) -> &[u8] {
        for b in self.buffers.iter() {
            if b.has_remaining() {
                return b.chunk()
            }
        }
        return &[];
    }

    fn advance(&mut self, mut cnt: usize) {
        loop {
            match self.buffers.front_mut() {
                Some(mut v) if v.has_remaining() => {
                    let v_rem = v.remaining();
                    if v_rem >= cnt {
                        v.advance(cnt);
                        return;
                    }
                    v.advance(v_rem);
                    cnt -= v_rem;
                }
                None => return,
                _ => {
                    // remove empty buffer without calling advance
                }
            }

            // if we get to this branch, pop the front value (does not have any remaining itmes)
            self.buffers.pop_front();
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut n = 0;
        for b in self.buffers.iter() {
            n += b.chunks_vectored(&mut dst[n..]);
            if n >= dst.len() {
                break;
            }
        }

        n
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        // pop empty buffers
        loop {
            match self.buffers.front() {
                Some(v) if v.has_remaining() => break,
                None => return Bytes::from_static(&[]),
                _ => {
                    // fallthrough to pop the buffer
                }
            }
            self.buffers.pop_front();
        }

        // fast path: the first buffer can be coppied
        {
            let buf_len = self.buffers.len();
            let front = self.buffers.front_mut().unwrap();
            if buf_len == 1 || front.remaining() <= len {
                return front.copy_to_bytes(len);
            }
        }

        let mut ret = BytesMut::with_capacity(len);
        let mut written = 0;
        for mut b in self.buffers.iter_mut() {
            let b_rem = b.remaining();
            if b_rem + written < len {
                ret.put(&mut b);
                written += b_rem;
            } else {
                ret.put((&mut b).take(len - b_rem));
                break;
            }
        }
        return ret.freeze();

        /*let a_rem = self.a.remaining();
        if a_rem >= len {
            self.a.copy_to_bytes(len)
        } else if a_rem == 0 {
            self.b.copy_to_bytes(len)
        } else {
            assert!(
                len - a_rem <= self.b.remaining(),
                "`len` greater than remaining"
            );
            let mut ret = crate::BytesMut::with_capacity(len);
            ret.put(&mut self.a);
            ret.put((&mut self.b).take(len - a_rem));
            ret.freeze()
        }*/
    }
}
