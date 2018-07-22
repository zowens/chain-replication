use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use commitlog::{
    message::{serialize, MessageSet, MessageSetMut},
    Offset,
};

/// Readonly view of messages, either via replication or as
/// a result of appending log entries.
#[derive(Clone)]
pub struct Messages(pub Bytes);

impl MessageSet for Messages {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Messages {
    /// Next offset to be appended.
    pub fn next_offset(&self) -> Option<Offset> {
        self.iter().last().map(|m| m.offset() + 1)
    }
}

impl AsRef<[u8]> for Messages {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Bytes> for Messages {
    fn from(bytes: Bytes) -> Messages {
        Messages(bytes)
    }
}

/// Mutable message set based on `BytesMut`.
pub struct MessagesMut(pub BytesMut);

impl From<BytesMut> for MessagesMut {
    fn from(bytes: BytesMut) -> MessagesMut {
        MessagesMut(bytes)
    }
}

impl MessageSet for MessagesMut {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageSetMut for MessagesMut {
    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl MessagesMut {
    /// Freezes the messages from modification.
    pub fn freeze(self) -> Messages {
        Messages(self.0.freeze())
    }

    /// Insert a new log entry to the message set.
    #[inline]
    pub fn push<B: AsRef<[u8]>>(&mut self, client_id: u64, client_req_id: u64, payload: B) {
        let mut meta = [0u8; 16];
        LittleEndian::write_u64(&mut meta[0..8], client_id);
        LittleEndian::write_u64(&mut meta[8..16], client_req_id);
        serialize(&mut self.0, 0, &meta, payload);
    }
}
