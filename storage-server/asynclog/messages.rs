use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use commitlog::{
    message::{serialize, MessageSet, MessageSetMut},
    Offset,
};

/// Single message append, with client_id, client_req_id and payload
pub type SingleMessage = (u64, u64, Bytes);

/// Readonly view of messages, either via replication or as
/// a result of appending log entries.
#[derive(Clone)]
pub struct Messages {
    bytes: Bytes,
    len: usize,
    next_offset: Option<Offset>,
}

impl MessageSet for Messages {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn len(&self) -> usize {
        self.len
    }
}

/// Error parsing the binary message set
#[derive(Debug, Clone, Copy)]
pub enum MessageParseError {
    /// A message has an invalid offset. This is an indication
    /// that the replication was not completed correctly.
    InvalidHash,

    /// No messages were appended.
    NoMessages,
}

impl Messages {
    /// Parses the messages from another byte source (such as an upstream replica).
    ///
    /// The messages must be non-empty.
    pub fn parse(bytes: Bytes) -> Result<Messages, MessageParseError> {
        // temporary struct to enumerate messages
        //
        // this optimization will cache the length
        // and next offset, which are used within the
        // async log when appending from replication
        // (which happens in the hot path)
        struct TmpMessages(Bytes);

        impl MessageSet for TmpMessages {
            fn bytes(&self) -> &[u8] {
                &self.0
            }
        }

        let msgs = TmpMessages(bytes);

        let mut len = 0;
        let mut last_offset = None;

        for msg in msgs.iter() {
            if rare!(!msg.verify_hash()) {
                return Err(MessageParseError::InvalidHash);
            }

            len += 1;
            last_offset = Some(msg.offset());
        }

        match last_offset {
            Some(off) => Ok(Messages {
                bytes: msgs.0,
                len,
                next_offset: Some(off + 1),
            }),
            None => Err(MessageParseError::NoMessages),
        }
    }

    #[inline]
    pub fn into_inner(self) -> Bytes {
        self.bytes
    }

    /// Next offset to be appended.
    #[inline]
    pub fn next_offset(&self) -> Option<Offset> {
        self.next_offset
    }
}

impl AsRef<[u8]> for Messages {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
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

#[derive(Debug, Copy, Clone)]
pub enum MessagePushError {
    /// No capacity to add the message
    OutOfCapacity,
}

impl MessagesMut {
    /// Freezes the messages from modification.
    pub fn freeze(self) -> Messages {
        let len = self.len();
        Messages {
            bytes: self.0.freeze(),
            len,
            next_offset: None,
        }
    }

    /// Insert a new log entry to the message set.
    #[inline]
    pub fn push<B: AsRef<[u8]>>(
        &mut self,
        client_id: u64,
        client_req_id: u64,
        payload: B,
    ) -> Result<(), MessagePushError> {
        let payload_bytes = payload.as_ref();
        if payload_bytes.len() + 25 + self.0.len() > self.0.capacity() {
            return Err(MessagePushError::OutOfCapacity);
        }

        let mut meta = [0u8; 16];
        LittleEndian::write_u64(&mut meta[0..8], client_id);
        LittleEndian::write_u64(&mut meta[8..16], client_req_id);
        serialize(&mut self.0, 0, &meta, payload_bytes);
        Ok(())
    }
}
