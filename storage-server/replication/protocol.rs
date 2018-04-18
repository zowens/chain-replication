///! # Replicaiton Protocol
///!
///! ## Replication Requests
///!
///! ReplicationRequest = Length OpCode Request
///!     Length : u32 = <length of entire request (including headers)>
///!     OpCode : u8 = 0
///!     Offset : u64 = <Inclusive offset of the first entry to pull>
///!
///! ## Replication Response
///!
///! ReplicationResponse = Length Response
///!     Length     : u32 = <length of entire response (including headers)>
///!     OpCode     : u8 = 0
///!     NextOffset : u64 = <Next Offset to request>
///!     MessageBuf : Message*
use byteorder::ByteOrder;
use bytes::{Buf, BufMut, BytesMut, IntoBuf, LittleEndian};
use std::intrinsics::unlikely;
use std::io;
use tokio_io::codec::{Decoder, Encoder};

macro_rules! rare {
    ($e:expr) => {
        unsafe { unlikely($e) }
    };
}

/// Request to replicate starting at the next offset
pub struct ReplicationRequest {
    pub starting_offset: u64,
}

/// Initial header for replicating messages
pub struct ReplicationResponseHeader {
    /// Length of the binary encoding of the messages
    pub messages_bytes_len: u32,
}

/// Result of replication
pub struct ReplicationResponse {
    /// Binary encoding of the message set
    pub messages: BytesMut,
}

type OpCode = u8;

#[inline]
fn decode_header(buf: &mut BytesMut) -> Option<(OpCode, BytesMut)> {
    // must have at least 5 bytes
    //    [0..4] - LittleEndian u32, length of message (including header)
    //    [5] - u8 - opcode
    buf.reserve(5);
    if rare!(buf.len() < 5) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }

    let len = LittleEndian::read_u32(&buf[0..4]) as usize;

    // ensure we have enough
    if rare!(buf.len() < len) {
        buf.reserve(len);
        return None;
    }

    // drain to the length and opcode, then remove the length field
    let mut buf = buf.split_to(len);
    // parse by op code, remove length field
    let op = buf.split_to(5)[4];
    Some((op, buf))
}

#[derive(Default)]
pub struct ServerProtocol;

impl Encoder for ServerProtocol {
    type Item = ReplicationResponseHeader;
    type Error = io::Error;

    fn encode(
        &mut self,
        item: ReplicationResponseHeader,
        dst: &mut BytesMut,
    ) -> Result<(), io::Error> {
        dst.put_u32_le(5 + item.messages_bytes_len);
        dst.put_u8(0);
        Ok(())
    }
}

impl Decoder for ServerProtocol {
    type Item = ReplicationRequest;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        match decode_header(src) {
            Some((0, buf)) => {
                if rare!(buf.len() < 8) {
                    return Err(io::Error::new(io::ErrorKind::Other, "Invalid length"));
                }

                // read the offset
                let starting_offset = buf.into_buf().get_u64_le();
                Ok(Some(ReplicationRequest { starting_offset }))
            }
            Some((opcode, _)) => {
                error!("Unknown op code {:X}", opcode);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            }
            None => Ok(None),
        }
    }
}

#[derive(Default)]
pub struct ClientProtocol;

impl Encoder for ClientProtocol {
    type Item = ReplicationRequest;
    type Error = io::Error;

    fn encode(&mut self, item: ReplicationRequest, dst: &mut BytesMut) -> Result<(), io::Error> {
        dst.reserve(13);
        dst.put_u32_le(13);
        dst.put_u8(0);
        dst.put_u64_le(item.starting_offset);
        Ok(())
    }
}

impl Decoder for ClientProtocol {
    type Item = ReplicationResponse;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        match decode_header(src) {
            Some((0, buf)) => {
                assert!(!buf.is_empty(), "Empty reply from upstream");
                trace!("Got message set num_bytes={}", buf.len());
                Ok(Some(ReplicationResponse { messages: buf }))
            }
            Some((opcode, _)) => {
                error!("Unknown op code {:X}", opcode);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            }
            None => Ok(None),
        }
    }
}
