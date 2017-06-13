///! Custom protocol
///!
///! # Common Data Structures
///!
///! u32, u64 = <Little Endedian, Unsigned>
///! Message = Offset PayloadSize Hash Payload
///!     Offset : u64
///!     Payload Size : u32
///!     Hash : u64 = seahash of payload
///!     Payload : u8* = <bytes up to Payload Size>
///!
///! # Replicaiton
///!
///! Replication is a separate pipelined protocol reserved for replication.
///!
///! ## Replication Requests
///!
///! ReplicationRequest = Length Request
///!     Length : u32 = <length of entire request (including headers)>
///!     Request : ReplicateMessagesRequest
///!
///! ReplicateMessagesRequest = Opcode Offset
///!     OpCode : u8 = 0
///!     Offset : u64 = <Inclusive offset of the first entry to pull>
///!
///! ## Replication Response
///!
///! ReplicationResponse = Length Response
///!     Length : u32 = <length of entire response (including headers)>
///!     Response : ReplicateMessages | ErrorResponse
///!
///! ReplicateMessages = OpCode MessageBuf
///!     OpCode : u8 = 0
///!     MessageBuf : Message*
///!
///! ErrorResponse = OpCode
///!     OpCode : u8 = 255
///!     Message : u8* = <error message in UTF-8>
///!
///! # Client API
///!
///! The client API is a multiplexing protocol.
///!
///! ## Client Requests
///!
///! Request = Length RequestId Request
///!     Length : u32 = <length of entire request (including headers)>
///!     RequestId : u64
///!     Request : AppendRequest | ReadLastOffsetRequest | ReadFromOffsetRequest | RequestTailReply
///!
///! AppendRequest = OpCode Payload
///!     OpCode      : u8 = 0
///!     ClientId    : u32
///!     ClientReqId : u32
///!     Payload     : u8* = <bytes up to length>
///!
///! ReadLastOffsetRequest = OpCode
///!     OpCode : u8 = 1
///!
///! ReadFromOffsetRequest = OpCode Offset
///!     OpCode : u8 = 2
///!     Offset : u64
///!
///! RequestTailReply = OpCode ClientId LastKnownOffset
///!     OpCode : u8 = 3
///!     ClientId : u32
///!     LastKnownOffset : u64
///!
///! ## Client Response
///!
///! Response = Length RequestId Response
///!     Length : u32 = length of entire response (including headers)
///!     RequestId : u64
///!     Response : OffsetResponse | MessagesResponse | AckResponse
///!
///! OffsetResponse = OpCode Offset
///!     OpCode : u8 = 0
///!     Offset : u64
///!
///! MessagesResponse = OpCode MessageBuf
///!     OpCode : u8 = 1
///!     MessageBuf : Message*
///!
///! EmptyOffsetResponse = OpCode
///!     OpCode : u8 = 2
///!
///! AckResponse = OpCode
///!     OpCode : u8 = 3
///!
///! TailReplyStarted = OpCode
///!     OpCode : u8 = 4
///!
///! # Server Streams
///!
///! AppendedMessages = OpCode Offset ClientReqId*
///!     OpCode : u8 = 5
///!     ClientReqId : u32
///!     Offset : u64
///!
///! StreamComplete = OpCode
///!     OpCode : u8 = 6
///!
///! Message = Offset PayloadSize Hash Payload
///!     Offset : u64
///!     PayloadSize : u32
///!     Hash : u64 = seahash of payload
///!     Payload : u8* = <bytes up to PayloadSize>

use std::io;
use std::intrinsics::unlikely;

use tokio_proto::streaming::multiplex::Frame;
use tokio_io::codec::{Encoder, Decoder};
use bytes::{LittleEndian, IntoBuf, Buf, BytesMut, BufMut};
use byteorder::ByteOrder;
use commitlog::Offset;
use commitlog::message::{MessageSet, MessageBuf};

use asynclog::ClientAppendSet;

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}

type ReqId = u64;
type OpCode = u8;
type ClientId = u32;
type ClientReqId = u32;

#[inline]
fn decode_header(buf: &mut BytesMut) -> Option<(ReqId, OpCode, BytesMut)> {
    trace!("Found {} chars in read buffer", buf.len());
    // must have at least 13 bytes
    if probably_not!(buf.len() < 13) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }


    // read the length of the message
    let len = LittleEndian::read_u32(&buf[0..4]) as usize;

    // ensure we have enough
    if probably_not!(buf.len() < len) {
        buf.reserve(len);
        return None;
    }

    // drain to the length and request ID, then remove the length field
    let mut buf = buf.split_to(len);
    let mut header = buf.split_to(13).into_buf();
    // skip the length field
    header.advance(4);
    let reqid = header.get_u64::<LittleEndian>();
    let op = header.get_u8();
    Some((reqid, op, buf))
}

#[inline]
fn decode_header_pipelined(buf: &mut BytesMut) -> Option<(OpCode, BytesMut)> {
    trace!("Found {} chars in read buffer", buf.len());
    // must have at least 5 bytes
    buf.reserve(5);
    if probably_not!(buf.len() < 5) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }

    let len = LittleEndian::read_u32(&buf[0..4]) as usize;

    // ensure we have enough
    if probably_not!(buf.len() < len) {
        buf.reserve(len);
        return None;
    }

    // drain to the length and opcode, then remove the length field
    let mut buf = buf.split_to(len);
    trace!("Drained len={}", buf.len());

    // parse by op code, remove length field
    let op = buf.split_to(5)[4];
    trace!("OpCode={}, len={}", op, buf.len());
    Some((op, buf))
}

#[inline]
fn encode_header(reqid: ReqId, opcode: OpCode, rest: usize, buf: &mut BytesMut) {
    buf.reserve(13 + rest);
    buf.put_u32::<LittleEndian>(13 + rest as u32);
    buf.put_u64::<LittleEndian>(reqid);
    buf.put_u8(opcode);
}


pub enum ReplicationRequest {
    StartFrom(u64),
}

pub enum ReplicationResponse {
    Messages(BytesMut),
    // TODO: error case
}

pub struct ReplicationResponseHeader(pub usize);

#[derive(Default)]
pub struct ReplicationServerProtocol;

impl Encoder for ReplicationServerProtocol {
    type Item = ReplicationResponseHeader;
    type Error = io::Error;

    fn encode(
        &mut self,
        item: ReplicationResponseHeader,
        dst: &mut BytesMut,
    ) -> Result<(), io::Error> {
        dst.put_u32::<LittleEndian>(5 + item.0 as u32);
        dst.put_u8(0);
        Ok(())
    }
}

impl Decoder for ReplicationServerProtocol {
    type Item = ReplicationRequest;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        match decode_header_pipelined(src) {
            Some((0, buf)) => {
                if probably_not!(buf.len() < 8) {
                    return Err(io::Error::new(io::ErrorKind::Other, "Invalid length"));
                }

                // read the offset
                let offset = buf.into_buf().get_u64::<LittleEndian>();
                Ok(Some(ReplicationRequest::StartFrom(offset)))
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
pub struct ReplicationClientProtocol;

impl Encoder for ReplicationClientProtocol {
    type Item = ReplicationRequest;
    type Error = io::Error;

    fn encode(&mut self, item: ReplicationRequest, dst: &mut BytesMut) -> Result<(), io::Error> {
        trace!("Starting encode of client replication protocol");
        match item {
            ReplicationRequest::StartFrom(offset) => {
                dst.reserve(13);
                dst.put_u32::<LittleEndian>(13);
                dst.put_u8(0);
                dst.put_u64::<LittleEndian>(offset);
            }
        }
        trace!("End encode");
        Ok(())
    }
}

impl Decoder for ReplicationClientProtocol {
    type Item = ReplicationResponse;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        match decode_header_pipelined(src) {
            Some((0, buf)) => {
                assert!(!buf.is_empty(), "Empty reply from upstream");
                trace!("Got message set num_bytes={}", buf.len());
                Ok(Some(ReplicationResponse::Messages(buf)))
            }
            Some((opcode, _)) => {
                error!("Unknown op code {:X}", opcode);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            }
            None => Ok(None),
        }
    }
}

// TODO: switch to struct enum for other variants
pub enum Req {
    /// Append message with bytes
    ///
    /// First 4 bytes are client_id, next 4 bytes are client_req_id,
    /// rest is the message payload.
    ///
    /// This encoding facilitates efficient append to the log.
    Append(BytesMut),
    Read(u64),
    LastOffset,
    RequestTailReply {
        client_id: ClientId,
        // TODO: optional?
        last_known_offset: Offset,
    },
}

pub enum Res {
    Offset(Offset),
    Messages(MessageBuf),
    EmptyOffset,
    Ack,
    TailReplyStarted,
}

impl From<Offset> for Res {
    fn from(other: Offset) -> Res {
        Res::Offset(other)
    }
}

impl From<MessageBuf> for Res {
    fn from(other: MessageBuf) -> Res {
        Res::Messages(other)
    }
}

impl From<Option<Offset>> for Res {
    fn from(other: Option<Offset>) -> Res {
        other.map(Res::Offset).unwrap_or(Res::EmptyOffset)
    }
}

pub enum ResChunk {
    AppendedMessages {
        offset: Offset,
        client_reqs: Vec<ClientReqId>,
    },
}

impl From<ClientAppendSet> for ResChunk {
    fn from(append_set: ClientAppendSet) -> ResChunk {
        ResChunk::AppendedMessages {
            offset: append_set.latest_offset,
            client_reqs: append_set.client_req_ids,
        }
    }
}


/// Custom protocol for the frontend
#[derive(Default)]
pub struct Protocol;

impl Protocol {
    fn encode_respone(reqid: ReqId, res: Res, dst: &mut BytesMut) {
        match res {
            Res::Offset(off) => {
                encode_header(reqid, 0, 8, dst);
                dst.put_u64::<LittleEndian>(off);
            }
            Res::Messages(ms) => {
                trace!("Writing messages len={}", ms.bytes().len());
                encode_header(reqid, 1, ms.bytes().len(), dst);
                dst.put_slice(ms.bytes());
            }
            Res::EmptyOffset => {
                encode_header(reqid, 2, 0, dst);
            }
            Res::Ack => {
                encode_header(reqid, 3, 0, dst);
            }
            Res::TailReplyStarted => {
                encode_header(reqid, 4, 0, dst);
            }
        }
    }

    fn encode_chunk(reqid: ReqId, chunk: ResChunk, dst: &mut BytesMut) {
        match chunk {
            ResChunk::AppendedMessages {
                offset,
                client_reqs,
            } => {
                encode_header(reqid, 5, 8 + (4 * client_reqs.len()), dst);
                dst.put_u64::<LittleEndian>(offset);
                for cli_req_id in client_reqs {
                    dst.put_u32::<LittleEndian>(cli_req_id);
                }
            }
        }
    }
}

impl Encoder for Protocol {
    type Item = Frame<Res, ResChunk, io::Error>;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), io::Error> {
        match item {
            Frame::Message { id, message, .. } => {
                trace!("Adding message id={}", id);
                Protocol::encode_respone(id, message, dst);
            }
            Frame::Body { id, chunk: Some(v) } => {
                trace!("Encoding response body for id={}", id);
                Protocol::encode_chunk(id, v, dst);
            }
            Frame::Body { id, chunk: None } => {
                trace!("No chunk data for Request ID={}", id);
                encode_header(id, 6, 0, dst);
            }
            Frame::Error { id, error } => {
                error!("Request ID {} generated an error: {}", id, error);
                // TODO: send error to client instead!
                return Err(error);
            }
        };

        Ok(())
    }
}

impl Decoder for Protocol {
    type Item = Frame<Req, (), io::Error>;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        let (req_id, op, buf) = match decode_header(src) {
            Some(v) => v,
            None => return Ok(None),
        };

        let req = match op {
            0 => Req::Append(buf),
            1 => Req::LastOffset,
            2 => {
                // parse out the starting offset
                if probably_not!(buf.len() < 8) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Offset not specified for read query",
                    ));
                }
                let starting_off = LittleEndian::read_u64(&buf[0..8]);
                Req::Read(starting_off)
            }
            3 => {
                if probably_not!(buf.len() < 12) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Last Known Offset and ClientID not specified",
                    ));
                }
                let client_id = LittleEndian::read_u32(&buf[0..4]);
                let last_known_offset = LittleEndian::read_u64(&buf[4..12]);
                Req::RequestTailReply {
                    client_id,
                    last_known_offset,
                }
            }
            op => {
                error!("Invalid operation op={:X}", op);
                return Err(io::Error::new(io::ErrorKind::Other, "Invalid operation"));
            }
        };

        Ok(Some(Frame::Message {
            id: req_id,
            message: req,
            body: false,
            solo: false,
        }))
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        trace!("DECODE EOF");
        match try!(self.decode(buf)) {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    Err(
                        io::Error::new(io::ErrorKind::Other, "bytes remaining on stream").into(),
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commitlog::message::{Message, MessageBuf};

    macro_rules! op {
        ($code: expr, $rid: expr, $payload: expr) => ({
            let mut v = Vec::new();
            let p = $payload;

            let mut size = [0u8; 4];
            LittleEndian::write_u32(&mut size, (13 + p.len()) as u32);
            v.extend(&size);

            let mut requestid = [0u8; 8];
            LittleEndian::write_u64(&mut requestid, $rid);
            v.extend(&requestid);


            v.push($code);
            v.extend(p);
            v.into()
        })
    }


    #[test]
    pub fn decode_append_request() {
        let mut codec = Protocol;
        let mut buf = op!(0u8, 123456u64, b"foobarbaz");
        match codec.decode(&mut buf) {
            Ok(Some((123456u64, Req::Append(buf)))) => {
                assert_eq!(b"foobarbaz", &buf[..]);
            }
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn decode_last_offset_request() {
        let mut codec = Protocol;
        let mut buf = op!(1u8, 123456u64, b"...extra ignored params...");
        match codec.decode(&mut buf) {
            Ok(Some((123456u64, Req::LastOffset))) => {}
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn decode_read_by_offset() {
        let mut codec = Protocol;

        let mut offset = [0u8; 8];
        LittleEndian::write_u64(&mut offset, 12345u64);
        let mut buf = op!(2u8, 54321u64, &offset);
        match codec.decode(&mut buf) {
            Ok(Some((54321u64, Req::Read(off)))) => {
                assert_eq!(12345u64, off);
            }
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn encode_offset() {
        let mut codec = Protocol;

        let mut vec = BytesMut::with_capacity(1024);
        codec
            .encode((12345u64, Res::Offset(9876543210u64)), &mut vec)
            .unwrap();
        assert_eq!(21, vec.len());
        assert_eq!(21, LittleEndian::read_u32(&vec[0..4]));
        assert_eq!(12345u64, LittleEndian::read_u64(&vec[4..12]));
        assert_eq!(0u8, vec[12]);
        assert_eq!(9876543210u64, LittleEndian::read_u64(&vec[13..]));
    }

    #[test]
    pub fn encode_messages() {
        let mut codec = Protocol;

        let mut msg_set_bytes = Vec::with_capacity(1024);
        Message::serialize(&mut msg_set_bytes, 10, b"1234567");
        Message::serialize(&mut msg_set_bytes, 11, b"abc");
        Message::serialize(&mut msg_set_bytes, 12, b"foobarbaz");

        let msg_set = msg_set_bytes.clone();
        let msg_set = MessageBuf::from_bytes(msg_set).unwrap();

        let mut output = BytesMut::with_capacity(1028);
        let extras = b"some_extra_crap";
        output.extend(extras);

        codec
            .encode((12345u64, Res::Messages(msg_set)), &mut output)
            .unwrap();
        assert_eq!(extras.len() + msg_set_bytes.len() + 13, output.len());

        let msg_slice = &output[extras.len()..];
        let res_size = LittleEndian::read_u32(&msg_slice[0..4]);
        assert_eq!(res_size as usize, msg_slice.len());
        let req_id = LittleEndian::read_u64(&msg_slice[4..12]);
        assert_eq!(12345u64, req_id);

        // op code
        assert_eq!(1u8, msg_slice[12]);

        assert_eq!(&msg_set_bytes[..], &msg_slice[13..]);
    }
}
