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
///!     Request : AppendRequest | ReadLastOffsetRequest | ReadFromOffsetRequest
///!
///! AppendRequest = OpCode Payload
///!     OpCode : u8 = 0
///!     Payload : u8* = <bytes up to length>
///!
///! ReadLastOffsetRequest = OpCode
///!     OpCode : u8 = 1
///!
///! ReadFromOffsetRequest = OpCode Offset
///!     OpCode : u8 = 2
///!     Offset : u64
///!
///! ## Client Response
///!
///! Response = Length RequestId Response
///!     Length : u32 = length of entire response (including headers)
///!     RequestId : u64
///!     Response : OffsetResponse | MessagesResponse
///!
///! OffsetResponse = OpCode Offset
///!     OpCode : u8 = 0
///!     Offset : u64
///!
///! EmptyOffsetResponse = OpCode
///!     OpCode : u8 = 2
///!
///! MessagesResponse = OpCode MessageBuf
///!     OpCode : u8 = 1
///!     MessageBuf : Message*
///!
///! Message = Offset PayloadSize Hash Payload
///!     Offset : u64
///!     PayloadSize : u32
///!     Hash : u64 = seahash of payload
///!     Payload : u8* = <bytes up to PayloadSize>

use std::io;
use std::intrinsics::unlikely;

use tokio_proto::multiplex::RequestId;
use tokio_core::io::{Codec, EasyBuf};
use byteorder::{LittleEndian, ByteOrder};
use commitlog::Offset;
use commitlog::message::{MessageSet, MessageBuf};

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}

type ReqId = u64;
type OpCode = u8;

#[inline]
fn start_decode(buf: &mut EasyBuf) -> Option<(ReqId, OpCode, EasyBuf)> {
    trace!("Found {} chars in read buffer", buf.len());
    // must have at least 13 bytes
    if probably_not!(buf.len() < 13) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }


    // read the length of the message
    let len = {
        let data = buf.as_slice();
        LittleEndian::read_u32(&data[0..4]) as usize
    };

    // ensure we have enough
    if probably_not!(buf.len() < len) {
        return None;
    }

    // drain to the length and request ID, then remove the length field
    let mut buf = buf.drain_to(len);

    let reqid = {
        let len_and_reqid = buf.drain_to(12);
        LittleEndian::read_u64(&len_and_reqid.as_slice()[4..12])
    };

    // parse by op code
    let op = buf.drain_to(1).as_slice()[0];

    Some((reqid, op, buf))
}

#[inline]
fn start_decode_pipelined(buf: &mut EasyBuf) -> Option<(OpCode, EasyBuf)> {
    trace!("Found {} chars in read buffer", buf.len());
    // must have at least 5 bytes
    if probably_not!(buf.len() < 5) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }

    // read the length of the message
    let len = {
        let data = buf.as_slice();
        LittleEndian::read_u32(&data[0..4]) as usize
    };

    // ensure we have enough
    if probably_not!(buf.len() < len) {
        return None;
    }

    // drain to the length and opcode, then remove the length field
    let mut buf = buf.drain_to(len);
    trace!("Drained len={}", buf.len());

    // parse by op code, remove length field
    let op = buf.drain_to(5).as_slice()[4];
    trace!("OpCode={}, len={}", op, buf.len());
    Some((op, buf))
}

#[inline]
fn encode_header(reqid: ReqId, opcode: OpCode, rest: usize, buf: &mut Vec<u8>) {
    let mut wbuf = [0u8; 13];
    LittleEndian::write_u32(&mut wbuf[0..4], 13 + rest as u32);
    LittleEndian::write_u64(&mut wbuf[4..12], reqid);
    wbuf[12] = opcode;
    buf.extend_from_slice(&wbuf);
}

pub enum ReplicationRequest {
    StartFrom(u64),
}

pub enum ReplicationResponse {
    Messages(EasyBuf), 
    // TODO: error case
}

pub struct ReplicationResponseHeader(pub usize);

#[derive(Default)]
pub struct ReplicationServerProtocol;
impl Codec for ReplicationServerProtocol {
    /// The type of decoded frames.
    type In = ReplicationRequest;

    /// The type of frames to be encoded.
    type Out = ReplicationResponseHeader;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match start_decode_pipelined(buf) {
            Some((0, buf)) => {
                if probably_not!(buf.len() < 8) {
                    return Err(io::Error::new(io::ErrorKind::Other, "Invalid length"));
                }

                // read the offset
                let offset = LittleEndian::read_u64(buf.as_slice());
                Ok(Some(ReplicationRequest::StartFrom(offset)))
            }
            Some((opcode, _)) => {
                error!("Unknown op code {:X}", opcode);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            }
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let ReplicationResponseHeader(msg_set_size) = msg;

        let mut wbuf = [0u8; 5];
        LittleEndian::write_u32(&mut wbuf[0..4], 5 + msg_set_size as u32);
        wbuf[4] = 0;
        buf.extend_from_slice(&wbuf);
        Ok(())
    }
}

#[derive(Default)]
pub struct ReplicationClientProtocol;

impl Codec for ReplicationClientProtocol {
    /// The type of decoded frames.
    /// TODO: replace EasyBuf with something else...
    type In = ReplicationResponse;

    /// The type of frames to be encoded.
    type Out = ReplicationRequest;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match start_decode_pipelined(buf) {
            Some((0, buf)) => {
                assert!(buf.len() > 0, "Empty reply from upstream");
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

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            ReplicationRequest::StartFrom(offset) => {
                let mut wbuf = [0u8; 13];
                LittleEndian::write_u32(&mut wbuf[0..4], 13);
                wbuf[4] = 0;
                LittleEndian::write_u64(&mut wbuf[5..13], offset);
                buf.extend_from_slice(&wbuf);
            }
        }
        Ok(())
    }
}

pub enum Req {
    Append(EasyBuf),
    Read(u64),
    LastOffset,
}

pub enum Res {
    Offset(Offset),
    Messages(MessageBuf),
    EmptyOffset,
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

/// Custom protocol for the frontend
///
/// u32, u64 = <Little Endedian, Unsigned>
///
#[derive(Default)]
pub struct Protocol;

impl Codec for Protocol {
    /// The type of decoded frames.
    type In = (RequestId, Req);

    /// The type of frames to be encoded.
    type Out = (RequestId, Res);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match start_decode(buf) {
            Some((reqid, 0, buf)) => Ok(Some((reqid, Req::Append(buf)))),
            Some((reqid, 1, _)) => Ok(Some((reqid, Req::LastOffset))),
            Some((reqid, 2, buf)) => {
                // parse out the starting offset
                if probably_not!(buf.len() < 8) {
                    Err(io::Error::new(io::ErrorKind::Other, "Offset not specified for read query"))
                } else {
                    let data = buf.as_slice();
                    let starting_off = LittleEndian::read_u64(&data[0..8]);
                    Ok(Some((reqid, Req::Read(starting_off))))
                }
            }
            Some((_, op, _)) => {
                error!("Invalid operation op={:X}", op);
                Err(io::Error::new(io::ErrorKind::Other, "Invalid operation"))
            }
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let reqid = msg.0;
        match msg.1 {
            Res::Offset(off) => {
                encode_header(reqid, 0, 8, buf);

                // add the offset
                let mut wbuf = [0u8; 8];
                LittleEndian::write_u64(&mut wbuf, off);
                buf.extend_from_slice(&wbuf);
            }
            Res::Messages(ms) => {
                encode_header(reqid, 1, ms.bytes().len(), buf);
                buf.extend_from_slice(ms.bytes());
            }
            Res::EmptyOffset => {
                encode_header(reqid, 2, 0, buf);
            }
        }

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use tokio_core::io::{Codec, EasyBuf};
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
            v
        })
    }

    fn easy_buf_of(bytes: Vec<u8>) -> EasyBuf {
        let mut buf = EasyBuf::new();

        {
            let mut bufmut = buf.get_mut();
            bufmut.extend_from_slice(bytes.as_ref());
        }

        buf
    }

    #[test]
    pub fn decode_append_request() {
        let mut codec = Protocol;
        let mut buf = easy_buf_of(op!(0u8, 123456u64, b"foobarbaz"));
        match codec.decode(&mut buf) {
            Ok(Some((123456u64, Req::Append(buf)))) => {
                assert_eq!(b"foobarbaz", buf.as_slice());
            }
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn decode_last_offset_request() {
        let mut codec = Protocol;
        let mut buf = easy_buf_of(op!(1u8, 123456u64, b"...extra ignored params..."));
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
        let mut buf = easy_buf_of(op!(2u8, 54321u64, &offset));
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

        let mut vec = Vec::new();
        codec.encode((12345u64, Res::Offset(9876543210u64)), &mut vec).unwrap();
        assert_eq!(21, vec.len());
        assert_eq!(21, LittleEndian::read_u32(&vec[0..4]));
        assert_eq!(12345u64, LittleEndian::read_u64(&vec[4..12]));
        assert_eq!(0u8, vec[12]);
        assert_eq!(9876543210u64, LittleEndian::read_u64(&vec[13..]));
    }

    #[test]
    pub fn encode_messages() {
        let mut codec = Protocol;

        let mut msg_set_bytes = Vec::new();
        Message::serialize(&mut msg_set_bytes, 10, b"1234567");
        Message::serialize(&mut msg_set_bytes, 11, b"abc");
        Message::serialize(&mut msg_set_bytes, 12, b"foobarbaz");

        let msg_set = msg_set_bytes.clone();
        let msg_set = MessageBuf::from_bytes(msg_set).unwrap();

        let mut output = Vec::new();
        let extras = b"some_extra_crap";
        output.extend(extras);

        codec.encode((12345u64, Res::Messages(msg_set)), &mut output)
            .unwrap();
        assert_eq!(extras.len() + msg_set_bytes.len() + 13, output.len());

        let msg_slice = &output[extras.len()..];
        let res_size = LittleEndian::read_u32(&msg_slice[0..4]);
        assert_eq!(res_size as usize, msg_slice.len());
        let req_id = LittleEndian::read_u64(&msg_slice[4..12]);
        assert_eq!(12345u64, req_id);

        // op code
        assert_eq!(1u8, msg_slice[12]);

        assert_eq!(msg_set_bytes.as_slice(), &msg_slice[13..]);
    }
}
