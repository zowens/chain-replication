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
///! Replication is a separate streaming multiplex server reserved for replication.
///!
///! ## Replication Requests
///!
///! ReplicationRequest = Length RequestId Request
///!     Length : u32 = <length of entire request (including headers)>
///!     RequestId : u64
///!     Request : StartReplicationRequest
///!
///! TODO: allow no offset set (beginning of the log!)
///! StartReplicationRequest = Opcode Offset
///!     OpCode : u8 = 0
///!     Offset : u64
///!
///! ## Replication Response
///!
///! ReplicationResponse = Length RequestId Response
///!     Length : u32 = length of entire response (including headers)
///!     RequestId : u64
///!     Response : StartReplication | ReplicateMessages | FinishReplication | ErrorResponse
///!
///! StartReplication = OpCode
///!     OpCode : u8 = 0
///!
///! ReplicateMessages = OpCode MessageBuf
///!     OpCode : u8 = 1
///!     MessageBuf : Message*
///!
///! FinishReplication = OpCode
///!     OpCode : u8 = 2
///!
///! ErrorResponse = OpCode
///!     OpCode : u8 = 255
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

use tokio_proto::streaming::multiplex::*;
use tokio_core::io::{Codec, EasyBuf};
use byteorder::{LittleEndian, ByteOrder};
use commitlog::Offset;
use commitlog::message::MessageSet;

use asynclog::Messages;

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

    // drain to the length and request ID (not used yet), then remove the length field
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
fn encode_header(reqid: ReqId, opcode: OpCode, rest: usize, buf: &mut Vec<u8>) {
    let mut wbuf = [0u8; 13];
    LittleEndian::write_u32(&mut wbuf[0..4], 13 + rest as u32);
    LittleEndian::write_u64(&mut wbuf[4..12], reqid);
    wbuf[12] = opcode;
    buf.extend_from_slice(&wbuf);
}


pub enum ReplicationRequestHeaders {
    StartFrom(u64),
}

pub enum ReplicationResponseHeaders {
    Replicate,
}

pub type RequestFrame = Frame<ReplicationRequestHeaders, (), io::Error>;
pub type ResponseFrame<T> = Frame<ReplicationResponseHeaders, T, io::Error>;

#[derive(Default)]
pub struct ReplicationServerProtocol;
impl Codec for ReplicationServerProtocol {
    /// The type of decoded frames.
    type In = RequestFrame;

    /// The type of frames to be encoded.
    type Out = ResponseFrame<Messages>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match start_decode(buf) {
            Some((reqid, 0, buf)) => {
                if probably_not!(buf.len() < 8) {
                    return Err(io::Error::new(io::ErrorKind::Other, "Invalid length"));
                }

                // read the offset
                let starting_off = LittleEndian::read_u64(buf.as_slice());
                Ok(Some(Frame::Message {
                    id: reqid,
                    message: ReplicationRequestHeaders::StartFrom(starting_off),
                    body: false,
                    solo: false,
                }))
            }
            Some((reqid, opcode, _)) => {
                error!("Unknown op code {:X}, reqId={}", opcode, reqid);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            }
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            // StartReplication
            Frame::Message { id, .. } => {
                encode_header(id, 0u8, 0usize, buf);
            }
            // ReplicateMessages
            Frame::Body { id, chunk: Some(messages) } => {
                encode_header(id, 1u8, messages.bytes().len(), buf);
                buf.extend_from_slice(messages.bytes());
            }
            // FinishReplication
            Frame::Body { id, chunk: None } => {
                encode_header(id, 2u8, 0usize, buf);
            }
            // ErrorResponse
            Frame::Error { id, .. } => {
                encode_header(id, 255u8, 0usize, buf);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct ReplicationClientProtocol;

impl Codec for ReplicationClientProtocol {
    /// The type of decoded frames.
    /// TODO: replace EasyBuf with something else...
    type In = ResponseFrame<EasyBuf>;

    /// The type of frames to be encoded.
    type Out = RequestFrame;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match start_decode(buf) {
            // StartReplication
            Some((reqid, 0, _)) => {
                Ok(Some(Frame::Message {
                    id: reqid,
                    message: ReplicationResponseHeaders::Replicate,
                    solo: false,
                    body: true,
                }))
            }
            // ReplicateMessages
            Some((reqid, 1, buf)) => {
                Ok(Some(Frame::Body {
                    id: reqid,
                    chunk: Some(buf),
                }))
            }
            // FinishReplication
            Some((reqid, 2, _)) => {
                Ok(Some(Frame::Body {
                    id: reqid,
                    chunk: None,
                }))
            }
            // ErrorResponse
            Some((reqid, 255, _)) => {
                Ok(Some(Frame::Error {
                    id: reqid,
                    error: io::Error::new(io::ErrorKind::Other, "Received error from remote"),
                }))
            }
            Some((reqid, opcode, _)) => {
                error!("Unknown op code {:X}, reqId={}", opcode, reqid);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            }
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            // start replication request
            Frame::Message { id, message: ReplicationRequestHeaders::StartFrom(off), .. } => {
                let mut wbuf = [0u8; 21];
                LittleEndian::write_u32(&mut wbuf[0..4], 21);
                LittleEndian::write_u64(&mut wbuf[4..12], id);
                LittleEndian::write_u64(&mut wbuf[13..21], off);
                buf.extend_from_slice(&wbuf);
            }
            _ => {
                unreachable!("Unknown frame type");
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
    Messages(Messages),
}

impl From<Offset> for Res {
    fn from(other: Offset) -> Res {
        Res::Offset(other)
    }
}

impl From<Messages> for Res {
    fn from(other: Messages) -> Res {
        Res::Messages(other)
    }
}

/// Custom protocol, since most of the libraries out there are not zero-copy :(
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
        }

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use tokio_core::io::{Codec, EasyBuf};
    use tokio_proto::streaming::multiplex::Frame;
    use super::*;
    use commitlog::{Message, MessageSet, MessageBuf};
    use asynclog::*;

    #[test]
    fn encode_decode_request() {
        let mut server_codec = ReplicationServerProtocol;
        let mut client_codec = ReplicationClientProtocol;

        // encode the client request
        let mut bytes = Vec::new();
        client_codec.encode(Frame::Message {
                        id: 123456789u64,
                        message: ReplicationRequestHeaders::StartFrom(999u64),
                        solo: false,
                        body: false,
                    },
                    &mut bytes)
            .unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = server_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Message { id, message: ReplicationRequestHeaders::StartFrom(offset), .. } => {
                assert_eq!(id, 123456789u64);
                assert_eq!(offset, 999u64);
            }
            _ => panic!("Expected message"),
        }
    }

    #[test]
    fn encode_decode_start_replication() {
        let mut server_codec = ReplicationServerProtocol;
        let mut client_codec = ReplicationClientProtocol;

        // encode the client request
        let mut bytes = Vec::new();
        server_codec.encode(Frame::Message {
                        id: 123456789u64,
                        message: ReplicationResponseHeaders::Replicate,
                        solo: false,
                        body: false,
                    },
                    &mut bytes)
            .unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = client_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Message { id, message: ReplicationResponseHeaders::Replicate, body, solo } => {
                assert_eq!(id, 123456789u64);
                assert!(body);
                assert!(!solo);
            }
            _ => panic!("Expected message"),
        }
    }

    #[test]
    fn encode_decode_replicate_messages() {
        let mut server_codec = ReplicationServerProtocol;
        let mut client_codec = ReplicationClientProtocol;

        // encode the client request
        let mut bytes = Vec::new();

        let mut msg_set_bytes = Vec::new();
        Message::serialize(&mut msg_set_bytes, 10, b"1234567");
        let msg_set = msg_set_bytes.clone();
        let msg_set = MessageBuf::from_bytes(msg_set).unwrap();
        server_codec.encode(Frame::Body {
                        id: 123456789u64,
                        chunk: Some(Messages::new(msg_set)),
                    },
                    &mut bytes)
            .unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = client_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Body { id, chunk: Some(buf) } => {
                assert_eq!(id, 123456789u64);
                assert_eq!(buf.as_slice(), msg_set_bytes.as_slice());
            }
            _ => panic!("Expected Body"),
        }
    }

    #[test]
    fn encode_decode_finish_replication() {
        let mut server_codec = ReplicationServerProtocol;
        let mut client_codec = ReplicationClientProtocol;

        // encode the client request
        let mut bytes = Vec::new();

        server_codec.encode(Frame::Body {
                        id: 123456789u64,
                        chunk: None,
                    },
                    &mut bytes)
            .unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = client_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Body { id, chunk: None } => {
                assert_eq!(id, 123456789u64);
            }
            _ => panic!("Expected Body"),
        }
    }

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
        codec.encode((12345u64, Res::Offset(Offset(9876543210u64))), &mut vec).unwrap();
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

        codec.encode((12345u64, Res::Messages(Messages::new(msg_set))),
                    &mut output)
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
