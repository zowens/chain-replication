/// Custom protocol
///
/// u32, u64 = <Little Endedian, Unsigned>
///
/// Request = Length RequestId Request
///     Length : u32 = <length of entire request (including headers)>
///     RequestId : u64
///     Request : StartReplicationRequest
///
/// StartReplicationRequest = Opcode Offset
///     OpCode : u8 = 0
///     Offset : u64
///
/// -----------------
///
/// Response = Length RequestId Response
///     Length : u32 = length of entire response (including headers)
///     RequestId : u64
///     Response : StartReplication | ReplicateMessages | FinishReplication | ErrorResponse
///
/// StartReplication = OpCode
///     OpCode : u8 = 0
///
/// ReplicateMessages = OpCode MessageBuf
///     OpCode : u8 = 1
///     MessageBuf : Message*
///
/// FinishReplication = OpCode
///     OpCode : u8 = 2
///
/// ErrorResponse = OpCode
///     OpCode : u8 = 255
///
/// Message = Offset PayloadSize Hash Payload
///     Offset : u64
///     PayloadSize : u32
///     Hash : u64 = seahash of payload
///     Payload : u8* = <bytes up to PayloadSize>
///
use std::io;
use std::intrinsics::unlikely;
use byteorder::{LittleEndian, ByteOrder};
use tokio_proto::streaming::multiplex::*;
use tokio_core::io::{Codec, EasyBuf};
use super::asynclog::Messages;
use commitlog::MessageSet;
use super::proto::*;

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}


pub enum ReplicationRequestHeaders {
    StartFrom(u64),
}

pub enum ReplicationResponseHeaders {
    Replicate,
}

pub type RequestFrame = Frame<ReplicationRequestHeaders, (), io::Error>;
pub type ResponseFrame = Frame<ReplicationResponseHeaders, Messages, io::Error>;

/*
 * pub enum Frame<T, B, E> {
    Message {
        id: RequestId,
        message: T,
        body: bool,
        solo: bool,
    },
    Body {
        id: RequestId,
        chunk: Option<B>,
    },
    Error {
        id: RequestId,
        error: E,
    },
}*/

#[derive(Default)]
pub struct ReplicationServerProtocol;
impl Codec for ReplicationServerProtocol {
    /// The type of decoded frames.
    type In = RequestFrame;

    /// The type of frames to be encoded.
    type Out = ResponseFrame;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match start_decode(buf) {
            Some((reqid, 0, buf)) => {
                if probably_not!(buf.len() < 8) {
                    return Err(io::Error::new(io::ErrorKind::Other, "Invalid length"));
                }

                // read the offset
                let starting_off = LittleEndian::read_u64(&buf.as_slice());
                Ok(Some(Frame::Message {
                    id: reqid,
                    message: ReplicationRequestHeaders::StartFrom(starting_off),
                    body: false,
                    solo: false,
                }))
            },
            Some((reqid, opcode, _)) => {
                error!("Unknown op code {:X}, reqId={}", opcode, reqid);
                Err(io::Error::new(io::ErrorKind::Other, "Unknown opcode"))
            },
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            // StartReplication
            Frame::Message { id, .. } => {
                encode_header(id, 0u8, 0usize, buf);
            },
            // ReplicateMessages
            Frame::Body { id, chunk: Some(messages) } => {
                encode_header(id, 1u8, messages.bytes().len(), buf);
                buf.extend_from_slice(messages.bytes());
            },
            // FinishReplication
            Frame::Body { id, chunk: None } => {
                encode_header(id, 2u8, 0usize, buf);
            },
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
    type In = ResponseFrame;

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
            },
            // ReplicateMessages
            Some((reqid, 1, buf)) => {
                Ok(Some(Frame::Body {
                    id: reqid,
                    chunk: Some(Messages::from_easybuf(buf)),
                }))
            },
            // FinishReplication
            Some((reqid, 2, _)) => {
                Ok(Some(Frame::Body {
                    id: reqid,
                    chunk: None,
                }))
            },
            // ErrorResponse
            Some((reqid, 255, _)) => {
                Ok(Some(Frame::Error {
                    id: reqid,
                    error: io::Error::new(io::ErrorKind::Other, "Received error from remote"),
                }))
            },
            _ => unreachable!(),
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
            },
            _ => {
                unreachable!("Unknown frame type");
            }
        }
        Ok(())
    }
}

/*
 * pub trait ServerProto<T: 'static>: 'static {
    type Request: 'static;
    type RequestBody: 'static;
    type Response: 'static;
    type ResponseBody: 'static;
    type Error: From<Error> + 'static;
    type Transport: Transport<Self::RequestBody,
        Item=Frame<Self::Request, Self::RequestBody, Self::Error>,
        SinkItem=Frame<Self::Response, Self::ResponseBody, Self::Error>>;
    type BindTransport: IntoFuture<Item=Self::Transport, Error=Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport;
}*/

#[cfg(test)]
mod tests {
    use tokio_core::io::{Codec, EasyBuf};
    use tokio_proto::streaming::multiplex::Frame;
    use super::*;
    use super::super::asynclog::Messages;
    use commitlog::MessageSet;

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
        }, &mut bytes).unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = server_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Message { id, message: ReplicationRequestHeaders::StartFrom(offset), .. } => {
                assert_eq!(id, 123456789u64);
                assert_eq!(offset, 999u64);
            },
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
        }, &mut bytes).unwrap();

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
            },
            _ => panic!("Expected message"),
        }
    }

    #[test]
    fn encode_decode_replicate_messages() {
        let mut server_codec = ReplicationServerProtocol;
        let mut client_codec = ReplicationClientProtocol;

        // encode the client request
        let mut bytes = Vec::new();

        let replicamsgs = b"0123456789".to_vec().into();
        server_codec.encode(Frame::Body {
            id: 123456789u64,
            chunk: Some(Messages::from_easybuf(replicamsgs)),
        }, &mut bytes).unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = client_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Body { id, chunk: Some(buf) } => {
                assert_eq!(id, 123456789u64);
                assert_eq!(buf.bytes(), b"0123456789");
            },
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
        }, &mut bytes).unwrap();

        // push in some extra garbage
        bytes.extend_from_slice(b"foo");

        // decode it
        let mut bytes: EasyBuf = bytes.into();
        let val = client_codec.decode(&mut bytes).unwrap().unwrap();
        match val {
            Frame::Body { id, chunk: None } => {
                assert_eq!(id, 123456789u64);
            },
            _ => panic!("Expected Body"),
        }
    }
}
