use std::io;
use std::intrinsics::unlikely;
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder, Framed};
use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use tokio_core::net::TcpStream;
use tokio_proto::streaming::multiplex::{ClientProto, Frame, RequestId};
use tokio_proto::streaming::{Message, Body};
use tokio_proto::util::client_proxy::ClientProxy;
use byteorder::{ByteOrder, LittleEndian};
use futures::stream;
use msgs::Messages;

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}

pub type EmptyStream = stream::Empty<(), io::Error>;
pub type RequestMsg = Message<Request, EmptyStream>;
pub type ResponseMsg = Message<Response, Body<ReplyResponse, io::Error>>;
pub type ProtoConnection = ClientProxy<RequestMsg, ResponseMsg, io::Error>;
pub type ReplyStream = Body<ReplyResponse, io::Error>;

pub enum Request {
    Append {
        client_id: u32,
        client_req_id: u32,
        payload: Vec<u8>,
    },
    AppendBytes {
        client_id: u32,
        client_req_id: u32,
        payload: BytesMut,
    },
    Read { starting_offset: u64 },
    LatestOffset,
    RequestTailReply {
        client_id: u32,
        last_known_offset: u64,
    },
}

pub enum Response {
    Offset(Option<u64>),
    Messages(Messages),
    ACK,
    TailReplyStarted,
}

pub enum ReplyResponse {
    AppendedMessages { offset: u64, client_reqs: Vec<u32> },
}

#[inline]
fn decode_header(buf: &mut BytesMut) -> Option<(RequestId, u8, BytesMut)> {
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
        return None;
    }

    // drain to the length and request ID, then remove the length field
    let mut buf = buf.split_to(len);
    let header = buf.split_to(13);
    let reqid = LittleEndian::read_u64(&header[4..12]);
    let op = header[12];
    Some((reqid, op, buf))
}

#[inline]
fn encode_header(reqid: RequestId, opcode: u8, rest: usize, buf: &mut BytesMut) {
    buf.put_u32::<LittleEndian>(13 + rest as u32);
    buf.put_u64::<LittleEndian>(reqid);
    buf.put_u8(opcode);
}


pub struct Protocol;
impl Decoder for Protocol {
    type Item = Frame<Response, ReplyResponse, io::Error>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        let (req_id, op, buf) = match decode_header(src) {
            Some(v) => v,
            None => return Ok(None),
        };

        let res = match op {
            0 => {
                // TODO: ensure correct length
                let off = buf.into_buf().get_u64::<LittleEndian>();
                trace!("OffsetResponse offset={}", off);
                Response::Offset(Some(off))
            }
            1 => {
                trace!("messages response");
                Response::Messages(Messages::new(buf))
            }
            2 => Response::Offset(None),
            3 => {
                trace!("ACK");
                Response::ACK
            }
            4 => {
                trace!("Tail reply started");
                return Ok(Some(Frame::Message {
                    id: req_id,
                    message: Response::TailReplyStarted,
                    body: true,
                    solo: false,
                }));
            }
            5 => {
                trace!("Received trail appended");
                let mut buf = buf.into_buf();
                let offset = buf.get_u64::<LittleEndian>();
                trace!("Remaining = {}", buf.remaining());

                let mut reqs = Vec::with_capacity(buf.remaining() / 4);
                while buf.remaining() >= 4 {
                    reqs.push(buf.get_u32::<LittleEndian>());
                }
                return Ok(Some(Frame::Body {
                    id: req_id,
                    chunk: Some(ReplyResponse::AppendedMessages {
                        offset,
                        client_reqs: reqs,
                    }),
                }));
            }
            op => {
                error!("Unknown response code {}", op);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid operation",
                ));
            }
        };
        Ok(Some(Frame::Message {
            id: req_id,
            message: res,
            body: false,
            solo: false,
        }))
    }
}

impl Encoder for Protocol {
    type Item = Frame<Request, (), io::Error>;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), io::Error> {
        let (reqid, req) = match item {
            Frame::Message { id, message, .. } => {
                trace!("Adding message id={}", id);
                (id, message)
            }
            Frame::Body { id, .. } => {
                warn!("Encountered body for request id={}", id);
                return Ok(());
            }
            Frame::Error { id, error } => {
                error!("Request ID {} generated an error: {}", id, error);
                // TODO: send error to client instead!
                return Err(error);
            }
        };
        match req {
            Request::LatestOffset => {
                encode_header(reqid, 1, 0, dst);
            }
            Request::Append {
                client_id,
                client_req_id,
                payload,
            } => {
                encode_header(reqid, 0, 8 + payload.len(), dst);
                dst.put_u32::<LittleEndian>(client_id);
                dst.put_u32::<LittleEndian>(client_req_id);
                dst.put_slice(&payload);
            }
            Request::AppendBytes {
                client_id,
                client_req_id,
                payload,
            } => {
                encode_header(reqid, 0, 8 + payload.len(), dst);
                dst.put_u32::<LittleEndian>(client_id);
                dst.put_u32::<LittleEndian>(client_req_id);
                dst.extend(&payload);
            }
            Request::Read { starting_offset } => {
                encode_header(reqid, 2, 8, dst);
                dst.put_u64::<LittleEndian>(starting_offset);
            }
            Request::RequestTailReply {
                client_id,
                last_known_offset,
            } => {
                encode_header(reqid, 3, 12, dst);
                dst.put_u32::<LittleEndian>(client_id);
                dst.put_u64::<LittleEndian>(last_known_offset);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct LogServerProto;
impl ClientProto<TcpStream> for LogServerProto {
    type Request = Request;
    type RequestBody = ();
    type Response = Response;
    type ResponseBody = ReplyResponse;
    type Error = io::Error;
    type Transport = Framed<TcpStream, Protocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        trace!("Bind transport");
        try!(io.set_nodelay(true));
        trace!("Setting up protocol");
        Ok(io.framed(Protocol))
    }
}
