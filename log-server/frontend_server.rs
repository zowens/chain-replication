use std::io;
use std::rc::Rc;
use std::cell::RefCell;
use std::mem;
use std::time::Duration;
use std::intrinsics::unlikely;
use std::net::SocketAddr;
use commitlog::*;
use commitlog::message::{MessageBuf, MessageSet};
use futures::{self, Async, Future, Poll};
use futures::future::{ok, FutureResult};
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Handle, Timeout};
use bytes::BytesMut;
use tokio_proto::streaming::{Body, Message};
use tokio_proto::streaming::multiplex::ServerProto;
use tokio_service::{NewService, Service};
use asynclog::{AsyncLog, LogFuture};
use proto::*;
use tcp::TcpServer;
use messages::{MessageBufPool, PooledMessageBuf};
use tail_reply::TailReplyRegistrar;

union_future!(ResFuture<Res, io::Error>,
Immediate => FutureResult<Res, io::Error>,
OptionalOffset => LogFuture<Option<Offset>>,
Messages => LogFuture<MessageBuf>);

type RequestMsg = Message<Req, Body<(), io::Error>>;
type ResponseMsg = Message<Res, Body<ResChunk, io::Error>>;

enum ResponseMsgFuture {
    Res(ResFuture),
    TailReply(Option<Body<ResChunk, io::Error>>),
}

impl Future for ResponseMsgFuture {
    type Item = ResponseMsg;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<ResponseMsg, io::Error> {
        match *self {
            ResponseMsgFuture::Res(ref mut f) => {
                let res = try_ready!(f.poll());
                Ok(Async::Ready(Message::WithoutBody(res)))
            }
            ResponseMsgFuture::TailReply(ref mut stream) => {
                let body = stream.take().expect("Expected future to be polled once");
                Ok(Async::Ready(Message::WithBody(Res::TailReplyStarted, body)))
            }
        }
    }
}

pub fn spawn(
    log: &AsyncLog,
    tail_register: TailReplyRegistrar,
    addr: SocketAddr,
    handle: &Handle,
) -> impl Future<Item = (), Error = io::Error> {
    let batcher = MessageBatcher::new(handle.clone(), log.clone());
    TcpServer::new(
        LogProto,
        LogServiceCreator {
            log: log.clone(),
            tail: tail_register,
            batcher,
        },
    ).spawn(addr, handle)
}

struct LogServiceCreator {
    log: AsyncLog,
    tail: TailReplyRegistrar,
    batcher: MessageBatcher,
}

impl NewService for LogServiceCreator {
    type Request = RequestMsg;
    type Response = ResponseMsg;
    type Error = io::Error;
    type Instance = LogService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(LogService {
            log: self.log.clone(),
            tail: self.tail.clone(),
            batcher: self.batcher.clone(),
        })
    }
}

struct LogService {
    log: AsyncLog,
    tail: TailReplyRegistrar,
    batcher: MessageBatcher,
}
impl Service for LogService {
    type Request = RequestMsg;
    type Response = ResponseMsg;
    type Error = io::Error;
    type Future = ResponseMsgFuture;

    fn call(&self, req_msg: RequestMsg) -> Self::Future {
        let res_fut = match req_msg.into_inner() {
            Req::Append {
                client_id,
                client_req_id,
                payload,
            } => {
                self.batcher.push(client_id, client_req_id, payload);
                ok(Res::Ack).into()
            }
            Req::Read(off) => self.log.read(off, ReadLimit::max_bytes(1024)).into(),
            Req::LastOffset => self.log.last_offset().into(),
            Req::RequestTailReply { client_id, .. } => {
                return ResponseMsgFuture::TailReply(Some(self.tail.listen(client_id)));
            }
        };
        ResponseMsgFuture::Res(res_fut)
    }
}


#[derive(Default)]
struct LogProto;
impl ServerProto<TcpStream> for LogProto {
    type Request = Req;
    type RequestBody = ();
    type Response = Res;
    type ResponseBody = ResChunk;
    type Error = io::Error;
    type Transport = Framed<TcpStream, Protocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(Protocol::default()))
    }
}

const MAX_BUFFER_BYTES: usize = 16_384;
const LINGER_MS: u64 = 5;

struct Inner {
    buf: PooledMessageBuf,
    log: AsyncLog,
    pool: MessageBufPool,

    // TODO: not a huge fan of the approach of using a boolean as a flag...
    linger_spawned: bool,
}

impl Inner {
    fn send(&mut self) {
        trace!("Sending batch through the log");

        if unsafe { unlikely(self.buf.is_empty()) } {
            return;
        }

        let mut buf = self.pool.take();
        mem::swap(&mut buf, &mut self.buf);
        self.log.append(buf);
    }
}

/// MessageBatcher will buffer messages on a per-core basis when either the linger
/// time has lapsed or the maximum number of bytes has been reached.
#[derive(Clone)]
struct MessageBatcher {
    inner: Rc<RefCell<Inner>>,
    handle: Handle,
}

impl MessageBatcher {
    fn new(handle: Handle, log: AsyncLog) -> MessageBatcher {
        let mut pool = MessageBufPool::new(5, MAX_BUFFER_BYTES);
        let buf = pool.take();
        MessageBatcher {
            handle: handle,
            inner: Rc::new(RefCell::new(Inner {
                buf: buf,
                pool: pool,
                log: log,
                linger_spawned: false,
            })),
        }
    }

    fn push(&self, client_id: u32, client_req_id: u32, msg: BytesMut) {
        let mut inner = self.inner.borrow_mut();

        // send immediately if we're exceeding capacity
        if inner.buf.bytes().len() + msg.len() > MAX_BUFFER_BYTES {
            trace!("Buffer exceeded, sending immediately");
            inner.send();
        }

        inner.buf.push(client_id, client_req_id, msg);

        if !inner.linger_spawned {
            trace!("Spawning timeout to send to the log");
            inner.linger_spawned = true;
            drop(inner);
            self.handle.spawn(
                LingerFuture {
                    timeout: Timeout::new(Duration::from_millis(LINGER_MS), &self.handle).unwrap(),
                    batcher: self.inner.clone(),
                }.map_err(|e| {
                    error!("Error spawning future to send to the log: {}", e);
                    ()
                }),
            );
        }
    }
}

struct LingerFuture {
    timeout: Timeout,
    batcher: Rc<RefCell<Inner>>,
}

impl Future for LingerFuture {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(), io::Error> {
        trace!("Polling timeout future");
        try_ready!(self.timeout.poll());
        trace!("Timeout reached, sending batch through the log");
        let mut inner = self.batcher.borrow_mut();
        inner.send();
        inner.linger_spawned = false;
        Ok(Async::Ready(()))
    }
}
