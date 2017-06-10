use std::io;
use std::rc::Rc;
use std::cell::RefCell;
use std::mem;
use std::time::Duration;
use std::intrinsics::unlikely;
use std::net::SocketAddr;
use commitlog::*;
use commitlog::message::{MessageBuf, MessageSet};
use futures::{self, Future, Poll, Async};
use futures::future::{ok, FutureResult};
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Handle, Timeout};
use bytes::BytesMut;
use tokio_proto::multiplex::ServerProto;
use tokio_service::{NewService, Service};
use asynclog::{AsyncLog, LogFuture};
use proto::*;
use tcp::TcpServer;
use messages::{MessageBufPool, PooledMessageBuf};

union_future!(ResFuture<Res, io::Error>,
              Immediate => FutureResult<Res, io::Error>,
              OptionalOffset => LogFuture<Option<Offset>>,
              Messages => LogFuture<MessageBuf>);

pub fn spawn(log: &AsyncLog,
             addr: SocketAddr,
             handle: &Handle)
             -> impl Future<Item = (), Error = io::Error> {
    TcpServer::new(LogProto,
                   LogServiceCreator::new(handle.clone(), log.clone()))
            .spawn(addr, handle)
}

struct LogServiceCreator {
    log: AsyncLog,
    batcher: MessageBatcher,
}

impl LogServiceCreator {
    pub fn new(handle: Handle, log: AsyncLog) -> LogServiceCreator {
        LogServiceCreator {
            log: log.clone(),
            batcher: MessageBatcher::new(handle, log),
        }
    }
}

impl NewService for LogServiceCreator {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Instance = LogService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(LogService(self.log.clone(), self.batcher.clone()))
    }
}

struct LogService(AsyncLog, MessageBatcher);
impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => {
                self.1.push(val);
                ok(Res::Ack).into()
            }
            Req::Read(off) => self.0.read(off, ReadLimit::max_bytes(1024)).into(),
            Req::LastOffset => self.0.last_offset().into(),
        }
    }
}


#[derive(Default)]
struct LogProto;
impl ServerProto<TcpStream> for LogProto {
    type Request = Req;
    type Response = Res;
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

    fn push(&self, msg: BytesMut) {
        let mut inner = self.inner.borrow_mut();

        // send immediately if we're exceeding capacity
        if inner.buf.bytes().len() + msg.len() > MAX_BUFFER_BYTES {
            trace!("Buffer exceeded, sending immediately");
            inner.send();
        }

        inner.buf.push(msg);

        if !inner.linger_spawned {
            trace!("Spawning timeout to send to the log");
            inner.linger_spawned = true;
            drop(inner);
            self.handle
                .spawn(LingerFuture {
                               timeout: Timeout::new(Duration::from_millis(LINGER_MS),
                                                     &self.handle)
                                       .unwrap(),
                               batcher: self.inner.clone(),
                           }
                           .map_err(|e| {
                                        error!("Error spawning future to send to the log: {}", e);
                                        ()
                                    }));
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
