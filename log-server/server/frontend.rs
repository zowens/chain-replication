use std::io;
use commitlog::*;
use commitlog::message::MessageBuf;
use futures;
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use tokio_proto::multiplex::ServerProto;
use tokio_service::{NewService, Service};
use asynclog::{AsyncLog, LogFuture};
use proto::*;

union_future!(ResFuture<Res, io::Error>,
              Offset => LogFuture<Offset>,
              OptionalOffset => LogFuture<Option<Offset>>,
              Messages => LogFuture<MessageBuf>);

pub struct LogServiceCreator {
    log: AsyncLog,
}

impl LogServiceCreator {
    pub fn new(log: AsyncLog) -> LogServiceCreator {
        LogServiceCreator { log: log }
    }
}

impl NewService for LogServiceCreator {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Instance = LogService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(LogService(self.log.clone()))
    }
}

pub struct LogService(AsyncLog);
impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => self.0.append(val).into(),
            Req::Read(off) => self.0.read(off, ReadLimit::max_bytes(1024)).into(),
            Req::LastOffset => self.0.last_offset().into(),
        }
    }
}


#[derive(Default)]
pub struct LogProto;
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
