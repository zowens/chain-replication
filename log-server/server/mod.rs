use std::io;
use commitlog::*;
use futures;
use tokio_core::io::{Io, Framed};
use tokio_core::net::TcpStream;
use tokio_proto::multiplex::ServerProto;
use tokio_service::Service;
use super::asynclog::{AsyncLog, LogFuture, Messages};
mod protocol;
use server::protocol::*;

union_future!(ResFuture<Res, io::Error>,
              Offset => LogFuture<Offset>,
              Messages => LogFuture<Messages>);

pub struct LogService(pub AsyncLog);

impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => self.0.append(val).into(),
            Req::ReplicateFrom(offset) => self.0.replicate_from(offset).into(),
            Req::Read(off) => {
                self.0
                    .read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10))
                    .into()
            }
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
