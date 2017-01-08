#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;

#[macro_use]
extern crate union_future;

#[macro_use]
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;

extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate num_cpus;
extern crate memchr;

use std::io;
use tokio_core::io::{Io, Framed};
use tokio_core::net::TcpStream;
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::{Service, NewService};

use commitlog::*;

mod asynclog;
use asynclog::{LogFuture, AsyncLog};
mod protocol;
use protocol::*;

union_future!(ResFuture<Res, io::Error>,
              Offset => LogFuture<Offset>,
              Messages => LogFuture<MessageSet>);


pub struct LogService {
    log: AsyncLog,
}

impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&mut self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => self.log.append(val).into(),
            Req::Read(off) => {
                self.log
                    .read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10))
                    .into()
            }
            Req::LastOffset => self.log.last_offset().into(),
        }
    }
}


#[derive(Default)]
pub struct LogProto;

impl ServerProto<TcpStream> for LogProto {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Transport = Framed<TcpStream, Protocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(Protocol))
    }
}

pub struct ServiceCreator(AsyncLog);

impl NewService for ServiceCreator {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Instance = LogService;

    fn new_service(&self) -> io::Result<LogService> {
        let log = self.0.clone();
        Ok(LogService { log: log })
    }
}

fn main() {
    env_logger::init().unwrap();

    let addr = "0.0.0.0:4000".parse().unwrap();

    let (_handle, log) = AsyncLog::open();

    let mut srv = TcpServer::new(LogProto {}, addr);
    srv.threads(num_cpus::get());
    srv.serve(ServiceCreator(log));
}
