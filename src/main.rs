#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;
extern crate metrics;

#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;

extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate num_cpus;

use std::sync::Arc;
use std::str;
use std::io;

use futures::{Async, Poll, Future};
use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_core::net::TcpStream;
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::{Service, NewService};

use commitlog::*;

mod asynclog;
use asynclog::{LogFuture, AsyncLog};
mod reporter;

pub enum Req {
    Append(EasyBuf),
    Read(u64),
}

pub enum Res {
    Offset(Offset),
    Messages(MessageSet),
}

pub enum ResFuture {
    Offset(LogFuture<Offset>),
    Messages(LogFuture<MessageSet>),
}

macro_rules! try_poll {
    ($e: expr) => (
        try_ready!($e.map_err(|e| {
            error!("{}", e);
            e
        }));
    )
}

impl Future for ResFuture {
    type Item = Res;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Res, io::Error> {
        match *self {
            ResFuture::Offset(ref mut f) => {
                let v = try_poll!(f.poll());
                Ok(Async::Ready(Res::Offset(v)))
            }
            ResFuture::Messages(ref mut f) => {
                let vs = try_poll!(f.poll());
                Ok(Async::Ready(Res::Messages(vs)))
            }
        }
    }
}

pub struct LogService {
    log: Arc<AsyncLog>,
}

impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => ResFuture::Offset(self.log.append(val.as_slice())),
            Req::Read(off) => {
                ResFuture::Messages(self.log
                    .read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10)))
            }
        }
    }
}

#[derive(Default)]
pub struct ServiceCodec;

impl Codec for ServiceCodec {
    /// The type of decoded frames.
    type In = Req;

    /// The type of frames to be encoded.
    type Out = Res;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        let pos = buf.as_slice().iter().position(|v| *v == b'\n');
        match pos {
            Some(p) => {
                let mut m = buf.drain_to(p + 1);

                // Remove trailing newline character
                m.split_off(p);

                if m.len() > 0 {
                    if m.as_slice()[0] == b'+' {
                        let data = m.as_slice();
                        let s = String::from_utf8_lossy(&data[1..]);
                        if let Ok(n) = str::parse::<u64>(&s) {
                            return Ok(Some(Req::Read(n)));
                        }
                    }

                    Ok(Some(Req::Append(m)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> std::io::Result<()> {
        match msg {
            Res::Offset(off) => {
                let v = format!("+{}\n", off.0);
                buf.extend(v.as_bytes());
                Ok(())
            }
            Res::Messages(msgs) => {
                for m in msgs.iter() {
                    let v = format!("{}: {}\n", m.offset(), String::from_utf8_lossy(m.payload()));
                    buf.extend(v.as_bytes());
                }
                Ok(())
            }
        }
    }
}

#[derive(Default)]
pub struct LogProto;

impl ServerProto<TcpStream> for LogProto {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Transport = Framed<TcpStream, ServiceCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(ServiceCodec))
    }
}

pub struct ServiceCreator(Arc<AsyncLog>);

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

    let log = Arc::new(AsyncLog::open());

    let mut srv = TcpServer::new(LogProto {}, addr);
    srv.threads(num_cpus::get());
    srv.serve(ServiceCreator(log));
}
