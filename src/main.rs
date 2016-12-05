extern crate commitlog;
extern crate env_logger;

#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::str;
use std::io::{self, ErrorKind, Write};

use futures::{future, Async, Poll, Future, BoxFuture};
use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::{Service, NewService};

use commitlog::*;

mod log;
use log::{LogFuture, AsyncLog};

#[derive(Debug)]
pub enum Req {
    Append(Vec<u8>),
    Read(u64),
}

pub enum Res {
    Offset(u64),
    Messages(Vec<Message>),
}

pub enum ResFuture {
    Offset(LogFuture<Offset>),
    Messages(LogFuture<Vec<Message>>),
}

impl Future for ResFuture {
    type Item = Res;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Res, io::Error> {
        match *self {
            ResFuture::Offset(ref mut f) => {
                let Offset(v) = try_ready!(f.poll());
                Ok(Async::Ready(Res::Offset(v)))
            },
            ResFuture::Messages(ref mut f) => {
                let vs = try_ready!(f.poll());
                Ok(Async::Ready(Res::Messages(vs)))
            }
        }
    }
}

pub struct LogService {
    log: Arc<AsyncLog>
}

impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => ResFuture::Offset(self.log.append(val)),
            Req::Read(off) => ResFuture::Messages(self.log.read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10))),
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
                let m = buf.drain_to(p + 1);
                let data = m.as_slice();
                let data = &data[0..data.len() - 1];
                if data.len() > 0 {
                    if data[0] == b'+' {
                        let s = String::from_utf8_lossy(&data[1..]);
                        if let Ok(n) = str::parse::<u64>(&s) {
                            return Ok(Some(Req::Read(n)));
                        }
                    }

                    Ok(Some(Req::Append(m.as_slice().iter().map(|v| *v).collect::<Vec<_>>())))
                } else {
                    Ok(None)
                }
            },
            None => Ok(None)
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> std::io::Result<()> {
        match msg {
            Res::Offset(off) => {
                let v = format!("+{}\n", off);
                buf.extend(v.as_bytes());
                Ok(())
            },
            Res::Messages(msgs) => {
                for m in msgs.iter() {
                    let v = format!("{}: {}", m.offset(), String::from_utf8_lossy(m.payload()));
                    buf.extend(v.as_bytes());
                }
                Ok(())
            },
        }
    }
}

#[derive(Default)]
pub struct LogProto;

impl<T: Io + 'static> ServerProto<T> for LogProto {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Transport = Framed<T, ServiceCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
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
        Ok(LogService {
            log: log,
        })
    }
}

fn main() {
    env_logger::init().unwrap();


    let addr = "0.0.0.0:4000".parse().unwrap();

    let log = Arc::new(AsyncLog::open());

    TcpServer::new(LogProto{}, addr)
        .serve(ServiceCreator(log));
}

