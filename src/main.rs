#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;
extern crate metrics;

#[macro_use]
extern crate union_future;

#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;

extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate num_cpus;
extern crate memchr;

use std::str;
use std::io::{self, Write};
use std::intrinsics::likely;

use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_core::net::TcpStream;
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::{Service, NewService};

use commitlog::*;

mod asynclog;
use asynclog::{LogFuture, AsyncLog};
mod reporter;

macro_rules! probably {
    ($e: expr) => (
        unsafe {
            likely($e)
        }
    )
}

pub enum Req {
    Append(EasyBuf),
    Read(u64),
    LastOffset,
}

pub enum Res {
    Offset(Offset),
    Messages(MessageSet),
}

impl From<Offset> for Res {
    fn from(other: Offset) -> Res {
        Res::Offset(other)
    }
}

impl From<MessageSet> for Res {
    fn from(other: MessageSet) -> Res {
        Res::Messages(other)
    }
}

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
            Req::Append(val) => ResFuture::Offset(self.log.append(val.as_slice())),
            Req::Read(off) => {
                ResFuture::Messages(self.log
                    .read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10)))
            },
            Req::LastOffset => ResFuture::Offset(self.log.last_offset())
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
        let pos = memchr::memchr(b'\n', buf.as_slice());

        match pos {
            Some(p) => {
                let mut m = buf.drain_to(p + 1);
                // Remove trailing newline character
                m.split_off(p);

                if probably!(m.len() > 0) {
                    {
                        let data = m.as_slice();
                        if data[0] == b'+' {
                            let s = String::from_utf8_lossy(&data[1..]);
                            return str::parse::<u64>(&s)
                                .map(|n| Some(Req::Read(n)))
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));
                        } else if data[0] == b'?' {
                            return Ok(Some(Req::LastOffset));
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
                write!(buf, "+{}\n", off.0)
            }
            Res::Messages(msgs) => {
                for m in msgs.iter() {
                    write!(buf, "{}: {}\n", m.offset(), String::from_utf8_lossy(m.payload()))?;
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

    let log = AsyncLog::open();

    let mut srv = TcpServer::new(LogProto {}, addr);
    srv.threads(num_cpus::get());
    srv.serve(ServiceCreator(log));
}
