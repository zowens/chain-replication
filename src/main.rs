extern crate commitlog;
extern crate futures;
extern crate env_logger;

extern crate tokio_core as tokio;
extern crate tokio_proto;
extern crate tokio_service;

mod log;
use log::AsyncLog;
use commitlog::*;

use futures::{Future, Stream, Sink};
use futures::future::{BoxFuture, ok};
use tokio_service::Service;
use tokio::reactor::Core;
use tokio::net::TcpListener;
use tokio::io::{Io, EasyBuf, Codec};

use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::io::{Error, ErrorKind, BufReader};


#[derive(Debug)]
enum Request {
    Append(Vec<u8>),
    Read(u64),
}

enum Response {
    Offset(u64),
    Messages(Vec<Message>),
}

struct LogService(AsyncLog);

impl Service for LogService {
    type Request = Request;
    type Response = Response;
    type Error = Error;
    type Future = BoxFuture<Response, Error>;

    fn call(&self, req: Request) -> Self::Future {
        match req {
            Request::Append(val) => self.0.append(val)
                .map(|Offset(o)| Response::Offset(o))
                .boxed(),
            Request::Read(off) => self.0.read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10))
                .map(|msgs| Response::Messages(msgs))
                .boxed(),
        }
    }
}

struct ServiceCodec;
impl Codec for ServiceCodec {
    /// The type of decoded frames.
    type In = Request;

    /// The type of frames to be encoded.
    type Out = Response;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, Error> {
        let pos = buf.as_slice().iter().position(|v| *v == b'\n');
        match pos {
            Some(p) => {
                let m = buf.drain_to(p + 1);
                let data = m.as_slice();
                let data = &data[0..data.len() - 1];
                if data.len() > 0 {
                    println!("read {}", data.len());
                    if data[0] == b'+' {
                        let s = String::from_utf8_lossy(&data[1..]);
                        println!("{}", s);
                        if let Ok(n) = str::parse::<u64>(&s) {
                            return Ok(Some(Request::Read(n)));
                        }
                    }

                    Ok(Some(Request::Append(m.as_slice().iter().map(|v| *v).collect::<Vec<_>>())))
                } else {
                    Ok(None)
                }
            },
            None => Ok(None)
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> std::io::Result<()> {
        match msg {
            Response::Offset(off) => {
                let v = format!("{}\n", off);
                buf.extend(v.as_bytes());
                Ok(())
            },
            Response::Messages(msgs) => {
                for m in msgs.iter() {
                    let v = format!("{}: {}", m.offset(), String::from_utf8_lossy(m.payload()));
                    buf.extend(v.as_bytes());
                }
                Ok(())
            },
        }
    }
}


fn main() {
    env_logger::init().unwrap();

    let mut core = Core::new().unwrap();
    // Bind to port 4000
    let addr = "0.0.0.0:4000".parse().unwrap();
    //
    // Create the new TCP listener
    let handle = &core.handle();
    let listener = TcpListener::bind(&addr, handle).unwrap();

    let log = Rc::new(AsyncLog::open());
    let srv = listener.incoming().for_each(move |(stream, addr)| {
        let (sink, stream) = stream.framed(ServiceCodec{}).split();

        let log = log.clone();
        let r = stream.and_then(move |req| {
            println!("{:?}", req);
            match req {
                Request::Append(val) => log.append(val)
                    .map(|Offset(o)| Response::Offset(o))
                    .boxed(),
                Request::Read(off) => log.read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10))
                    .map(|msgs| Response::Messages(msgs))
                    .boxed(),
            }

        })
        .forward(sink);

        handle.spawn(r.map(|_| ()).map_err(|_| ()));

        Ok(())
    });
    core.run(srv).unwrap();
}
