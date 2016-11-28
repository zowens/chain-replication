extern crate commitlog;
extern crate futures;
extern crate env_logger;

extern crate tokio_core as tokio;
extern crate tokio_proto;
extern crate tokio_service;

mod log;
use log::AsyncLog;
use commitlog::*;

use futures::{Future, Sink};
use futures::future::BoxFuture;
use tokio_service::Service;
use std::io::{Error, ErrorKind, BufReader};


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

fn main() {
    env_logger::init().unwrap();

    // let mut core = Core::new().unwrap();
    //
    // Bind to port 4000
    // let addr = "0.0.0.0:4000".parse().unwrap();
    //
    // Create the new TCP listener
    // let handle = &core.handle();
    // let listener = TcpListener::bind(&addr, handle).unwrap();
    //
    // let srv = listener.incoming().for_each(move |(stream, addr)| {
    // Ok(())
    // });
    //
    // core.run(srv).unwrap();
}
