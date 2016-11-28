extern crate commitlog;
extern crate futures;
extern crate env_logger;

extern crate tokio_core as tokio;
extern crate tokio_proto;
extern crate tokio_service;

mod log;

use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Future, Sink};
use futures::stream::{self, Stream};
use commitlog::*;
use std::iter;
use std::thread;
use std::io::{Error, ErrorKind, BufReader};

use tokio::reactor::Core;
use tokio::io::{self, Io};
use tokio::net::TcpListener;

enum Request {
    Append(Vec<u8>),
    Read(u64),
}

enum Response {
    Offset(u64),
    Messages(Vec<Message>),
    Error,
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
