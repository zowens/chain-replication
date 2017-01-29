#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;
extern crate net2;
extern crate byteorder;

#[macro_use]
extern crate union_future;

#[macro_use]
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;
extern crate pool;

extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate num_cpus;

mod asynclog;
mod server;
mod proto;
mod replication;
mod net;

use tokio_core::reactor::Core;
use asynclog::AsyncLog;

fn main() {
    env_logger::init().unwrap();

    let addr: std::net::SocketAddr = "0.0.0.0:4000".parse().unwrap();

    let mut core = Core::new().unwrap();
    let (_handle, log) = AsyncLog::open();

    let server = net::TcpServer::new(server::LogProto, move || Ok(server::LogService(log.clone())));

    let handle = core.handle();
    core.run(server.spawn(addr, &handle)).unwrap();
}
