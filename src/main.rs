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

extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate num_cpus;

mod asynclog;
mod protocol;
mod service;

use tokio_core::reactor::Core;
use asynclog::AsyncLog;

fn main() {
    env_logger::init().unwrap();

    let addr: std::net::SocketAddr = "0.0.0.0:4000".parse().unwrap();

    let mut core = Core::new().unwrap();
    let (_handle, log) = AsyncLog::open();
    let handle = core.handle();
    core.run(service::spawn_service(addr, &handle, log)).unwrap();
}
