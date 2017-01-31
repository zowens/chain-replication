#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;
extern crate net2;
extern crate byteorder;
extern crate getopts;

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

use std::env;
use std::process::exit;

use tokio_core::reactor::Core;
use getopts::Options;

enum NodeOptions {
    HeadNode,
    ReplicaNode,
}

fn parse_opts() -> NodeOptions {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    // TODO: allow configuring addresses
    //opts.optopt("a", "address", "address of the server", "HOST:PORT");

    opts.optflag("r", "replica", "connect to replica");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options]", program);
        print!("{}", opts.usage(&brief));
        exit(1);
    }

    if matches.opt_present("r") {
        trace!("Starting as replica node");
        NodeOptions::ReplicaNode
    } else {
        trace!("Starting as head node");
        NodeOptions::HeadNode
    }
}

fn main() {
    env_logger::init().unwrap();

    let addr: std::net::SocketAddr = "0.0.0.0:4000".parse().unwrap();
    let replication_addr: std::net::SocketAddr = "0.0.0.0:4001".parse().unwrap();

    let mut core = Core::new().unwrap();

    match parse_opts() {
        NodeOptions::HeadNode => {
            server::start(&mut core, addr, replication_addr);
        }
        NodeOptions::ReplicaNode => {
            let handle = core.handle();
            core.run(replication::ReplicationFuture::new(replication_addr, handle))
                .unwrap();
        }
    }

}
