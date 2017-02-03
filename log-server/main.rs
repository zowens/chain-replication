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

use futures::Future;
use tokio_core::reactor::Core;
use getopts::Options;

struct NodeOptions {
    log_dir: String,
    node_type: NodeType,
}

enum NodeType {
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
    opts.optopt("l", "log", "replication log", "DIR");

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
        let log_dir = matches.opt_str("l")
            .unwrap_or_else(|| "replica_log".to_string());
        NodeOptions {
            log_dir: log_dir,
            node_type: NodeType::ReplicaNode,
        }
    } else {
        trace!("Starting as head node");
        let log_dir = matches.opt_str("l")
            .unwrap_or_else(|| "log".to_string());
        NodeOptions {
            log_dir: log_dir,
            node_type: NodeType::HeadNode,
        }
    }
}

fn main() {
    env_logger::init().unwrap();

    let addr: std::net::SocketAddr = "0.0.0.0:4000".parse().unwrap();
    let replication_addr: std::net::SocketAddr = "0.0.0.0:4001".parse().unwrap();

    let mut core = Core::new().unwrap();

    let cmd_opts = parse_opts();
    let (_handle, log) = asynclog::AsyncLog::open(&cmd_opts.log_dir);
    match cmd_opts.node_type {
        NodeType::HeadNode => {
            let handle = core.handle();
            core.run(server::spawn_frontend(&log, addr, &handle)
                     .join(server::spawn_replication(&log, replication_addr, &handle)))
                .unwrap();
        }
        NodeType::ReplicaNode => {
            let handle = core.handle();
            core.run(replication::ReplicationFuture::new(&log, replication_addr, handle))
                .unwrap();
        }
    }

}
