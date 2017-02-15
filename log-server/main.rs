#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;
extern crate net2;
extern crate byteorder;
extern crate getopts;
extern crate config;
extern crate nix;
extern crate libc;

#[macro_use]
extern crate union_future;

#[macro_use]
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;
extern crate pool;
#[macro_use]
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate num_cpus;

mod asynclog;
mod server;
mod proto;
mod replication;
mod messages;

use std::env;
use std::process::exit;
use std::net::SocketAddr;
use std::thread;

use tokio_core::reactor::Core;
use getopts::Options;

#[derive(Debug)]
struct NodeOptions {
    log_dir: String,
    replication_server_addr: SocketAddr,
    node_type: NodeType,
}

#[derive(Debug)]
enum NodeType {
    HeadNode { addr: SocketAddr },
    ReplicaNode { upstream_addr: SocketAddr },
}

fn expect_opt(v: &str) -> String {
    config::get_str(v).expect(format!("Expected config key: {}", v).as_str()).into_owned()
}

fn parse_opts() -> NodeOptions {

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("c", "config", "Configuration file in TOML format", "FILE");
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

    if matches.opt_present("c") {
        trace!("Loading config file");
        let config_file = matches.opt_str("c").unwrap().to_string().replace(".toml", "");
        config::merge(config::File::new(&config_file, config::FileFormat::Toml)).unwrap();
    }

    config::merge(config::Environment::new("CR")).unwrap();

    let mode = match config::get_str("frontend.server_addr") {
        Some(v) => NodeType::HeadNode { addr: v.into_owned().parse().unwrap() },
        None => {
            NodeType::ReplicaNode {
                upstream_addr: expect_opt("replication.upstream_addr").parse().unwrap(),
            }
        }
    };

    NodeOptions {
        log_dir: expect_opt("log.dir"),
        replication_server_addr: expect_opt("replication.server_addr").parse().unwrap(),
        node_type: mode,
    }
}

fn main() {
    env_logger::init().unwrap();


    let cmd_opts = parse_opts();

    let (_handle, log) = asynclog::AsyncLog::open(&cmd_opts.log_dir);

    // TODO: is this a good idea...? Probably should be over in the server realm
    let replication_thread = {
        let log = log.clone();
        let repl_addr = cmd_opts.replication_server_addr;
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let hdl = core.handle();
            core.run(server::spawn_replication(&log, repl_addr, &hdl)).unwrap();
        })
    };

    let mut core = Core::new().unwrap();
    match cmd_opts.node_type {
        NodeType::HeadNode { addr } => {
            let handle = core.handle();
            core.run(server::spawn_frontend(&log, addr, &handle))
                .unwrap();
        }
        NodeType::ReplicaNode { upstream_addr } => {
            let handle = core.handle();
            core.run(replication::ReplicationClient::new(&log, upstream_addr, &handle))
                .unwrap();
        }
    }

    replication_thread.join().unwrap();
}
