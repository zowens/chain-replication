#![feature(core_intrinsics)]
extern crate commitlog;
extern crate env_logger;
extern crate net2;
extern crate byteorder;
extern crate nix;
extern crate libc;
#[macro_use]
extern crate serde_derive;
extern crate toml;

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
mod config;

use std::env;
use std::fs;
use std::str;
use std::process::exit;
use std::io::Read;
use std::thread;

use tokio_core::reactor::Core;
use config::Config;

fn main() {
    env_logger::init().unwrap();
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} [config_file]", args[0]);
        exit(1);
    }

    let config: Config = {
        let mut f = fs::File::open(&args[1]).expect("Unable to open config file");
        let mut bytes = vec![];
        f.read_to_end(&mut bytes).expect("Unable to read config file");
        let cfg = str::from_utf8(&bytes).expect("Invalid UTF-8");
        toml::from_str(cfg).expect("Unable to parse TOML")
    };

    info!("Starting with configuration {:?}", config);


    let (_handle, log) = asynclog::AsyncLog::open(&config.log.dir);

    // TODO: is this a good idea...? Probably should be over in the server realm
    let replication_thread = {
        let log = log.clone();
        let repl_addr = config.replication.server_addr.clone();
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let hdl = core.handle();
            core.run(server::spawn_replication(&log, repl_addr, &hdl)).unwrap();
        })
    };

    let mut core = Core::new().unwrap();
    if let Some(v) = config.frontend {
        let handle = core.handle();
        core.run(server::spawn_frontend(&log, v.server_addr, &handle))
                .unwrap();
    } else {
        if let Some(upstream_addr) = config.replication.upstream_addr {
            let handle = core.handle();
            core.run(replication::ReplicationClient::new(&log, upstream_addr, &handle))
                .unwrap();
        } else {
            error!("No upstream address provided");
            exit(1);
        }
    }

    replication_thread.join().unwrap();
}
