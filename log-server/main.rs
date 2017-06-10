#![feature(core_intrinsics, conservative_impl_trait)]
extern crate commitlog;
extern crate env_logger;
extern crate net2;
extern crate byteorder;
extern crate nix;
extern crate libc;
#[macro_use]
extern crate serde_derive;
extern crate toml;
extern crate bytes;

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
extern crate tokio_io;
extern crate num_cpus;
#[macro_use]
extern crate prometheus;
extern crate hyper;
#[macro_use]
extern crate lazy_static;

mod asynclog;
mod frontend_server;
mod internal_server;
mod proto;
mod replication;
mod messages;
mod config;
mod metrics;
mod tcp;

use std::env;
use std::fs;
use std::str;
use std::process::exit;
use std::io::Read;
use std::thread;

use futures::Future;
use tokio_core::reactor::Core;
use config::Config;

fn run_server(config: Config, log: &asynclog::AsyncLog) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    if let Some(metrics_config) = config.metrics {
        handle.spawn(metrics::spawn(&handle, &metrics_config));
    }

    {
        let repl_addr = config.replication.server_addr;
        handle.spawn(internal_server::spawn(log, repl_addr, &handle).map_err(|_| ()));
    }

    if let Some(v) = config.frontend {
        handle.spawn(frontend_server::spawn(log, v.server_addr, &handle).map_err(|_| ()));
    }

    if let Some(upstream_addr) = config.replication.upstream_addr {
        let handle = core.handle();
        handle.spawn(replication::ReplicationClient::new(log, upstream_addr, &handle)
                         .map_err(|_| ()));
    }

    loop {
        core.turn(None)
    }
}

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
        f.read_to_end(&mut bytes)
            .expect("Unable to read config file");
        let cfg = str::from_utf8(&bytes).expect("Invalid UTF-8");
        toml::from_str(cfg).expect("Unable to parse TOML")
    };

    info!("Starting with configuration {:?}", config);

    let (_handle, log) = asynclog::AsyncLog::open(&config.log.dir);

    for _ in 1..num_cpus::get() {
        let log = log.clone();
        let config = config.clone();
        thread::spawn(move || run_server(config, &log));
    }
    run_server(config, &log);

}
