#![feature(core_intrinsics, test)]
extern crate bytes;
extern crate commitlog;
extern crate test;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate prometheus;
extern crate byteorder;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate either;
extern crate fnv;
extern crate http;
extern crate hyper;
extern crate libc;
extern crate nix;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;
#[macro_use]
extern crate serde_derive;
extern crate env_logger;
extern crate grpcio;
extern crate protobuf;
extern crate toml;

mod admin_server;
mod asynclog;
mod config;
mod protocol;
mod replication;
mod server;
mod tail_reply;

use futures::{future::lazy, Future};
use std::io::Read;
use std::process::exit;
use std::{env, fs, str};
use tokio::executor::current_thread::spawn;
use tokio::runtime::current_thread::Runtime;

fn config() -> config::Config {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} [config_file]", args[0]);
        exit(1);
    }

    let config: config::Config = {
        let mut f = fs::File::open(&args[1]).expect("Unable to open config file");
        let mut bytes = vec![];
        f.read_to_end(&mut bytes)
            .expect("Unable to read config file");
        let cfg = str::from_utf8(&bytes).expect("Invalid UTF-8");
        toml::from_str(cfg).expect("Unable to parse TOML")
    };

    info!("Starting with configuration {:?}", config);
    config
}

pub fn main() {
    env_logger::init();

    let config = config();
    let mut rt = Runtime::new().unwrap();
    // TODO: remove unwrap here
    rt.block_on(lazy(move || {
        let (listener, register) = tail_reply::new();
        let lr = replication::log_reader::FileSliceMessageReader;
        let (log, r_log) = asynclog::open(config.log, listener, lr);

        spawn(replication::server(
            &config.replication.server_addr,
            r_log.clone(),
        ));

        if let Some(ref upstream_addr) = config.replication.upstream_addr {
            spawn(replication::Replication::new(upstream_addr, r_log).map_err(|_| ()));
        }

        if let Some(ref admin) = config.admin {
            spawn(admin_server::server(&admin.server_addr));
        }

        server::server(&config.frontend, log, register)
    })).unwrap();
}
