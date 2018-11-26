extern crate futures;
extern crate grpcio;
extern crate protobuf;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate env_logger;
extern crate tokio;
extern crate tokio_signal;
extern crate toml;

mod chain;
mod config;
mod handle;
mod protocol;

use config::Config;
use futures::{Future, Stream};
use grpcio::{Environment, ServerBuilder};
use std::io::Read;
use std::process::exit;
use std::sync::Arc;
use std::{env, fs, str};

fn load_config() -> Config {
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
    config
}

fn main() {
    env_logger::init();

    let Config { server_addr, failure_detection } = load_config();
    let service = handle::ManagementService(chain::Chain::new(failure_detection));

    let env = Arc::new(Environment::new(1));

    let host = server_addr.ip().to_string();
    let port = server_addr.port();

    let mut server = ServerBuilder::new(env)
        .register_service(protocol::create_configuration(service))
        .bind(host, port)
        .build()
        .unwrap();
    server.start();

    for &(ref host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }

    // wait for sigint
    let ctrl_c = tokio_signal::ctrl_c().flatten_stream().into_future();
    tokio::runtime::current_thread::block_on_all(ctrl_c)
        .map_err(|_| ())
        .expect("unable to capture ctrl-c");
    server.shutdown().wait().unwrap();
}
