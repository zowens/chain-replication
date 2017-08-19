#![allow(unknown_lints)]
extern crate rand;
extern crate histogram;
extern crate getopts;
#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_io;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate byteorder;
extern crate bytes;
extern crate client;

use std::io;
use std::time;
use std::sync::{Arc, Mutex};
use std::thread;
use std::env;
use rand::{Rng, XorShiftRng};
use getopts::Options;
use std::process::exit;
use futures::{Future, Poll};
use tokio_core::reactor::Core;
use client::{Configuration, Connection, LogServerClient, RequestFuture};
use bytes::BytesMut;


macro_rules! to_ms {
    ($e:expr) => (
        (($e as f32) / 1000000f32)
    )
}

struct RandomSource {
    chars: usize,
    rand: XorShiftRng,
    buf: BytesMut,
}

impl RandomSource {
    fn new(chars: usize) -> RandomSource {
        RandomSource {
            chars,
            rand: XorShiftRng::new_unseeded(),
            buf: BytesMut::with_capacity(20 * chars),
        }
    }

    fn random_chars(&mut self) -> BytesMut {
        self.buf.reserve(self.chars);
        let it = self.rand
            .gen_ascii_chars()
            .take(self.chars)
            .map(|c| c as u8);
        self.buf.extend(it);
        self.buf.split_to(self.chars)
    }
}

#[derive(Clone)]
struct Metrics {
    state: Arc<Mutex<(u32, histogram::Histogram)>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        let metrics = Metrics {
            state: Arc::new(Mutex::new((0, histogram::Histogram::new()))),
        };

        {
            let metrics = metrics.clone();
            thread::spawn(move || {
                let mut last_report = time::Instant::now();
                loop {
                    thread::sleep(time::Duration::from_secs(10));
                    let now = time::Instant::now();
                    metrics
                        .snapshot(now.duration_since(last_report))
                        .unwrap_or_else(|e| {
                            error!("Error writing metrics: {}", e);
                            ()
                        });
                    last_report = now;
                }
            });
        }

        metrics
    }

    pub fn incr(&self, duration: time::Duration) {
        if duration.as_secs() > 0 {
            println!("WARN: {}s latency", duration.as_secs());
            return;
        }

        let nanos = duration.subsec_nanos() as u64;
        let mut data = self.state.lock().unwrap();
        data.0 += 1;
        data.1.increment(nanos).unwrap();
    }

    pub fn snapshot(&self, since_last: time::Duration) -> Result<(), &str> {
        let (requests, p95, p99, p999, max) = {
            let mut data = self.state.lock().unwrap();
            let v = (
                data.0,
                data.1.percentile(95.0)?,
                data.1.percentile(99.0)?,
                data.1.percentile(99.9)?,
                data.1.maximum()?,
            );
            data.0 = 0;
            data.1.clear();
            v

        };
        println!(
            "AVG REQ/s :: {}",
            (requests as f32) /
                (since_last.as_secs() as f32 + (since_last.subsec_nanos() as f32 / 1000000000f32))
        );

        println!(
            "LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
            to_ms!(p95),
            to_ms!(p99),
            to_ms!(p999),
            to_ms!(max)
        );

        Ok(())
    }
}

#[allow(or_fun_call)]
fn parse_opts() -> (String, u32, u32, usize) {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "address of the server", "HOST:PORT");
    opts.optopt("w", "threads", "number of connections", "N");
    opts.optopt(
        "c",
        "concurrent-requests",
        "number of concurrent requests",
        "N",
    );
    opts.optopt("b", "bytes", "number of bytes per message", "N");
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

    let addr = matches.opt_str("a").unwrap_or("127.0.0.1:4000".to_string());

    let threads = matches.opt_str("w").unwrap_or("1".to_string());
    let threads = u32::from_str_radix(threads.as_str(), 10).unwrap();

    let concurrent = matches.opt_str("c").unwrap_or("2".to_string());
    let concurrent = u32::from_str_radix(concurrent.as_str(), 10).unwrap();

    let bytes = matches.opt_str("b").unwrap_or("100".to_string());
    let bytes = u32::from_str_radix(bytes.as_str(), 10).unwrap() as usize;

    (addr, threads, concurrent, bytes)
}

struct TrackedRequest {
    client: Connection,
    rand: RandomSource,
    f: RequestFuture<()>,
    metrics: Metrics,
    start: time::Instant,
}

impl TrackedRequest {
    fn new(metrics: Metrics, conn: Connection, chars: usize) -> TrackedRequest {
        let mut rand = RandomSource::new(chars);
        let f = conn.append_buf(rand.random_chars());
        TrackedRequest {
            client: conn,
            metrics: metrics,
            start: time::Instant::now(),
            rand,
            f,
        }
    }
}

impl Future for TrackedRequest {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            try_ready!(self.f.poll());
            let stop = time::Instant::now();
            self.metrics.incr(stop.duration_since(self.start));
            self.f = self.client.append_buf(self.rand.random_chars());
            self.start = stop;
        }
    }
}

pub fn main() {
    env_logger::init().unwrap();

    let (addr, connections, concurrent, bytes) = parse_opts();

    let metrics = Metrics::new();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut client_config = Configuration::default();
    client_config.head(addr).unwrap();
    let client = LogServerClient::new(client_config, handle.clone());

    for _ in 0..connections {
        let m = metrics.clone();
        let hdl = core.handle();
        handle.spawn(
            client
                .new_connection()
                .map(move |conn| {
                    for _ in 0..concurrent {
                        hdl.spawn(
                            TrackedRequest::new(m.clone(), conn.clone(), bytes).map_err(|e| {
                                error!("I/O Error for request: {}", e);
                            }),
                        );
                    }

                    ()
                })
                .map_err(|e| {
                    error!("I/O Error for connection: {}", e);
                    ()
                }),
        );
    }

    loop {
        core.turn(None)
    }
}
