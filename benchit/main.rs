#![feature(duration_as_u128)]
#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
#[macro_use]
extern crate futures;
extern crate getopts;
extern crate histogram;
#[macro_use]
extern crate log;
extern crate rand;
extern crate tokio;

use client::{AppendSentFuture, Configuration, Connection, LogServerClient};
use futures::stream::poll_fn;
use futures::{Async, Future, Poll, Stream};
use getopts::Options;
use rand::{distributions::Alphanumeric, rngs::SmallRng, FromEntropy, Rng};
use std::cell::RefCell;
use std::env;
use std::process::exit;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Interval;

macro_rules! to_ms {
    ($e:expr) => {
        (($e as f32) / 1_000_000f32)
    };
}

struct RandomSource {
    chars: usize,
    rand: SmallRng,
}

impl RandomSource {
    fn new(chars: usize) -> RandomSource {
        RandomSource {
            chars,
            rand: SmallRng::from_entropy(),
        }
    }

    fn random_chars(&mut self) -> Vec<u8> {
        let mut v: Vec<u8> = Vec::with_capacity(self.chars);
        v.extend(
            self.rand
                .sample_iter(&Alphanumeric)
                .map(|c| c as u8)
                .take(self.chars),
        );
        v
    }
}

#[allow(dead_code)]
struct Metrics {
    state: histogram::Histogram,
    conn: Connection,
    msg_size: usize,
}

impl Metrics {
    pub fn spawn(
        mut conn: Connection,
        start_instant: Instant,
        msg_size: usize,
    ) -> impl Future<Item = (), Error = ()> {
        let replies = conn.raw_replies(0);
        let metrics = Rc::new(RefCell::new(Metrics {
            state: histogram::Histogram::default(),
            conn,
            msg_size,
        }));

        let periodic_report = {
            let metrics = metrics.clone();
            let wait = Duration::from_secs(10);
            Interval::new(Instant::now() + wait, wait)
                .map(move |_| {
                    if let Err(e) = metrics.borrow_mut().snapshot() {
                        error!("ERROR with metrics snapshot: {}", e);
                    }
                })
                .map_err(|e| {
                    error!("ERROR with timer: {}", e);
                })
        };

        let replies = {
            let mut reply_stream = replies.map_err(|e| {
                error!("ERROR with reply stream: {}", e);
                ()
            });

            poll_fn(move || -> Poll<Option<()>, ()> {
                // Capture the relative nanoseconds since the test start.
                let since_start = (Instant::now() - start_instant).as_nanos() as u64;
                while let Some(v) = try_ready!(reply_stream.poll()) {
                    let mut metrics = metrics.borrow_mut();
                    for t in v.client_request_ids {
                        // We take the two deltas since the start
                        // and capture the difference to get the
                        // amount of time it took end-to-end
                        metrics.incr(since_start - t);
                    }
                }
                Ok(Async::Ready(None))
            })
        };

        periodic_report.select(replies).for_each(|_| Ok(()))
    }

    fn incr(&mut self, nanos: u64) {
        self.state.increment(nanos).unwrap();
    }

    fn snapshot(&mut self) -> Result<(), &str> {
        let (requests, p95, p99, p999, max) = {
            let v = (
                self.state.entries(),
                self.state.percentile(95.0)?,
                self.state.percentile(99.0)?,
                self.state.percentile(99.9)?,
                self.state.maximum()?,
            );
            self.state.clear();
            v
        };

        let req_per_sec = (requests as f32) / 10f32;
        let mb_per_sec = req_per_sec * (self.msg_size as f32) / 1_000_000f32;
        info!("AVG REQ/s :: {}, {} MB/s", req_per_sec, mb_per_sec);
        info!(
            "LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
            to_ms!(p95),
            to_ms!(p99),
            to_ms!(p999),
            to_ms!(max)
        );
        Ok(())
    }
}

struct BenchOptions {
    head_addr: String,
    tail_addr: String,
    throughput: u32,
    bytes: usize,
}

impl BenchOptions {
    fn parse() -> BenchOptions {
        // TODO: add multi-threading, add batching
        let args: Vec<String> = env::args().collect();
        let program = args[0].clone();

        let mut opts = Options::new();
        opts.optopt(
            "a",
            "head-address",
            "address of the head server",
            "HOST:PORT",
        );
        opts.optopt(
            "z",
            "tail-address",
            "address of the tail server",
            "HOST:PORT",
        );
        opts.optopt("t", "throughput", "number of connections per second", "N");
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

        let head_addr = matches.opt_str("a").unwrap_or("127.0.0.1:4000".to_string());
        let tail_addr = matches.opt_str("z").unwrap_or("127.0.0.1:4004".to_string());

        let throughput = matches.opt_str("t").unwrap_or("10".to_string());
        let throughput = u32::from_str_radix(throughput.as_str(), 10).unwrap();

        let bytes = matches.opt_str("b").unwrap_or("100".to_string());
        let bytes = u32::from_str_radix(bytes.as_str(), 10).unwrap() as usize;

        BenchOptions {
            head_addr,
            tail_addr,
            throughput,
            bytes,
        }
    }
}

enum AppenderState {
    Sending(AppendSentFuture),
    Waiting,
}

struct Appender {
    start_instant: Instant,
    conn: Connection,
    interval: Interval,
    state: AppenderState,
    rand: RandomSource,
}

impl Future for Appender {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            let next_state = match self.state {
                AppenderState::Sending(ref mut f) => {
                    try_ready!(f.poll().map_err(|_| ()));
                    AppenderState::Waiting
                }
                AppenderState::Waiting => {
                    try_ready!(self.interval.poll().map_err(|_| ()));
                    // IMPORTANT!!!!!
                    // To benchmark, we've selected the request ID to be the
                    // nanoseconds since the start in relative terms. When
                    // the reply is captured, the histogram is incremented
                    // with the time delta.
                    let since_start = Instant::now() - self.start_instant;
                    let req_id = since_start.as_nanos() as u64;
                    AppenderState::Sending(self.conn.raw_append(
                        0,
                        req_id,
                        self.rand.random_chars(),
                    ))
                }
            };
            self.state = next_state;
        }
    }
}

pub fn main() {
    env_logger::init();

    let opts = BenchOptions::parse();

    let mut client_config = Configuration::default();
    client_config.head(&opts.head_addr).unwrap();
    client_config.tail(&opts.tail_addr).unwrap();
    let client = LogServerClient::new(client_config);

    let mut rt = Runtime::new().unwrap();
    let start_instant = Instant::now();

    let msg_size = opts.bytes;
    rt.spawn(
        client
            .new_connection()
            .map_err(|e| {
                error!("Error opening connection: {}", e);
            })
            .and_then(move |conn| Metrics::spawn(conn, start_instant, msg_size)),
    );

    let mut throughput = opts.throughput;

    // spawn connections that run ever 1ms
    while throughput > 1000 {
        throughput -= 1000;

        let rand = RandomSource::new(opts.bytes);
        rt.spawn(
            client
                .new_connection()
                .map_err(|e| {
                    error!("Error opening connection: {}", e);
                })
                .and_then(move |conn| Appender {
                    conn,
                    start_instant,
                    state: AppenderState::Waiting,
                    interval: Interval::new(
                        start_instant + Duration::from_millis(1),
                        Duration::from_millis(1),
                    ),
                    rand,
                }),
        );
    }

    if throughput > 0 {
        let wait = Duration::from_millis((1000 / throughput).into());
        let rand = RandomSource::new(opts.bytes);
        rt.spawn(
            client
                .new_connection()
                .map_err(|e| {
                    error!("Error opening connection: {}", e);
                })
                .and_then(move |conn| Appender {
                    conn,
                    state: AppenderState::Waiting,
                    interval: Interval::new(start_instant + wait, wait),
                    rand,
                    start_instant,
                }),
        );
    }

    rt.run().unwrap();
}
