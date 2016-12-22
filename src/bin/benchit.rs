extern crate rand;
extern crate histogram;
extern crate getopts;

use std::time;
use std::net;
use std::io::{Write, Read};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;
use getopts::Options;
use std::env;
use std::process::exit;


macro_rules! to_ms {
    ($e:expr) => (
        (($e as f32) / 1000000f32)
    )
}

#[derive(Clone)]
struct Metrics {
    state: Arc<Mutex<(u32, histogram::Histogram)>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics {
            state: Arc::new(Mutex::new((0, histogram::Histogram::new()))),
        }
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

    pub fn snapshot(&self, since_last: time::Duration) {
        let (requests, p95, p99, p999, max) = {
            let mut data = self.state.lock().unwrap();
            let reqs = data.0;
            data.0 = 0;
            (reqs, data.1.percentile(95.0).unwrap(), data.1.percentile(99.0).unwrap(), data.1.percentile(99.9).unwrap(), data.1.maximum().unwrap())
        };
        println!("AVG REQ/s :: {}",
                (requests as f32) / (since_last.as_secs() as f32 + (since_last.subsec_nanos() as f32 / 1000000000f32)));

        println!("LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
            to_ms!(p95),
            to_ms!(p99),
            to_ms!(p999),
            to_ms!(max));
    }
}

fn parse_opts() -> (String, u32) {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "address of the server", "HOST:PORT");
    opts.optopt("w", "threads", "number of connections", "N");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options]", program);
        print!("{}", opts.usage(&brief));
        exit(1);
    }

    let addr = matches.opt_str("a").unwrap_or("127.0.0.1:4000".to_string());

    let threads = matches.opt_str("w").unwrap_or("1".to_string());
    let threads = u32::from_str_radix(threads.as_str(), 10).unwrap();

    (addr, threads)
}

pub fn main() {
    let (addr, threads) = parse_opts();

    let metrics = Metrics::new();
    let mut last_report = time::Instant::now();

    for _ in 0..threads {
        let metrics = metrics.clone();
        let addr = addr.clone();
        thread::spawn(move || {
            let mut stream = net::TcpStream::connect(addr.as_str()).unwrap();
            stream.set_nodelay(true).unwrap();
            let mut buf = [0; 128];
            let mut rng = rand::thread_rng();

            loop {
                let s: String = rng.gen_ascii_chars().take(100).collect();
                let start = time::Instant::now();
                write!(&mut stream, "{}\n", s).unwrap();
                let size = stream.read(&mut buf).unwrap();
                let end = time::Instant::now();
                if size == 0 || buf[0] != b'+' {
                    println!("ERROR: Got {}", String::from_utf8_lossy(&buf[0..size]));
                    break;
                }

                metrics.incr(end.duration_since(start));
            }
        });
    }

    loop {
        thread::sleep(time::Duration::from_secs(10));
        let now = time::Instant::now();
        metrics.snapshot(now.duration_since(last_report));
        last_report = now;
    }

}
