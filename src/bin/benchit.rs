extern crate rand;
extern crate histogram;

use std::time;
use std::net;
use std::io::{Write, Read};
use rand::Rng;

macro_rules! to_ms {
    ($e:expr) => (
        (($e as f32) / 1000000f32)
    )
}

pub fn main() {
    let mut stream = net::TcpStream::connect("127.0.0.1:4000").unwrap();
    stream.set_nodelay(true).unwrap();
    let mut buf = [0; 128];
    let mut rng = rand::thread_rng();

    let mut latency = histogram::Histogram::new();
    let mut last_report = time::Instant::now();
    let mut num_requests = 0u32;

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

        num_requests += 1;

        let duration = end.duration_since(start);
        if duration.as_secs() > 0 {
            println!("WARN: {}s latency", duration.as_secs());
        } else {
            latency.increment(duration.subsec_nanos() as u64).unwrap();
        }

        let report_dur = end.duration_since(last_report);
        if report_dur.as_secs() > 10 {
            println!("AVG REQ/s :: {}",
                (num_requests as f32) / (report_dur.as_secs() as f32 + (report_dur.subsec_nanos() as f32 / 1000000000f32)));

            println!("LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
                to_ms!(latency.percentile(95.0).unwrap()),
                to_ms!(latency.percentile(99.0).unwrap()),
                to_ms!(latency.percentile(99.9).unwrap()),
                to_ms!(latency.maximum().unwrap()));


            num_requests = 0;
            last_report = end;
        }
    }
}
