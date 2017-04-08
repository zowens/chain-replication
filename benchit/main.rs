#![allow(unknown_lints)]
#![feature(core_intrinsics)]
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

use std::io::{self, Error};
use std::time;
use std::net::{ToSocketAddrs, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::env;
use rand::{Rng, XorShiftRng};
use getopts::Options;
use std::process::exit;
use futures::{Future, Stream, Async, Poll};
use futures::stream::{futures_unordered, FuturesUnordered, StreamFuture};
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder, Framed};
use bytes::{Buf, BufMut, BytesMut, LittleEndian, IntoBuf};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_proto::multiplex::{Multiplex, ClientService, ClientProto, RequestId};
use tokio_proto::{TcpClient, Connect};
use tokio_service::Service;
use byteorder::ByteOrder;

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            std::intrinsics::unlikely($e)
        }
    )
}

macro_rules! to_ms {
    ($e:expr) => (
        (($e as f32) / 1000000f32)
    )
}


#[derive(Default)]
struct Request;
struct Response(u64);

#[inline]
fn decode_header(buf: &mut BytesMut) -> Option<(RequestId, u8, BytesMut)> {
    trace!("Found {} chars in read buffer", buf.len());
    // must have at least 13 bytes
    if probably_not!(buf.len() < 13) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }


    // read the length of the message
    let len = LittleEndian::read_u32(&buf[0..4]) as usize;

    // ensure we have enough
    if probably_not!(buf.len() < len) {
        return None;
    }

    // drain to the length and request ID, then remove the length field
    let mut buf = buf.split_to(len);
    let mut header = buf.split_to(13).into_buf();
    // skip the length field
    header.advance(4);
    let reqid = header.get_u64::<LittleEndian>();
    let op = header.get_u8();
    Some((reqid, op, buf))
}

#[inline]
fn encode_header(reqid: RequestId, opcode: u8, rest: usize, buf: &mut BytesMut) {
    buf.reserve(rest + 13);
    buf.put_u32::<LittleEndian>(13 + rest as u32);
    buf.put_u64::<LittleEndian>(reqid);
    buf.put_u8(opcode);
}


struct Protocol(XorShiftRng);
impl Decoder for Protocol {
    type Item = (RequestId, Response);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        match decode_header(src) {
            Some((reqid, 0, buf)) => {
                if probably_not!(buf.len() < 8) {
                    return Err(io::Error::new(io::ErrorKind::Other, "Invalid length"));
                }
                let off = buf.into_buf().get_u64::<LittleEndian>();
                Ok(Some((reqid, Response(off))))
            }
            None => Ok(None),
            _ => {
                println!("Unknown response");
                Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid operation"))
            }
        }
    }
}

impl Encoder for Protocol {
    type Item = (RequestId, Request);
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), io::Error> {
        encode_header(item.0, 0, 100, dst);
        dst.extend(self.0.gen_ascii_chars().take(100).map(|c| c as u8));
        Ok(())
    }
}

#[derive(Default)]
struct LogProto;
impl ClientProto<TcpStream> for LogProto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<TcpStream, Protocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        trace!("Bind transport");
        try!(io.set_nodelay(true));
        trace!("Setting up protocol");
        Ok(io.framed(Protocol(XorShiftRng::new_unseeded())))
    }
}

#[derive(Clone)]
struct Metrics {
    state: Arc<Mutex<(u32, histogram::Histogram)>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        let metrics = Metrics { state: Arc::new(Mutex::new((0, histogram::Histogram::new()))) };

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
            let reqs = data.0;
            data.0 = 0;
            (reqs,
             data.1.percentile(95.0)?,
             data.1.percentile(99.0)?,
             data.1.percentile(99.9)?,
             data.1.maximum()?)
        };
        println!("AVG REQ/s :: {}",
                 (requests as f32) /
                 (since_last.as_secs() as f32 +
                  (since_last.subsec_nanos() as f32 / 1000000000f32)));

        println!("LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
                 to_ms!(p95),
                 to_ms!(p99),
                 to_ms!(p999),
                 to_ms!(max));

        Ok(())
    }
}

#[allow(or_fun_call)]
fn parse_opts() -> (SocketAddr, u32, u32) {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "address of the server", "HOST:PORT");
    opts.optopt("w", "threads", "number of connections", "N");
    opts.optopt("c",
                "concurrent-requests",
                "number of concurrent requests",
                "N");
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

    let addr = matches
        .opt_str("a")
        .unwrap_or("127.0.0.1:4000".to_string());

    let threads = matches.opt_str("w").unwrap_or("1".to_string());
    let threads = u32::from_str_radix(threads.as_str(), 10).unwrap();

    let concurrent = matches.opt_str("c").unwrap_or("2".to_string());
    let concurrent = u32::from_str_radix(concurrent.as_str(), 10).unwrap();

    (addr.to_socket_addrs().unwrap().next().unwrap(), threads, concurrent)
}

struct TrackedRequest<S: Service> {
    client: S,
    f: S::Future,
    metrics: Metrics,
    start: time::Instant,
}

impl<S> TrackedRequest<S>
    where S: Service<Request = Request, Response = Response, Error = Error>
{
    fn new(metrics: Metrics, client: S) -> TrackedRequest<S> {
        let f = client.call(Request);
        TrackedRequest {
            client: client,
            f: f,
            metrics: metrics,
            start: time::Instant::now(),
        }
    }
}

impl<S> Future for TrackedRequest<S>
    where S: Service<Request = Request, Response = Response, Error = Error>
{
    type Item = ();
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            try_ready!(self.f.poll());
            let stop = time::Instant::now();
            self.metrics.incr(stop.duration_since(self.start));
            self.f = self.client.call(Request);
            self.start = stop;
        }
    }
}

struct RunFuture {
    f: StreamFuture<FuturesUnordered<TrackedRequest<ClientService<TcpStream, LogProto>>>>,
}

impl RunFuture {
    fn spawn(metrics: Metrics, client: ClientService<TcpStream, LogProto>, n: u32) -> RunFuture {
        debug!("Spawning request");

        let mut reqs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            reqs.push(TrackedRequest::new(metrics.clone(), client.clone()));
        }
        RunFuture { f: futures_unordered(reqs).into_future() }
    }
}

impl Future for RunFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.f.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(_)) => Ok(Async::Ready(())),
            Err(_) => Err(io::Error::new(io::ErrorKind::Other, "something happened")),
        }
    }
}

enum ConnectionState {
    Connect(Metrics, u32, Connect<Multiplex, LogProto>),
    Run(RunFuture),
}

impl Future for ConnectionState {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (m, concurrent, conn) = match *self {
            ConnectionState::Connect(ref metrics, concurrent, ref mut f) => {
                let conn = try_ready!(f.poll());
                debug!("Connected");
                (metrics.clone(), concurrent, conn)
            }
            ConnectionState::Run(ref mut f) => {
                return f.poll();
            }
        };

        *self = ConnectionState::Run(RunFuture::spawn(m, conn, concurrent));
        self.poll()
    }
}

pub fn main() {
    env_logger::init().unwrap();

    let (addr, threads, concurrent) = parse_opts();

    let metrics = Metrics::new();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let client = TcpClient::new(LogProto);
    core.run(futures_unordered((0..threads).map(|_| {
                                                    ConnectionState::Connect(metrics.clone(),
                                         concurrent,
                                         client.connect(&addr, &handle))
                                                }))
                     .into_future())
        .map_err(|(e, _)| e)
        .unwrap();
}
