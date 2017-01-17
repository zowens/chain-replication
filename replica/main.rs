#![feature(core_intrinsics)]
extern crate getopts;
#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate byteorder;
extern crate commitlog;


use getopts::Options;
use futures::{Poll, Future};
use futures::future::BoxFuture;
use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_proto::multiplex::{ClientProto, RequestId, ClientService};
use tokio_proto::TcpClient;
use tokio_service::Service;
use byteorder::{ByteOrder, LittleEndian};
use commitlog::MessageSet;
use std::io::{self, Error};
use std::net::{ToSocketAddrs, SocketAddr};
use std::env;
use std::process::exit;

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

struct WrappedMessageSet {
    buf: EasyBuf,
}

impl MessageSet for WrappedMessageSet {
    fn bytes(&self) -> &[u8] {
        self.buf.as_slice()
    }
}


struct Request(/* starting offset */ u64);
struct Response(/* next starting offset */ u64);

struct Protocol;

impl Codec for Protocol {
    /// The type of decoded frames.
    type In = (RequestId, Response);

    /// The type of frames to be encoded.
    type Out = (RequestId, Request);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        trace!("Decode, size={}", buf.len());
        if probably_not!(buf.len() < 21) {
            return Ok(None);
        }

        let res = buf.drain_to(13);
        let response = res.as_slice();
        let len = LittleEndian::read_u32(&response[0..4]);
        let reqid = LittleEndian::read_u64(&response[4..12]);
        let opcode = response[12];

        assert!(len >= 13);
        let rest = buf.drain_to(len as usize - 13);

        match opcode {
            0 => {
                Err(io::Error::new(io::ErrorKind::Other, "Unexpected offset response"))
            },
            // TODO: this is buggy if we have 0 messages
            1 => {
                let msg_set = WrappedMessageSet {
                    buf: rest,
                };
                let last_off = msg_set.iter()
                    .last()
                    .map(|m| Some((reqid, Response(m.offset().0))));
                last_off
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No messages returned during replication"))
            },
            _ => {
                println!("Unknown response");
                Err(io::Error::new(io::ErrorKind::Other, "Unknown response type"))
            }
        }

    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        trace!("Writing request");
        let mut wbuf = [0u8; 21];
        // length
        LittleEndian::write_u32(&mut wbuf[0..4], 21);
        // request ID
        LittleEndian::write_u64(&mut wbuf[4..12], msg.0);
        // op code
        wbuf[12] = 3;
        // offset
        LittleEndian::write_u64(&mut wbuf[13..21], (msg.1).0);
        buf.extend_from_slice(&wbuf);
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
        Ok(io.framed(Protocol))
    }
}


struct ReplicaFuture {
    replicate_cmd: BoxFuture<Response, Error>,
    client: ClientService<TcpStream, LogProto>,
}

impl ReplicaFuture {
    fn run(client: ClientService<TcpStream, LogProto>) -> ReplicaFuture {
        let f = client.call(Request(0)).boxed();
        ReplicaFuture {
            replicate_cmd: f,
            client: client,
        }
    }
}

impl Future for ReplicaFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let Response(offset) = try_ready!(self.replicate_cmd.poll());
            self.replicate_cmd = self.client.call(Request(offset)).boxed();
        }
    }
}


fn parse_opts() -> SocketAddr {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "address of the server", "HOST:PORT");
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
    addr.to_socket_addrs().unwrap().next().unwrap()
}

pub fn main() {
    env_logger::init().unwrap();

    let addr = parse_opts();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let client = TcpClient::new(LogProto);

    core.run(client.connect(&addr, &handle)
        .and_then(ReplicaFuture::run))
       .unwrap();
}
