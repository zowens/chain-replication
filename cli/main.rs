#![allow(unknown_lints)]
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

use std::io::{self, Write, BufRead};
use std::net::{ToSocketAddrs, SocketAddr};
use std::env;
use getopts::Options;
use std::process::exit;
use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_proto::multiplex::{ClientProto, RequestId};
use tokio_proto::TcpClient;
use tokio_service::Service;
use byteorder::{ByteOrder, LittleEndian};
use commitlog::message::{MessageBuf, MessageSet};

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


enum Request {
    Append(Vec<u8>),
    Read(u64),
    LatestOffset,
}

struct Response;
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
                let offset = LittleEndian::read_u64(rest.as_slice());
                println!("{}", offset);
            }
            // TODO: this is buggy if we have 0 messages
            1 => {
                match MessageBuf::from_bytes(Vec::from(rest.as_slice())) {
                    Ok(msg_set) => {
                        for m in msg_set.iter() {
                            println!(":{} => {}",
                                     m.offset(),
                                     std::str::from_utf8(m.payload()).unwrap());
                        }
                    }
                    Err(e) => {
                        println!("ERROR: Invalid message set returned.\n{:?}", e);
                    }
                }
            }
            _ => {
                println!("Unknown response");
            }
        }

        Ok(Some((reqid, Response)))
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        trace!("Writing request");

        match msg {
            (reqid, Request::LatestOffset) => {
                let mut wbuf = [0u8; 12];
                LittleEndian::write_u32(&mut wbuf[0..4], 13);
                LittleEndian::write_u64(&mut wbuf[4..12], reqid);
                // add length and request ID
                buf.extend_from_slice(&wbuf);
                // add op code
                buf.push(1u8);
            }
            (reqid, Request::Append(bytes)) => {
                let mut wbuf = [0u8; 12];
                LittleEndian::write_u32(&mut wbuf[0..4], 13 + bytes.len() as u32);
                LittleEndian::write_u64(&mut wbuf[4..12], reqid);
                // add length and request ID
                buf.extend_from_slice(&wbuf);
                // add op code
                buf.push(0u8);
                buf.extend_from_slice(&bytes);
            }
            (reqid, Request::Read(offset)) => {
                let mut wbuf = [0u8; 21];
                LittleEndian::write_u32(&mut wbuf[0..4], 21);
                LittleEndian::write_u64(&mut wbuf[4..12], reqid);
                wbuf[12] = 2;
                LittleEndian::write_u64(&mut wbuf[13..21], offset);
                buf.extend_from_slice(&wbuf);
            }
        }

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

#[allow(or_fun_call)]
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

    let conn = core.run(client.connect(&addr, &handle)).unwrap();

    loop {
        print!("{}> ", addr);
        io::stdout().flush().expect("Could not flush stdout");

        let stdin = io::stdin();
        let line = stdin.lock()
            .lines()
            .next()
            .expect("there was no next line")
            .expect("the line could not be read");

        let (cmd, rest) = {
            let word_pos = line.find(' ').unwrap_or_else(|| line.len());
            let (x, y) = line.split_at(word_pos);
            (x.trim().to_lowercase(), y.trim())
        };

        match cmd.as_str() {
            "append" => {
                core.run(conn.call(Request::Append(rest.bytes().collect()))).unwrap();
            }
            "latest" => {
                core.run(conn.call(Request::LatestOffset)).unwrap();
            }
            "read" => {
                match u64::from_str_radix(rest, 10) {
                    Ok(offset) => {
                        core.run(conn.call(Request::Read(offset))).unwrap();
                    }
                    Err(_) => println!("ERROR Invalid offset"),
                }
            }
            "help" => {
                println!("
    append [payload...]
        Appends a message to the log.

    latest
        Queries the log for the latest offset.

    read [offset]
        Reads the log from the starting offset.

    help
        Shows this menu.

    quit
        Quits the application.
                ");
            }
            "quit" | "exit" => {
                return;
            }
            _ => {
                println!("Unknown command. Use 'help' for usage");
            }
        }

    }


}
