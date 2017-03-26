#![allow(unknown_lints)]
#![feature(core_intrinsics)]
extern crate getopts;
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_io;
extern crate bytes;
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
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder, Framed};
use bytes::{Buf, BufMut, BytesMut, IntoBuf};
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

    let reqid = {
        let len_and_reqid = buf.split_to(12);
        LittleEndian::read_u64(&len_and_reqid[4..12])
    };

    // parse by op code
    let op = buf.split_to(1)[0];

    Some((reqid, op, buf))
}

#[inline]
fn encode_header(reqid: RequestId, opcode: u8, rest: usize, buf: &mut BytesMut) {
    buf.put_u32::<bytes::LittleEndian>(13 + rest as u32);
    buf.put_u64::<bytes::LittleEndian>(reqid);
    buf.put_u8(opcode);
}


struct Protocol;
impl Decoder for Protocol {
    type Item = (RequestId, Response);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        let reqid = match decode_header(src) {
            Some((reqid, 0, buf)) => {
                let off = buf.into_buf().get_u64::<bytes::LittleEndian>();
                println!("{}", off);
                reqid
            },
            Some((reqid, 1, buf)) => {
                match MessageBuf::from_bytes(buf.to_vec()) {
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
                reqid
            },
            None => return Ok(None),
            _ => {
                println!("Unknown response");
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid operation"));
            }
        };
        Ok(Some((reqid, Response)))
    }
}

impl Encoder for Protocol {
    type Item = (RequestId, Request);
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), io::Error> {
        match item {
            (reqid, Request::LatestOffset) => {
                encode_header(reqid, 1, 0, dst);
            }
            (reqid, Request::Append(bytes)) => {
                encode_header(reqid, 0, bytes.len(), dst);
                dst.put_slice(&bytes);
            }
            (reqid, Request::Read(offset)) => {
                encode_header(reqid, 2, 8, dst);
                dst.put_u64::<bytes::LittleEndian>(offset);
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
