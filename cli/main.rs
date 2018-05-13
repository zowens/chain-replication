#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
extern crate futures;
extern crate getopts;
extern crate tokio;
extern crate tokio_io;
#[macro_use]
extern crate log;

use client::{Configuration, LogServerClient};
use futures::future::ok;
use futures::{Future, Stream};
use getopts::Options;
use std::env;
use std::io::Error;
use std::process::exit;
use tokio::io;
use tokio::runtime::Runtime;
use tokio_io::codec::{FramedRead, LinesCodec};

const MAX_READ_BYTES: u32 = 4096;
const USAGE: &'static str = "
    append [payload...]
        Appends a message to the log.

    latest
        Queries the log for the latest offset.

    read [offset]
        Reads the log from the starting offset.

    help
        Shows this menu.

    tail
        Grabs the tail replies.

    quit
        Quits the application.
";

#[allow(or_fun_call)]
fn parse_opts() -> String {
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

    matches.opt_str("a").unwrap_or("127.0.0.1:4000".to_string())
}

fn request_stream(mut conn: client::Connection) -> impl Stream<Item = String, Error = Error> {
    FramedRead::new(io::stdin(), LinesCodec::new())
        .map(|line| {
            let word_pos = line.find(' ').unwrap_or_else(|| line.len());
            let (x, y) = line.split_at(word_pos);
            (x.trim().to_lowercase().to_owned(), y.trim().to_owned())
        })
        .and_then(
            move |(cmd, rest)| -> Box<Future<Item = String, Error = Error> + Send> {
                match cmd.as_str() {
                    "append" => Box::new(
                        conn.append(rest.bytes().collect())
                            .map(|_| String::default()),
                    ),
                    "latest" => Box::new(
                        conn.latest_offset()
                            .map(|off| off.map(|off| format!("{}", off)).unwrap_or_default()),
                    ),
                    "read" => match u64::from_str_radix(&rest, 10) {
                        Ok(offset) => {
                            Box::new(conn.read(offset, MAX_READ_BYTES).map(|msgs| {
                                let mut s = String::new();
                                for m in msgs.iter() {
                                    s.push_str(format!(":{} => ", m.0).as_str());
                                    s.push_str(std::str::from_utf8(&m.1).unwrap());
                                    s.push('\n');
                                }

                                // remove trailing newline (added back for generic command)
                                s.pop();
                                s
                            }))
                        }
                        Err(_) => Box::new(ok("ERROR: Invalid offset".to_owned())),
                    },
                    "help" => Box::new(ok(USAGE.to_owned())),
                    "quit" | "exit" => {
                        exit(0);
                    }
                    other => Box::new(ok(format!("Unknown command: {}\n{}", other, USAGE))),
                }
            },
        )
}

fn write_stdout(value: String) -> impl Future<Item = (), Error = Error> {
    io::write_all(io::stdout(), value)
        .and_then(|_| io::flush(io::stdout()))
        .map(|_| ())
}

pub fn main() {
    env_logger::init().unwrap();

    let mut rt = Runtime::new().unwrap();

    let addr = parse_opts();
    let mut client_config = Configuration::default();
    client_config.head(&addr).unwrap();
    let client = LogServerClient::new(client_config);

    let header = format!("{}> ", addr);

    let f = client
        .new_connection()
        // write the first header
        .and_then({
            let header = header.clone();
            move |conn| write_stdout(header).map(move |_| conn)
        })
        .and_then(move |conn| {
            info!("Connected");

            // take in the request, write the response then add another header
            request_stream(conn).for_each(move |mut res| {
                if !res.is_empty() {
                    res.push('\n');
                }

                res.push_str(&header);
                write_stdout(res)
            })
        })
        .map_err(|e| {
            error!("ERROR: {}", e);
            ()
        });

    rt.spawn(f);
    rt.shutdown_on_idle().wait().unwrap();
}
