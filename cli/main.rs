#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
extern crate futures;
extern crate getopts;
extern crate tokio;
extern crate tokio_io;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

use client::{Configuration, LogServerClient};
use futures::future::{lazy, ok, FutureResult};
use futures::sync::{mpsc, oneshot};
use futures::{Future, Stream};
use getopts::Options;
use std::env;
use std::io::Error;
use std::process::exit;
use tokio::executor::thread_pool::ThreadPool;
use tokio::io;
use tokio::runtime::current_thread::Runtime;

const MAX_READ_BYTES: u32 = 4096;
const USAGE: &str = "
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

lazy_static! {
    static ref THREAD_POOL: ThreadPool = ThreadPool::new();
}

#[allow(or_fun_call)]
fn parse_opts() -> String {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "a",
        "head-address",
        "address of the management server",
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

    matches.opt_str("a").unwrap_or("127.0.0.1:5000".to_string())
}

#[allow(unreachable_code)]
fn line_stream() -> impl Stream<Item = String, Error = Error> {
    let (snd, recv) = mpsc::unbounded();
    THREAD_POOL.spawn(lazy(move || -> FutureResult<(), ()> {
        use std::io::stdin;
        let instream = stdin();
        loop {
            let mut line = String::new();
            instream.read_line(&mut line).unwrap();
            snd.unbounded_send(line).unwrap();
        }

        Ok(()).into()
    }));
    recv.map_err(|_| io::Error::new(io::ErrorKind::Interrupted, "Cancelled"))
}

fn write_stdout(value: String) -> impl Future<Item = (), Error = Error> {
    let (snd, recv) = oneshot::channel();
    THREAD_POOL.spawn(lazy(move || {
        use std::io::stdout;
        use std::io::Write;
        let mut out = stdout();

        out.write_all(value.as_bytes())
            .and_then(|_| out.flush())
            .map_err(|e| error!("Err writing to stdout: {:?}", e))
            .and_then(|_| snd.send(()).map_err(|_| ()))
            .map(|_| trace!("Wrote to stdout"))

        //ok(())
    }));

    recv.map_err(|_| io::Error::new(io::ErrorKind::Interrupted, "Cancelled"))
}

fn request_stream(mut conn: client::Connection) -> impl Stream<Item = String, Error = Error> {
    line_stream()
        .map(|line| {
            let word_pos = line.find(' ').unwrap_or_else(|| line.len());
            let (x, y) = line.split_at(word_pos);
            (x.trim().to_lowercase().to_owned(), y.trim().to_owned())
        })
        .and_then(
            move |(cmd, rest)| -> Box<Future<Item = String, Error = Error> + Send> {
                match cmd.as_str() {
                    "append" => Box::new(conn.append(rest.into()).map(|_| String::default())),
                    "latest" => Box::new(conn.latest_offset().map(|off| {
                        trace!("GOT OFFSET: {:?}", off);
                        off.map(|off| format!("{}", off)).unwrap_or_default()
                    })),
                    "read" => match u64::from_str_radix(&rest, 10) {
                        Ok(offset) => {
                            Box::new(conn.read(offset, MAX_READ_BYTES).map(|msgs| {
                                let mut s = String::new();
                                for m in &msgs {
                                    s.push_str(format!(":{} => ", m.0).as_str());
                                    match std::str::from_utf8(&m.1) {
                                        Ok(v) => s.push_str(v),
                                        Err(_) => {
                                            for &byte in m.1.iter() {
                                                s.push_str(&format!("{:X} ", byte));
                                            }
                                        }
                                    }
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

pub fn main() {
    env_logger::init();

    let mut rt = Runtime::new().unwrap();

    let management_server_addr = parse_opts();
    let mut client_config = Configuration::default();
    client_config
        .management_server(&management_server_addr)
        .unwrap();
    let client = LogServerClient::new(client_config);

    let header = "> ".to_string();

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

    rt.block_on(f).unwrap();
}
