#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
extern crate futures;
extern crate getopts;
extern crate tokio_core;

use std::io::{self, BufRead, Write};
use std::env;
use std::thread;
use std::process::exit;
use getopts::Options;
use futures::{Future, Stream};
use futures::future::ok;
use futures::sync::oneshot;
use futures::sync::mpsc;
use tokio_core::reactor::Core;
use client::{Configuration, LogServerClient, Messages};

#[allow(or_fun_call)]
fn parse_opts() -> String {
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

    matches.opt_str("a").unwrap_or("127.0.0.1:4000".to_string())
}

enum Request {
    Append(Vec<u8>, oneshot::Sender<()>),
    Latest(oneshot::Sender<Option<u64>>),
    Read(u64, oneshot::Sender<Messages>),
}

fn run_request_thread(addr: String) -> mpsc::UnboundedSender<Request> {
    let (snd, recv) = mpsc::unbounded();
    let (set_done, done) = oneshot::channel::<()>();

    thread::spawn(move || {
        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let mut client_config = Configuration::default();
        client_config.head(&addr).unwrap();
        let client = LogServerClient::new(client_config, handle.clone());

        let mut conn = core.run(client.new_connection()).unwrap();
        set_done.send(()).unwrap_or(());

        let hdl = handle.clone();
        handle.spawn(
            recv.for_each(move |req| {
                match req {
                    Request::Append(bytes, res) => {
                        hdl.spawn(
                            conn.append(bytes)
                                .map(|v| {
                                    res.send(v).unwrap_or(());
                                    ()
                                })
                                .map_err(|_| ()),
                        );
                    }
                    Request::Latest(res) => {
                        hdl.spawn(
                            conn.latest_offset()
                                .map(|v| {
                                    res.send(v).unwrap_or(());
                                    ()
                                })
                                .map_err(|_| ()),
                        );
                    }
                    Request::Read(offset, res) => {
                        hdl.spawn(
                            conn.read(offset)
                                .map(|v| {
                                    res.send(v).unwrap_or(());
                                    ()
                                })
                                .map_err(|_| ()),
                        );
                    }
                }
                ok(())
            }).map_err(|_| ()),
        );

        loop {
            core.turn(None)
        }
    });

    done.wait().expect("Unable to connect");

    snd
}

macro_rules! unbound_send {
    ($sink:expr, $req:expr) => (
        <mpsc::UnboundedSender<Request>>::send(&$sink, $req).unwrap();
    )
}

pub fn main() {
    env_logger::init().unwrap();

    let addr = parse_opts();

    let conn = run_request_thread(addr.clone());

    loop {
        print!("{}> ", addr);
        io::stdout().flush().expect("Could not flush stdout");

        let stdin = io::stdin();
        let line = stdin
            .lock()
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
                let (snd, recv) = oneshot::channel::<()>();
                unbound_send!(conn, Request::Append(rest.bytes().collect(), snd));
                recv.wait().unwrap();
                println!("ACK");
            }
            "latest" => {
                let (snd, recv) = oneshot::channel::<Option<u64>>();
                unbound_send!(conn, Request::Latest(snd));
                let latest = recv.wait().unwrap();
                if let Some(off) = latest {
                    println!("{}", off);
                } else {
                    println!("EMPTY");
                }
            }
            "read" => match u64::from_str_radix(rest, 10) {
                Ok(offset) => {
                    let (snd, recv) = oneshot::channel::<Messages>();
                    unbound_send!(conn, Request::Read(offset, snd));
                    let msgs = recv.wait().unwrap();
                    for m in msgs.iter() {
                        println!(
                            ":{} => {}",
                            m.offset(),
                            std::str::from_utf8(m.payload()).unwrap()
                        );
                    }
                }
                Err(_) => println!("ERROR Invalid offset"),
            },
            "help" => {
                println!(
                    "
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
                "
                );
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
