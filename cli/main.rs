#![allow(unknown_lints)]
extern crate getopts;
extern crate futures;
extern crate tokio_core;
extern crate env_logger;
extern crate client;

use std::io::{self, Write, BufRead};
use std::env;
use getopts::Options;
use std::process::exit;
use tokio_core::reactor::Core;
use client::{LogServerClient, Configuration, ReplyResponse};
use futures::Stream;


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

pub fn main() {
    env_logger::init().unwrap();

    let addr = parse_opts();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut client_config = Configuration::default();
    client_config.head(&addr).unwrap();
    let client = LogServerClient::new(client_config, handle);

    let conn = core.run(client.new_connection()).unwrap();

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
                core.run(conn.append(rest.bytes().collect())).unwrap();
                println!("ACK");
            }
            "latest" => {
                let latest = core.run(conn.latest_offset()).unwrap();
                if let Some(off) = latest {
                    println!("{}", off);
                } else {
                    println!("EMPTY");
                }
            }
            "read" => {
                match u64::from_str_radix(rest, 10) {
                    Ok(offset) => {
                        let msgs = core.run(conn.read(offset)).unwrap();
                        for m in msgs.iter() {
                            println!(
                                ":{} => {}",
                                m.offset(),
                                std::str::from_utf8(m.payload()).unwrap()
                            );
                        }
                    }
                    Err(_) => println!("ERROR Invalid offset"),
                }
            }
            "tail" => {
                core.run(
                    client
                        .new_tail_stream()
                        .map_err(|e| {
                            println!("I/O Error: {}", e);
                        })
                        .for_each(|reply| {
                            let ReplyResponse::AppendedMessages {
                                offset,
                                client_reqs,
                            } = reply;
                            for r in client_reqs.iter() {
                                println!("reply => {}", r);
                            }
                            println!("latest_offset: {}", offset);
                            Ok(())
                        }),
                ).unwrap();
            }
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
