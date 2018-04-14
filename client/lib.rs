#![allow(unknown_lints)]
#![feature(core_intrinsics)]
extern crate byteorder;
extern crate bytes;
extern crate commitlog;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate rand;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

mod msgs;
mod proto;
mod reply;
pub use msgs::*;

use std::mem;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use futures::{Async, Future, Poll};
use futures::future::Join;
use tokio_core::reactor::Handle;
use tokio_proto::streaming::multiplex::StreamingMultiplex;
use tokio_proto::util::client_proxy::Response as ClientFuture;
use tokio_proto::streaming::Message;
use tokio_proto::TcpClient;
use tokio_service::Service;
use proto::*;
use bytes::BytesMut;


pub struct RequestFuture<T> {
    f: ClientFuture<ResponseMsg, io::Error>,
    m: fn(Response) -> Option<T>,
}

impl<T> Future for RequestFuture<T> {
    type Item = T;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<T, io::Error> {
        // waiting on `impl Future` to be cross-crate....

        match try_ready!(self.f.poll()) {
            Message::WithoutBody(res) => if let Some(v) = (self.m)(res) {
                Ok(Async::Ready(v))
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "Unexpected response"))
            },
            Message::WithBody(_, _) => {
                error!("Expected messange without body");
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Expected response with no body",
                ))
            }
        }
    }
}

enum AppendFutureState {
    Sending(ClientFuture<ResponseMsg, io::Error>, reply::Receiver),
    Waiting(reply::Receiver),
    Empty,
}

pub struct AppendFuture {
    state: AppendFutureState,
}

impl Future for AppendFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            match mem::replace(&mut self.state, AppendFutureState::Empty) {
                AppendFutureState::Sending(mut f, recv) => match f.poll()? {
                    Async::Ready(_) => {
                        self.state = AppendFutureState::Waiting(recv);
                    }
                    Async::NotReady => {
                        self.state = AppendFutureState::Sending(f, recv);
                        return Ok(Async::NotReady);
                    }
                },
                AppendFutureState::Waiting(mut recv) => match recv.poll() {
                    Ok(Async::Ready(_)) | Err(_) => {
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        self.state = AppendFutureState::Waiting(recv);
                        return Ok(Async::NotReady);
                    }
                },
                AppendFutureState::Empty => unreachable!(),
            };
        }
    }
}


#[derive(Clone)]
pub struct Connection {
    reply_mgr: reply::ReplyManager,
    head_conn: ProtoConnection,
    tail_conn: ProtoConnection,
}

impl Connection {
    pub fn append(&mut self, body: Vec<u8>) -> AppendFuture {
        let (client_req_id, res) = self.reply_mgr.push_req();
        let f = self.head_conn.call(Message::WithoutBody(Request::Append {
            client_id: self.reply_mgr.client_id(),
            client_req_id,
            payload: body,
        }));
        AppendFuture {
            state: AppendFutureState::Sending(f, res),
        }
    }

    pub fn append_buf(&mut self, body: BytesMut) -> AppendFuture {
        let (client_req_id, res) = self.reply_mgr.push_req();
        let f = self.head_conn
            .call(Message::WithoutBody(Request::AppendBytes {
                client_id: self.reply_mgr.client_id(),
                client_req_id,
                payload: body,
            }));
        AppendFuture {
            state: AppendFutureState::Sending(f, res),
        }
    }

    pub fn read(&self, starting_offset: u64) -> RequestFuture<Messages> {
        RequestFuture {
            f: self.tail_conn
                .call(Message::WithoutBody(Request::Read { starting_offset })),
            m: |res| match res {
                Response::Messages(msgs) => Some(msgs),
                _ => None,
            },
        }
    }

    pub fn latest_offset(&self) -> RequestFuture<Option<u64>> {
        RequestFuture {
            f: self.tail_conn
                .call(Message::WithoutBody(Request::LatestOffset)),
            m: |res| match res {
                Response::Offset(off) => Some(off),
                _ => None,
            },
        }
    }
}



#[derive(Debug, Clone, Hash, PartialEq)]
pub struct Configuration {
    head: SocketAddr,
    tail: SocketAddr,
}

impl Default for Configuration {
    fn default() -> Configuration {
        Configuration {
            head: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000),
            tail: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4004),
        }
    }
}

impl Configuration {
    pub fn head<A: ToSocketAddrs>(&mut self, addrs: A) -> Result<&mut Configuration, io::Error> {
        self.head = addrs.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found")
        })?;
        Ok(self)
    }

    pub fn tail<A: ToSocketAddrs>(&mut self, addrs: A) -> Result<&mut Configuration, io::Error> {
        self.tail = addrs.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found")
        })?;
        Ok(self)
    }
}

pub struct LogServerClient {
    client: TcpClient<StreamingMultiplex<EmptyStream>, LogServerProto>,
    config: Configuration,
    handle: Handle,
    reply_mgr: reply::ReplyManager,
}

impl LogServerClient {
    pub fn new(config: Configuration, handle: Handle) -> LogServerClient {
        let reply_mgr = reply::ReplyManager::new(&handle, &config.tail);
        LogServerClient {
            client: TcpClient::new(LogServerProto),
            handle,
            config,
            reply_mgr,
        }
    }

    pub fn new_connection(&self) -> impl Future<Item = Connection, Error = io::Error> {
        ConnectFuture {
            f: self.client
                .connect(&self.config.head, &self.handle)
                .join(self.client.connect(&self.config.tail, &self.handle)),
            reply_mgr: Some(self.reply_mgr.clone()),
        }
    }
}

struct ConnectFuture {
    f: Join<Connect, Connect>,
    reply_mgr: Option<reply::ReplyManager>,
}

impl Future for ConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (head_conn, tail_conn) = try_ready!(self.f.poll());
        Ok(Async::Ready(Connection {
            head_conn,
            tail_conn,
            reply_mgr: self.reply_mgr.take().unwrap(),
        }))
    }
}
