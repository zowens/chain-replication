#![allow(unknown_lints)]
#![feature(core_intrinsics,conservative_impl_trait)]
#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_io;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate commitlog;
extern crate rand;

mod msgs;
mod proto;
pub use msgs::*;
pub use proto::ReplyResponse;

use std::io;
use std::net::{SocketAddr, ToSocketAddrs, IpAddr, Ipv4Addr};
use futures::{Stream, Future, Poll, Async};
use futures::future::{ok, err};
use tokio_core::reactor::Handle;
use tokio_proto::streaming::multiplex::StreamingMultiplex;
use tokio_proto::util::client_proxy::Response as ClientFuture;
use tokio_proto::streaming::Message;
use tokio_proto::{TcpClient, Connect};
use tokio_service::Service;
use proto::*;
use bytes::BytesMut;
use rand::{OsRng, Rng};


pub struct RequestFuture<T> {
    f: ClientFuture<ResponseMsg, io::Error>,
    m: fn(Response) -> Option<T>,
}

impl<T> Future for RequestFuture<T> {
    type Item = T;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<T, io::Error> {
        match try_ready!(self.f.poll()) {
            Message::WithoutBody(res) => {
                if let Some(v) = (self.m)(res) {
                    Ok(Async::Ready(v))
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "Unexpected response"))
                }
            }
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


#[derive(Clone)]
pub struct Connection {
    client_id: u32,
    conn: ProtoConnection,
}

impl Connection {
    pub fn append(&self, body: Vec<u8>) -> RequestFuture<()> {
        RequestFuture {
            f: self.conn.call(Message::WithoutBody(Request::Append {
                client_id: self.client_id,
                // TODO: fix this
                client_req_id: 0,
                payload: body,
            })),
            m: |res| match res {
                Response::ACK => Some(()),
                _ => None,
            },
        }
    }

    pub fn append_buf(&self, body: BytesMut) -> RequestFuture<()> {
        RequestFuture {
            f: self.conn.call(Message::WithoutBody(Request::AppendBytes {
                client_id: self.client_id,
                // TODO: fix this
                client_req_id: 0,
                payload: body,
            })),
            m: |res| match res {
                Response::ACK => Some(()),
                _ => None,
            },
        }
    }

    pub fn read(&self, starting_offset: u64) -> RequestFuture<Messages> {
        RequestFuture {
            f: self.conn
                .call(Message::WithoutBody(Request::Read { starting_offset })),
            m: |res| match res {
                Response::Messages(msgs) => Some(msgs),
                _ => None,
            },
        }
    }

    pub fn latest_offset(&self) -> RequestFuture<Option<u64>> {
        RequestFuture {
            f: self.conn.call(Message::WithoutBody(Request::LatestOffset)),
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
        self.head = addrs
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found")
            })?;
        Ok(self)
    }

    pub fn tail<A: ToSocketAddrs>(&mut self, addrs: A) -> Result<&mut Configuration, io::Error> {
        self.tail = addrs
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found")
            })?;
        Ok(self)
    }
}

pub struct LogServerClient {
    client: TcpClient<StreamingMultiplex<EmptyStream>, LogServerProto>,
    config: Configuration,
    handle: Handle,
    client_id: u32,
}

impl LogServerClient {
    pub fn new(config: Configuration, handle: Handle) -> LogServerClient {
        // TODO: this + configuration should come from master/configurator process
        let client_id = OsRng::new().unwrap().next_u32();
        LogServerClient {
            client: TcpClient::new(LogServerProto),
            handle,
            config,
            client_id,
        }
    }

    pub fn new_connection(&self) -> impl Future<Item = Connection, Error = io::Error> {
        ConnectFuture {
            f: self.client.connect(&self.config.head, &self.handle),
            client_id: self.client_id,
        }
    }

    pub fn new_tail_stream(&self) -> impl Stream<Item = ReplyResponse, Error = io::Error> {
        let client_id = self.client_id;
        self.client
            .connect(&self.config.tail, &self.handle)
            .and_then(move |client| {
                client
                    .call(Message::WithoutBody(Request::RequestTailReply {
                        client_id,
                        last_known_offset: 0,
                    }))
                    .and_then(move |msg| match msg {
                        Message::WithBody(Response::TailReplyStarted, body) => ok(
                            ClientWrappedStream(client, body),
                        ),
                        _ => err(io::Error::new(
                            io::ErrorKind::Other,
                            "Unknown response for tail reply request",
                        )),
                    })
            })
            .into_stream()
            .flatten()
    }
}

struct ConnectFuture {
    f: Connect<StreamingMultiplex<EmptyStream>, LogServerProto>,
    client_id: u32,
}

impl Future for ConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let conn = try_ready!(self.f.poll());
        Ok(Async::Ready(Connection {
            conn,
            client_id: self.client_id,
        }))
    }
}

struct ClientWrappedStream(ProtoConnection, ReplyStream);

impl Stream for ClientWrappedStream {
    type Item = ReplyResponse;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.1.poll()
    }
}
