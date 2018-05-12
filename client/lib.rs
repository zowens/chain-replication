extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate h2;
extern crate http;
extern crate prost;
extern crate rand;
extern crate tokio;
extern crate tokio_io;
extern crate tower_grpc;
extern crate tower_h2;
extern crate tower_http;
extern crate tower_service;
#[macro_use]
extern crate prost_derive;

mod append;
mod proto;

use futures::future::Join;
use futures::{Async, Future, Poll};
use http::Uri;
use std::io;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use tokio::net::{ConnectFuture, TcpStream};
use tokio::spawn;
use tower_grpc::codegen::client::grpc::Request;
use tower_h2::client::Background;
use tower_h2::{
    client::{Connection as TowerConnection, Handshake},
    BoxBody,
};
use tower_http::{add_origin, AddOrigin};

pub use proto::{AppendSentFuture, LatestOffsetFuture, QueryFuture};

type StorageHttpService = AddOrigin<TowerConnection<TcpStream, ExecutorAdapter, BoxBody>>;
type StorageClient = proto::client::LogStorage<StorageHttpService>;

enum AppendFutureState {
    Sending(AppendSentFuture, append::Receiver),
    Waiting(append::Receiver),
    Empty,
}

pub struct AppendFuture(AppendFutureState);

impl Future for AppendFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            match mem::replace(&mut self.0, AppendFutureState::Empty) {
                AppendFutureState::Sending(mut f, recv) => match f.poll()? {
                    Async::Ready(_) => {
                        self.0 = AppendFutureState::Waiting(recv);
                    }
                    Async::NotReady => {
                        self.0 = AppendFutureState::Sending(f, recv);
                        return Ok(Async::NotReady);
                    }
                },
                AppendFutureState::Waiting(mut recv) => match recv.poll() {
                    Ok(Async::Ready(_)) | Err(_) => {
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        self.0 = AppendFutureState::Waiting(recv);
                        return Ok(Async::NotReady);
                    }
                },
                AppendFutureState::Empty => unreachable!(),
            };
        }
    }
}

pub struct Connection {
    req_mgr: append::RequestManager,
    head_conn: StorageClient,
    tail_conn: StorageClient,
}

impl Connection {
    pub fn append(&mut self, body: Vec<u8>) -> AppendFuture {
        use proto::AppendRequest;
        let (client_request_id, res) = self.req_mgr.push_req();
        let f = self.head_conn.append(Request::new(AppendRequest {
            client_id: self.req_mgr.client_id(),
            client_request_id,
            payload: body,
        }));

        AppendFuture(AppendFutureState::Sending(f.into(), res))
    }

    pub fn read(&mut self, start_offset: u64, max_bytes: u32) -> QueryFuture {
        use proto::QueryRequest;
        self.tail_conn
            .query_log(Request::new(QueryRequest {
                start_offset,
                max_bytes,
            }))
            .into()
    }

    pub fn latest_offset(&mut self) -> LatestOffsetFuture {
        use proto::LatestOffsetQuery;
        self.tail_conn
            .latest_offset(Request::new(LatestOffsetQuery {}))
            .into()
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
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found"))?;
        Ok(self)
    }

    pub fn tail<A: ToSocketAddrs>(&mut self, addrs: A) -> Result<&mut Configuration, io::Error> {
        self.tail = addrs
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found"))?;
        Ok(self)
    }
}

pub struct LogServerClient {
    config: Configuration,
}

impl LogServerClient {
    pub fn new(config: Configuration) -> LogServerClient {
        LogServerClient { config }
    }

    pub fn new_connection(&self) -> ClientConnectFuture {
        let head = ServerConnection::TcpConnect(TcpStream::connect(&self.config.head));
        let tail = ServerConnection::TcpConnect(TcpStream::connect(&self.config.tail));
        ClientConnectFuture(ClientConnectState::Connecting(head.join(tail)))
    }
}

pub struct ClientConnectFuture(ClientConnectState);

enum ClientConnectState {
    Connecting(Join<ServerConnection, ServerConnection>),
    OpenReplyStream {
        future: append::ReplyStartFuture<StorageHttpService>,
        head_conn: StorageClient,
        tail_conn: StorageClient,
    },
    Empty,
}

impl Future for ClientConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        use ClientConnectState::*;
        loop {
            match mem::replace(&mut self.0, ClientConnectState::Empty) {
                Connecting(mut f) => match f.poll()? {
                    Async::Ready((head_conn, mut tail_conn)) => {
                        debug!("Head and tail connected, opening reply stream...");
                        let future = append::RequestManager::start(&mut tail_conn);
                        self.0 = ClientConnectState::OpenReplyStream {
                            future,
                            head_conn,
                            tail_conn,
                        }
                    }
                    Async::NotReady => {
                        trace!("Connection not ready");
                        self.0 = ClientConnectState::Connecting(f);
                        return Ok(Async::NotReady);
                    }
                },
                OpenReplyStream {
                    mut future,
                    head_conn,
                    tail_conn,
                } => match future.poll()? {
                    Async::Ready(req_mgr) => {
                        debug!("Reply stream opened");
                        return Ok(Async::Ready(Connection {
                            head_conn,
                            tail_conn,
                            req_mgr,
                        }));
                    }
                    Async::NotReady => {
                        trace!("Open not ready");
                        self.0 = ClientConnectState::OpenReplyStream {
                            future,
                            head_conn,
                            tail_conn,
                        };
                        return Ok(Async::NotReady);
                    }
                },
                Empty => unreachable!(),
            }
        }
    }
}

enum ServerConnection {
    TcpConnect(ConnectFuture),
    HttpHandshake(Handshake<TcpStream, ExecutorAdapter, BoxBody>, Uri),
}

impl Future for ServerConnection {
    type Item = StorageClient;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let next_state = match *self {
                ServerConnection::TcpConnect(ref mut f) => {
                    let c = try_ready!(f.poll());
                    let uri = format!("http://{}/", c.peer_addr().unwrap())
                        .parse::<Uri>()
                        .unwrap();

                    trace!("Connection opened, performing HTTP hadshake");
                    ServerConnection::HttpHandshake(
                        TowerConnection::handshake(c, ExecutorAdapter),
                        uri,
                    )
                }
                ServerConnection::HttpHandshake(ref mut f, ref uri) => {
                    let conn = try_ready!(f.poll().map_err(|_| io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid handshake"
                    )));
                    let conn = add_origin::Builder::new().uri(uri).build(conn).unwrap();

                    trace!("HTTP hadshake completed");
                    return Ok(Async::Ready(proto::client::LogStorage::new(conn)));
                }
            };
            *self = next_state;
        }
    }
}

#[derive(Clone)]
struct ExecutorAdapter;
type BackgroundFuture = Background<TcpStream, BoxBody>;

impl futures::future::Executor<BackgroundFuture> for ExecutorAdapter {
    fn execute(
        &self,
        f: BackgroundFuture,
    ) -> Result<(), futures::future::ExecuteError<BackgroundFuture>> {
        // TODO: remove this hacky-ness around the various Executor traits
        spawn(Box::new(f));
        Ok(())
    }
}
