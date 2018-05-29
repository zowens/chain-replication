#![feature(test)]
extern crate bytes;
extern crate test;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate fnv;
extern crate grpcio;
extern crate protobuf;
extern crate rand;
extern crate tokio;

mod append;
mod protocol;

use bytes::Bytes;
use futures::future::Join;
use futures::{Async, Future, Poll};
use grpcio::{ChannelBuilder, EnvBuilder, Environment};
use protocol::*;
use std::io;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

pub use protocol::{AppendSentFuture, LatestOffsetFuture, QueryFuture, Reply, ReplyStream};

pub struct AppendFuture(AppendFutureState, append::Receiver);

enum AppendFutureState {
    Sending(AppendSentFuture),
    Waiting,
}

impl Future for AppendFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            match mem::replace(&mut self.0, AppendFutureState::Waiting) {
                AppendFutureState::Sending(mut f) => match f.poll()? {
                    Async::Ready(_) => {}
                    Async::NotReady => {
                        self.0 = AppendFutureState::Sending(f);
                        return Ok(Async::NotReady);
                    }
                },
                AppendFutureState::Waiting => match self.1.poll() {
                    Ok(Async::Ready(_)) | Err(_) => {
                        // TODO: handle err
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                },
            };
        }
    }
}

pub struct Connection {
    req_mgr: append::RequestManager,
    head_conn: LogStorageClient,
    tail_conn: LogStorageClient,
}

impl Connection {
    pub fn append(&mut self, body: Bytes) -> AppendFuture {
        let (client_request_id, res) = self.req_mgr.push_req();

        let mut append_req = AppendRequest::new();
        append_req.set_payload(body);
        append_req.set_client_id(self.req_mgr.client_id());
        append_req.set_client_request_id(client_request_id);

        let sent = AppendSentFuture::new(self.head_conn.append_async(&append_req));
        AppendFuture(AppendFutureState::Sending(sent), res)
    }

    pub fn raw_append(
        &mut self,
        client_id: u64,
        client_request_id: u64,
        payload: Bytes,
    ) -> AppendSentFuture {
        let mut append_req = AppendRequest::new();
        append_req.set_payload(payload);
        append_req.set_client_id(client_id);
        append_req.set_client_request_id(client_request_id);

        AppendSentFuture::new(self.head_conn.append_async(&append_req))
    }

    pub fn raw_replies(&mut self, client_id: u64) -> ReplyStream {
        let mut reply_req = ReplyRequest::new();
        reply_req.set_client_id(client_id);
        self.tail_conn.replies(&reply_req).unwrap().into()
    }

    pub fn read(&mut self, start_offset: u64, max_bytes: u32) -> QueryFuture {
        let mut read_req = QueryRequest::new();
        read_req.set_start_offset(start_offset);
        read_req.set_max_bytes(max_bytes);
        QueryFuture::new(self.tail_conn.query_log_async(&read_req))
    }

    pub fn latest_offset(&mut self) -> LatestOffsetFuture {
        let query = LatestOffsetQuery::new();
        LatestOffsetFuture::new(self.tail_conn.latest_offset_async(&query))
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
    env: Arc<Environment>,
}

impl LogServerClient {
    pub fn new(config: Configuration) -> LogServerClient {
        grpcio::redirect_log();
        LogServerClient {
            config,
            env: Arc::new(EnvBuilder::new().build()),
        }
    }

    fn connect(&self, addr: &SocketAddr) -> LogStorageClient {
        let cb = ChannelBuilder::new(self.env.clone())
            .default_compression_algorithm(grpcio::CompressionAlgorithms::None)
            .max_concurrent_stream(100000)
            .http2_bdp_probe(true);
        let conn = cb.connect(&format!("{}:{}", addr.ip(), addr.port()));
        LogStorageClient::new(conn)
    }

    pub fn new_connection(&self) -> ClientConnectFuture {
        let head = self.connect(&self.config.head);
        let tail = self.connect(&self.config.tail);

        // force connection open by querying for the latest offset
        let query = LatestOffsetQuery::new();
        let head_latest = LatestOffsetFuture::new(head.latest_offset_async(&query));
        let tail_latest = LatestOffsetFuture::new(tail.latest_offset_async(&query));

        ClientConnectFuture {
            future: head_latest.join(tail_latest),
            conns: Some((head, tail)),
        }
    }
}

pub struct ClientConnectFuture {
    future: Join<LatestOffsetFuture, LatestOffsetFuture>,
    conns: Option<(LogStorageClient, LogStorageClient)>,
}

impl Future for ClientConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.future.poll());
        debug!("Connected");

        let (head_conn, tail_conn) = self.conns.take().unwrap();
        let req_mgr = append::RequestManager::start(&tail_conn)?;
        Ok(Async::Ready(Connection {
            head_conn,
            tail_conn,
            req_mgr,
        }))
    }
}
