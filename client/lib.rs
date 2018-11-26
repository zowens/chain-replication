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
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::{io, mem, time};
use tokio::timer::Delay;

pub use protocol::{AppendSentFuture, LatestOffsetFuture, QueryFuture, Reply, ReplyStream};

// TODO: use exponential backoff
const SNAPSHOT_BACKOFF_DELAY: time::Duration = time::Duration::from_secs(1);

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

// TODO: repoll configuration from the management server
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
    management_server: SocketAddr,
}

impl Default for Configuration {
    fn default() -> Configuration {
        Configuration {
            management_server: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000),
        }
    }
}

impl Configuration {
    pub fn management_server<A: ToSocketAddrs>(
        &mut self,
        addrs: A,
    ) -> Result<&mut Configuration, io::Error> {
        self.management_server = addrs
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found"))?;
        Ok(self)
    }
}

fn connect(env: Arc<Environment>, addr: &str) -> LogStorageClient {
    let cb = ChannelBuilder::new(env)
        .default_compression_algorithm(grpcio::CompressionAlgorithms::None)
        .max_concurrent_stream(1000)
        .http2_bdp_probe(true);
    let conn = cb.connect(addr);
    LogStorageClient::new(conn)
}

fn connect_management_server(env: Arc<Environment>, addr: &SocketAddr) -> ConfigurationClient {
    let cb = ChannelBuilder::new(env)
        .default_compression_algorithm(grpcio::CompressionAlgorithms::Gzip)
        .max_concurrent_stream(10)
        .http2_bdp_probe(true);
    let conn = cb.connect(&format!("{}:{}", addr.ip(), addr.port()));
    ConfigurationClient::new(conn)
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

    pub fn new_connection(&self) -> ClientConnectFuture {
        let client = connect_management_server(self.env.clone(), &self.config.management_server);
        let snapshot_future = client.snapshot_async(&protocol::ClientNodeRequest::new());
        debug!("Requesting configuration from management server");
        ClientConnectFuture {
            state: ClientConnectState::RequestingConfiguration(ClientConfigurationFuture::new(
                snapshot_future,
            )),
            management_client: client,
            env: self.env.clone(),
        }
    }
}

enum ClientConnectState {
    RequestingConfiguration(protocol::ClientConfigurationFuture),
    Backoff(Delay),
    OpeningConnections {
        requests: Join<LatestOffsetFuture, LatestOffsetFuture>,
        connections: Option<(LogStorageClient, LogStorageClient)>,
    },
}

pub struct ClientConnectFuture {
    state: ClientConnectState,
    management_client: ConfigurationClient,
    env: Arc<Environment>,
}

impl Future for ClientConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let next_state = match self.state {
                ClientConnectState::RequestingConfiguration(ref mut cfg_future) => {
                    let nodes = try_ready!(cfg_future.poll()).take_nodes().into_vec();

                    if nodes.len() > 2 {
                        let head_addr = nodes.first().unwrap().get_client_address();
                        let tail_addr = nodes.last().unwrap().get_client_address();
                        debug!(
                            "Configuration found from management server, head={}, tail={}",
                            head_addr, tail_addr
                        );

                        let head = connect(self.env.clone(), head_addr);
                        let tail = connect(self.env.clone(), tail_addr);

                        // force connection open by querying for the latest offset
                        let query = LatestOffsetQuery::new();
                        let head_latest = LatestOffsetFuture::new(head.latest_offset_async(&query));
                        let query = LatestOffsetQuery::new();
                        let tail_latest = LatestOffsetFuture::new(tail.latest_offset_async(&query));
                        ClientConnectState::OpeningConnections {
                            requests: head_latest.join(tail_latest),
                            connections: Some((head, tail)),
                        }
                    } else {
                        debug!("No head or tail found in configuration, adding delay");
                        ClientConnectState::Backoff(Delay::new(
                            time::Instant::now() + SNAPSHOT_BACKOFF_DELAY,
                        ))
                    }
                }
                ClientConnectState::Backoff(ref mut delay) => {
                    try_ready!(delay
                        .poll()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "timer error")));
                    debug!("Requesting configuration from management server");
                    let snapshot_future = self
                        .management_client
                        .snapshot_async(&protocol::ClientNodeRequest::new());
                    ClientConnectState::RequestingConfiguration(ClientConfigurationFuture::new(
                        snapshot_future,
                    ))
                }
                ClientConnectState::OpeningConnections {
                    ref mut connections,
                    ref mut requests,
                } => {
                    try_ready!(requests.poll());
                    let (head_conn, tail_conn) = connections
                        .take()
                        .expect("Multiple calls to poll after ready");
                    debug!("Connections to head and tail succeeded");
                    let req_mgr = append::RequestManager::start(&tail_conn)?;
                    return Ok(Async::Ready(Connection {
                        head_conn,
                        tail_conn,
                        req_mgr,
                    }));
                }
            };
            self.state = next_state;
        }
    }
}
