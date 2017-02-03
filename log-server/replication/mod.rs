use std::mem;
use std::io::{self, Error};
use std::net::SocketAddr;

use commitlog::Offset;
use futures::{Future, Stream, Poll, Async};
use futures::stream::Empty;
use tokio_proto::{TcpClient, Connect};
use tokio_proto::streaming::*;
use tokio_proto::streaming::multiplex::ClientProto;
use tokio_proto::streaming::multiplex::StreamingMultiplex;
use tokio_proto::util::client_proxy::*;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_core::io::{Io, Framed, EasyBuf};
use tokio_service::Service;

use proto::*;
use asynclog::*;

type RequestBodyStream = Empty<(), io::Error>;

#[derive(Default)]
struct ReplicaProto;
impl ClientProto<TcpStream> for ReplicaProto {
    type Request = ReplicationRequestHeaders;
    type RequestBody = ();
    type Response = ReplicationResponseHeaders;
    type ResponseBody = EasyBuf;
    type Transport = Framed<TcpStream, ReplicationClientProtocol>;
    type Error = io::Error;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(ReplicationClientProtocol))
    }
}

pub struct ReplicationFuture {
    log: AsyncLog,
    state: ConnectionState,
}

enum ConnectionState {
    Connecting(Connect<StreamingMultiplex<RequestBodyStream>, ReplicaProto>),
    QueryLatestOffset(Client, LogFuture<Offset>),
    RequestingReplication(Client, ClientResponseFuture),
    Replicating(ReplicationState),
}

impl ReplicationFuture {
    pub fn new(log: &AsyncLog, addr: SocketAddr, handle: Handle) -> ReplicationFuture {
        let client: TcpClient<StreamingMultiplex<RequestBodyStream>, ReplicaProto> =
            TcpClient::new(ReplicaProto);
        let con = client.connect(&addr, &handle);

        ReplicationFuture {
            log: log.clone(),
            state: ConnectionState::Connecting(con),
        }
    }
}

type Client = ClientProxy<ClientRequest, ClientResponse, io::Error>;
type ClientRequest = Message<ReplicationRequestHeaders, RequestBodyStream>;
type ClientResponse = Message<ReplicationResponseHeaders, Body<EasyBuf, io::Error>>;
type ClientResponseFuture = Response<ClientResponse, io::Error>;

impl Future for ReplicationFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            let next = match self.state {
                ConnectionState::Connecting(ref mut conn) => {
                    let client: Client = try_ready!(conn.poll());
                    info!("Connected");
                    ConnectionState::QueryLatestOffset(client, self.log.last_offset())
                }
                ConnectionState::QueryLatestOffset(ref client, ref mut last_off_fut) => {
                    let last_offset = try_ready!(last_off_fut.poll());
                    let f: ClientResponseFuture =
                        client.call(Message::WithoutBody(ReplicationRequestHeaders::StartFrom(last_offset.0)));
                    info!("requesting replication, starting at offset {}", last_offset.0);
                    ConnectionState::RequestingReplication(client.clone(), f)
                }
                ConnectionState::RequestingReplication(ref client, ref mut f) => {
                    let m: ClientResponse = try_ready!(f.poll());
                    match m {
                        Message::WithBody(ReplicationResponseHeaders::Replicate, body) => {
                            info!("Replication started");
                            ConnectionState::Replicating(ReplicationState {
                                client: client.clone(),
                                log: self.log.clone(),
                                body_stream: body,
                                append_futures: Vec::new(),
                            })
                        }
                        Message::WithoutBody(ReplicationResponseHeaders::Replicate) => {
                            error!("Log server responded without a replication stream body");
                            return Err(io::Error::new(io::ErrorKind::Other,
                                                      "Unexpected response, no body provided"));
                        }
                    }
                }
                ConnectionState::Replicating(ref mut replication) => {
                    return replication.poll();
                }
            };
            self.state = next;
        }
    }
}

struct ReplicationState {
    #[allow(dead_code)]
    client: Client,
    log: AsyncLog,
    body_stream: Body<EasyBuf, io::Error>,
    append_futures: Vec<LogFuture<()>>,
}

impl ReplicationState {
    fn poll(&mut self) -> Poll<(), Error> {
        loop {
            // check log futures
            let mut futures = Vec::with_capacity(1 + self.append_futures.len());
            mem::swap(&mut futures, &mut self.append_futures);
            for mut f in futures.drain(0..) {
                match f.poll() {
                    Ok(Async::Ready(())) => {
                        trace!("Dropping append, which is finished");
                    },
                    Ok(Async::NotReady) => {
                        trace!("Found non-ready append");
                        self.append_futures.push(f);
                    },
                    Err(e) => return Err(e),
                }
            }

            // read from the body
            match try_ready!(self.body_stream.poll()) {
                Some(messages) => {
                    trace!("Got messages");
                    let f = self.log.append_from_replication(messages);
                    self.append_futures.push(f);
                }
                None => {
                    warn!("Replication stoppped");
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
