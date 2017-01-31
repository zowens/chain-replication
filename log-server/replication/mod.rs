use std::io;
use std::net::SocketAddr;

use futures::{Future, Stream, Poll, Async};
use futures::stream::Empty;
use tokio_proto::{TcpClient, Connect};
use tokio_proto::streaming::*;
use tokio_proto::streaming::multiplex::ClientProto;
use tokio_proto::streaming::multiplex::StreamingMultiplex;
use tokio_proto::util::client_proxy::*;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_core::io::{Io, Framed};
use tokio_service::Service;
use commitlog::MessageSet;

use proto::*;
use asynclog::Messages;

type RequestBodyStream = Empty<(), io::Error>;

#[derive(Default)]
struct ReplicaProto;
impl ClientProto<TcpStream> for ReplicaProto {
    type Request = ReplicationRequestHeaders;
    type RequestBody = ();
    type Response = ReplicationResponseHeaders;
    type ResponseBody = Messages;
    type Transport = Framed<TcpStream, ReplicationClientProtocol>;
    type Error = io::Error;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(ReplicationClientProtocol))
    }
}

pub struct ReplicationFuture {
    state: ReplicationState,
}

enum ReplicationState {
    Connecting(Connect<StreamingMultiplex<RequestBodyStream>, ReplicaProto>),
    RequestingReplication(Client, ClientResponseFuture),
    Replicating(Client, Body<Messages, io::Error>),
}

impl ReplicationFuture {
    pub fn new(addr: SocketAddr, handle: Handle) -> ReplicationFuture {
        let client: TcpClient<StreamingMultiplex<RequestBodyStream>, ReplicaProto> =
            TcpClient::new(ReplicaProto);
        let con = client.connect(&addr, &handle);

        ReplicationFuture { state: ReplicationState::Connecting(con) }
    }
}

type Client = ClientProxy<ClientRequest, ClientResponse, io::Error>;
type ClientRequest = Message<ReplicationRequestHeaders, RequestBodyStream>;
type ClientResponse = Message<ReplicationResponseHeaders, Body<Messages, io::Error>>;
type ClientResponseFuture = Response<ClientResponse, io::Error>;

impl Future for ReplicationFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            let next = match self.state {
                ReplicationState::Connecting(ref mut conn) => {
                    let client: Client = try_ready!(conn.poll());
                    // TODO: query for last offset from actual log
                    let f: ClientResponseFuture =
                        client.call(Message::WithoutBody(ReplicationRequestHeaders::StartFrom(0)));
                    info!("Connected, requesting replication");
                    ReplicationState::RequestingReplication(client, f)
                }
                ReplicationState::RequestingReplication(ref client, ref mut f) => {
                    let m: ClientResponse = try_ready!(f.poll());
                    match m {
                        Message::WithBody(ReplicationResponseHeaders::Replicate, body) => {
                            info!("Replication started");
                            ReplicationState::Replicating(client.clone(), body)
                        }
                        Message::WithoutBody(ReplicationResponseHeaders::Replicate) => {
                            error!("Log server responded without a replication stream body");
                            return Err(io::Error::new(io::ErrorKind::Other,
                                                      "Unexpected response, no body provided"));
                        }
                    }
                }
                ReplicationState::Replicating(ref _client, ref mut body) => {
                    match try_ready!(body.poll()) {
                        Some(messages) => {
                            // TODO: append to the log
                            info!("Got {} messages", messages.len());
                            return Ok(Async::NotReady);
                        }
                        None => {
                            info!("Replication stoppped");
                            return Ok(Async::Ready(()));
                        }
                    }
                }
            };
            self.state = next;
        }
    }
}
