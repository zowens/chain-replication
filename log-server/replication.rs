use std::io::{self, Error};
use std::net::SocketAddr;

use commitlog::{Offset, OffsetRange};
use futures::{Future, Poll};
use tokio_proto::{TcpClient, Connect};
use tokio_proto::pipeline::{ClientProto, ClientService, Pipeline};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_service::Service;
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;

use proto::*;
use asynclog::*;

#[derive(Default)]
struct ReplicaProto;
impl ClientProto<TcpStream> for ReplicaProto {
    type Request = ReplicationRequest;
    type Response = ReplicationResponse;
    type Transport = Framed<TcpStream, ReplicationClientProtocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        try!(io.set_keepalive_ms(Some(1000u32)));
        Ok(io.framed(ReplicationClientProtocol))
    }
}

pub struct ReplicationClient {
    log: AsyncLog,
    state: ConnectionState,
}

enum ConnectionState {
    Connecting(Connect<Pipeline, ReplicaProto>),
    QueryLatestOffset(Client, LogFuture<Option<Offset>>),

    Replicating(ReplicationFuture<Client>),
}

impl ReplicationClient {
    pub fn new(log: &AsyncLog, addr: SocketAddr, handle: &Handle) -> ReplicationClient {
        let client = TcpClient::new(ReplicaProto);
        let con = client.connect(&addr, handle);

        ReplicationClient {
            log: log.clone(),
            state: ConnectionState::Connecting(con),
        }
    }
}

type Client = ClientService<TcpStream, ReplicaProto>;

// TODO: remove in favor of `impl Trait`
impl Future for ReplicationClient {
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
                    let next_offset = try_ready!(last_off_fut.poll())
                        .map(|off| off + 1)
                        .unwrap_or(0);

                    info!("requesting replication, starting at offset {}", next_offset);
                    let f = client.call(ReplicationRequest::StartFrom(next_offset));

                    ConnectionState::Replicating(ReplicationFuture {
                                                     client: client.clone(),
                                                     log: self.log.clone(),
                                                     state: ReplicationState::Replicating(f),
                                                 })
                }

                ConnectionState::Replicating(ref mut replication) => {
                    trace!("POLL REPLICATION");
                    return match replication.poll() {
                               Err(e) => {
                                   error!("XXXXXX {}", e);
                                   Err(e)
                               }
                               e => e,
                           };
                }
            };
            self.state = next;
        }
    }
}

struct ReplicationFuture<S: Service> {
    client: S,
    log: AsyncLog,
    state: ReplicationState<S::Future>,
}

enum ReplicationState<F> {
    Replicating(F),
    Appending(LogFuture<OffsetRange>),
}

impl<S> Future for ReplicationFuture<S>
    where S: Service<Request = ReplicationRequest,
                     Response = ReplicationResponse,
                     Error = io::Error>
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), Error> {
        loop {
            let next_state = match self.state {
                ReplicationState::Appending(ref mut f) => {
                    let range = try_ready!(f.poll());
                    let next_off = 1 +
                                   range
                                       .iter()
                                       .next_back()
                                       .expect("Expected append range to be non-empty");
                    debug!("Logs appended, requesting replication starting at offset {}",
                           next_off);
                    ReplicationState::Replicating(self.client
                        .call(ReplicationRequest::StartFrom(next_off)))
                }
                ReplicationState::Replicating(ref mut f) => {
                    match try_ready!(f.poll()) {
                        ReplicationResponse::Messages(msgs) => {
                            debug!("Got messages from upstream, appending to the log");
                            ReplicationState::Appending(self.log.append_from_replication(msgs))
                        }
                    }
                }
            };
            self.state = next_state;
        }
    }
}
