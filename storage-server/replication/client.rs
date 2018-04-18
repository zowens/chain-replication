use super::protocol::{ClientProtocol, ReplicationRequest, ReplicationResponse};
use futures::sink::Send;
use futures::stream::StreamFuture;
use futures::{Async, Future, Poll, Sink, Stream};
use std::io;
use std::net::SocketAddr;
use tokio::net::{ConnectFuture, TcpStream};
use tokio_io::codec::Framed;
use tokio_io::AsyncRead;

/// Connection to an upstream node that is idle
pub struct Connection(Framed<TcpStream, ClientProtocol>);

impl Connection {
    fn new(io: TcpStream) -> Connection {
        Connection(io.framed(ClientProtocol))
    }

    /// Sends a replication request
    pub fn send(self, offset: u64) -> ClientRequestFuture {
        trace!("Requesting replication for offset {}", offset);
        let req = ReplicationRequest {
            starting_offset: offset,
        };
        ClientRequestFuture(RequestState::Sending(self.0.send(req)))
    }
}

/// Future that sends a request for replication and receives a batch of messages.
pub struct ClientRequestFuture(RequestState);

enum RequestState {
    Sending(Send<Framed<TcpStream, ClientProtocol>>),
    Receiving(StreamFuture<Framed<TcpStream, ClientProtocol>>),
}

impl Future for ClientRequestFuture {
    type Item = (ReplicationResponse, Connection);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let next_state = match self.0 {
                RequestState::Sending(ref mut f) => {
                    let s = try_ready!(f.poll());
                    RequestState::Receiving(s.into_future())
                }
                RequestState::Receiving(ref mut f) => {
                    return match f.poll() {
                        Ok(Async::Ready((Some(res), s))) => Ok(Async::Ready((res, Connection(s)))),
                        Ok(Async::Ready((None, _))) => Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "Connection closed",
                        )),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Err((err, _)) => Err(err),
                    };
                } // TODO: add reconnection/retry
            };
            self.0 = next_state;
        }
    }
}

/// Future for connecting to the upstream node
pub struct ClientConnectFuture(ConnectFuture);

impl Future for ClientConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Connection, io::Error> {
        let s = try_ready!(self.0.poll());
        trace!("Connected to the upstream node");
        s.set_nodelay(true).unwrap_or_default();
        Ok(Async::Ready(Connection::new(s)))
    }
}

/// Connects to an upstream node
pub fn connect(addr: &SocketAddr) -> ClientConnectFuture {
    ClientConnectFuture(TcpStream::connect(addr))
}
