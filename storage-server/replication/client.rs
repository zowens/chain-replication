use super::protocol::{ClientProtocol, ReplicationRequest, ReplicationResponse};
use futures::sink::Send;
use futures::stream::StreamFuture;
use futures::{Async, Future, Poll, Sink, Stream};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{ConnectFuture, TcpStream};
use tokio::timer::{Deadline, Delay};
use tokio_io::codec::Framed;
use tokio_io::AsyncRead;

const CONNECT_TIMEOUT_MS: u64 = 1000;
const CONNECT_BACKOFF_MS: u64 = 1000;

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

enum ClientConnectState {
    Backoff(Delay),
    Connecting(Deadline<ConnectFuture>),
}

impl ClientConnectState {
    fn connect(addr: &SocketAddr) -> ClientConnectState {
        let deadline = Instant::now() + Duration::from_millis(CONNECT_TIMEOUT_MS);
        ClientConnectState::Connecting(Deadline::new(TcpStream::connect(addr), deadline))
    }

    fn backoff() -> ClientConnectState {
        let time = Instant::now() + Duration::from_millis(CONNECT_BACKOFF_MS);
        ClientConnectState::Backoff(Delay::new(time))
    }
}

/// Future for connecting to the upstream node
pub struct ClientConnectFuture {
    addr: SocketAddr,
    state: ClientConnectState,
}

impl Future for ClientConnectFuture {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Connection, io::Error> {
        enum NextAction {
            Backoff,
            Reconnect,
        }

        loop {
            debug!("Poll replication connect future");

            let next = match self.state {
                ClientConnectState::Backoff(ref mut backoff) => {
                    try_ready!(backoff.poll().map_err(|e| {
                        error!("Timer error: {}", e);
                        io::Error::new(io::ErrorKind::Other, "Unknown timer error")
                    }));
                    debug!("Done backing off, reconnecting");
                    NextAction::Reconnect
                }
                ClientConnectState::Connecting(ref mut connecting) => match connecting.poll() {
                    Ok(Async::Ready(s)) => {
                        debug!("Connected to the upstream node");
                        s.set_nodelay(true).unwrap_or_default();
                        return Ok(Async::Ready(Connection::new(s)));
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => if e.is_inner() {
                        let e = e.into_inner().unwrap();
                        debug!("Replication inner error, backoff then reconnect: {}", e);
                        NextAction::Backoff
                    } else if e.is_timer() {
                        error!("Timer error");
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Unexpected timer issue",
                        ));
                    } else {
                        // try to connect again
                        debug!("Timeout connecting to upstream node, retrying");
                        NextAction::Reconnect
                    },
                },
            };

            self.state = match next {
                NextAction::Backoff => ClientConnectState::backoff(),
                NextAction::Reconnect => ClientConnectState::connect(&self.addr),
            };
        }
    }
}

/// Connects to an upstream node
pub fn connect(addr: SocketAddr) -> ClientConnectFuture {
    let state = ClientConnectState::connect(&addr);
    ClientConnectFuture { addr, state }
}
