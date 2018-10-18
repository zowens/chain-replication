use super::client::{connect, ClientConnectFuture, ClientRequestFuture, Connection};
use super::log_reader::FileSlice;
use super::protocol::ReplicationResponse;
use asynclog::Messages;
use asynclog::{LogFuture, ReplicatorAsyncLog};
use commitlog::{Offset, OffsetRange};
use futures::future::{Join, Map};
use futures::{Async, Future, Poll};
use std::io;
use std::net::SocketAddr;

/// Replication state machine that reads from an upstream server
pub struct UpstreamReplication {
    addr: SocketAddr,
    state: ReplicationState,
    log: ReplicatorAsyncLog<FileSlice>,
}

impl UpstreamReplication {
    /// Creates a replication state machine connecting to the upstream node
    pub fn new(addr: &SocketAddr, log: ReplicatorAsyncLog<FileSlice>) -> UpstreamReplication {
        let state = ReplicationState::connect(addr, &log);
        UpstreamReplication {
            addr: *addr,
            state,
            log,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.addr
    }

    pub fn into_async_log(self) -> ReplicatorAsyncLog<FileSlice> {
        self.log
    }
}

type ResponseConnectionPair = (ReplicationResponse, Connection);

enum ReplicationState {
    // Initial State, Concurrently:
    // * Connect to the upstream node
    // * Determine the latest log offset
    ConnectingAndOffset(Join<ClientConnectFuture, LogFuture<Option<Offset>>>),
    // * Requesting replication from upstream node
    Request(ClientRequestFuture),
    // Concurrenty:
    // * Requesting next batch of messages
    // * Appending current batch of messages to log
    RequestAndAppend(
        Map<
            Join<LogFuture<OffsetRange>, ClientRequestFuture>,
            fn((OffsetRange, ResponseConnectionPair)) -> ResponseConnectionPair,
        >,
    ),
}

impl ReplicationState {
    fn connect(addr: &SocketAddr, log: &ReplicatorAsyncLog<FileSlice>) -> ReplicationState {
        let conn = connect(*addr);
        let latest_offset = log.last_offset();
        ReplicationState::ConnectingAndOffset(conn.join(latest_offset))
    }

    #[inline]
    fn poll_step(&mut self, log: &ReplicatorAsyncLog<FileSlice>) -> Poll<(), io::Error> {
        let next_state = match *self {
            ReplicationState::ConnectingAndOffset(ref mut f) => {
                let (conn, latest) = try_ready!(f.poll());
                let first_missing_offset = latest.map(|off| off + 1).unwrap_or(0);
                ReplicationState::Request(conn.send(first_missing_offset))
            }
            ReplicationState::Request(ref mut f) => next_batch(try_ready!(f.poll()), log)?,
            ReplicationState::RequestAndAppend(ref mut f) => next_batch(try_ready!(f.poll()), log)?,
        };

        *self = next_state;
        Ok(Async::Ready(()))
    }
}

// Coordinates the append of a single batch and starts the request for the next batch
#[inline]
fn next_batch(
    p: ResponseConnectionPair,
    log: &ReplicatorAsyncLog<FileSlice>,
) -> Result<ReplicationState, io::Error> {
    fn map_second_elem(res: (OffsetRange, ResponseConnectionPair)) -> ResponseConnectionPair {
        res.1
    }

    // find the next offset to request by adding 1 to the last offset
    let msgs = Messages::parse(p.0.messages.freeze()).map_err(|e| {
        error!("Error with upstream: {:?}", e);
        io::Error::new(io::ErrorKind::Other, "Invalid messages")
    })?;

    let next_off = msgs
        .next_offset()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Zero messages"))?;

    debug!("Requesting next batch starting at offset {}", next_off);

    // append the current batch of messages to the log
    let append = log.append_from_replication(msgs);
    // start the request for the next batch of messages
    let next_batch = p.1.send(next_off);

    Ok(ReplicationState::RequestAndAppend(
        append.join(next_batch).map(map_second_elem),
    ))
}

impl Future for UpstreamReplication {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            match self.state.poll_step(&self.log) {
                Ok(Async::Ready(())) => {}
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    error!("Error with replication, attempting reconnect: {}", e);
                    // TODO: send this to metadata server?
                    self.state = ReplicationState::connect(&self.addr, &self.log);
                }
            }
        }
    }
}
