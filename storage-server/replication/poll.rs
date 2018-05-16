use super::client::{connect, ClientConnectFuture, ClientRequestFuture, Connection};
use super::protocol::ReplicationResponse;
use asynclog::{AsyncLog, LogFuture, ReplicationMessages};
use commitlog::{Offset, OffsetRange};
use futures::future::{Join, Map};
use futures::{Future, Poll};
use std::io;
use std::net::SocketAddr;

/// Replication state machine that reads from an upstream server
pub struct Replication {
    state: ReplicationState,
    log: AsyncLog,
}

impl Replication {
    /// Creates a replication state machine connecting to the upstream node
    pub fn new(addr: &SocketAddr, log: AsyncLog) -> Replication {
        let conn = connect(addr);
        let latest_offset = log.last_offset();
        Replication {
            state: ReplicationState::ConnectingAndOffset(conn.join(latest_offset)),
            log,
        }
    }
}

type ResponseConnectionPair = (ReplicationResponse, Connection);

// Coordinates the append of a single batch and starts the request for the next batch
fn next_batch(p: ResponseConnectionPair, log: &AsyncLog) -> Result<ReplicationState, io::Error> {
    fn map_second_elem(res: (OffsetRange, ResponseConnectionPair)) -> ResponseConnectionPair {
        res.1
    }

    // find the next offset to request by adding 1 to the last offset
    let msgs = ReplicationMessages(p.0.messages);
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

impl Future for Replication {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            let next_state = match self.state {
                ReplicationState::ConnectingAndOffset(ref mut f) => {
                    let (conn, latest) = try_ready!(f.poll());
                    let first_missing_offset = latest.map(|off| off + 1).unwrap_or(0);
                    ReplicationState::Request(conn.send(first_missing_offset))
                }
                ReplicationState::Request(ref mut f) => {
                    next_batch(try_ready!(f.poll()), &self.log)?
                }
                ReplicationState::RequestAndAppend(ref mut f) => {
                    next_batch(try_ready!(f.poll()), &self.log)?
                }
            };

            // TODO: reconnect logic

            self.state = next_state;
        }
    }
}
