use super::client::{connect, ClientConnectFuture, ClientRequestFuture, Connection};
use super::protocol::ReplicationResponse;
use asynclog::{AsyncLog, LogFuture};
use commitlog::{Offset, OffsetRange};
use futures::future::{Join, Map};
use futures::{Async, Future, Poll};
use messages::Messages;
use prometheus::{linear_buckets, Histogram};
use std::io;
use std::net::SocketAddr;
use std::time::Instant;

lazy_static! {
    static ref REQUEST_REPLICATION_TIMER: Histogram = register_histogram!(
        "replication_request_timer_ms",
        "Number of milliseconds for replication request",
        linear_buckets(0f64, 10f64, 20usize).unwrap()
    ).unwrap();
    static ref REPLICATION_APPEND_TIMER: Histogram = register_histogram!(
        "replication_append_timer_ms",
        "Number of milliseconds for appending to the log",
        linear_buckets(0f64, 1f64, 10usize).unwrap()
    ).unwrap();
}

/// Replication state machine that reads from an upstream server
pub struct Replication {
    state: ReplicationState,
    log: AsyncLog,
}

impl Replication {
    /// Creates a replication state machine connecting to the upstream node
    pub fn new(addr: &SocketAddr, log: AsyncLog) -> Replication {
        let conn = connect(addr.clone());
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
    let msgs = Messages::from_replication(p.0.messages);
    let next_off = msgs
        .next_offset()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Zero messages"))?;

    debug!("Requesting next batch starting at offset {}", next_off);

    let inst = Instant::now();

    // append the current batch of messages to the log
    let append = Timed(
        log.append_from_replication(msgs),
        inst,
        REPLICATION_APPEND_TIMER.clone(),
    );
    // start the request for the next batch of messages
    let next_batch = Timed(p.1.send(next_off), inst, REQUEST_REPLICATION_TIMER.clone());

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
    Request(Timed<ClientRequestFuture>),
    // Concurrenty:
    // * Requesting next batch of messages
    // * Appending current batch of messages to log
    RequestAndAppend(
        Map<
            Join<Timed<LogFuture<OffsetRange>>, Timed<ClientRequestFuture>>,
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
                    ReplicationState::Request(Timed(
                        conn.send(first_missing_offset),
                        Instant::now(),
                        REQUEST_REPLICATION_TIMER.clone(),
                    ))
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

struct Timed<F: Future>(F, Instant, Histogram);

impl<F: Future> Future for Timed<F> {
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let res = try_ready!(self.0.poll());
        self.2
            .observe(((Instant::now() - self.1).subsec_nanos() / 1_000_000) as f64);
        Ok(Async::Ready(res))
    }
}
