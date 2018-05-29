use bytes::Bytes;
use commitlog::message::{set_offsets, MessageBuf, MessageSet};
use commitlog::reader::LogSliceReader;
use commitlog::{CommitLog, LogOptions, Offset, OffsetRange, ReadError, ReadLimit};
use config::LogConfig;
use either::Either;
use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use prometheus::{exponential_buckets, linear_buckets, Gauge, Histogram};
use std::cell::RefCell;
use std::intrinsics::unlikely;
use std::io::{Error, ErrorKind};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

mod bufpool;
mod messages;
mod sync;

use self::bufpool::BytesPool;
pub use self::messages::{Messages, MessagesMut};
pub use self::sync::LogFuture;
use self::sync::{channel, LogSender};

macro_rules! rare {
    ($e:expr) => {
        unsafe { unlikely($e) }
    };
}

type ReplicationSource<R> = Either<R, Messages>;

// TODO: allow configuration
static MAX_REPLICATION_SIZE: usize = 50 * 1_024;

lazy_static! {
    static ref LOG_LATEST_OFFSET: Gauge = register_gauge!(opts!(
        "log_last_offset",
        "The log offset of the last entry to the log.",
        labels!{"mod" => "log",}
    )).unwrap();
    static ref APPEND_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "log_append_count",
        "Number of messages appended",
        linear_buckets(0f64, 2f64, 20usize).unwrap()
    ).unwrap();
    static ref APPEND_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "log_append_bytes",
        "Number of bytes appended",
        exponential_buckets(500f64, 2f64, 10usize).unwrap()
    ).unwrap();
    static ref REPLICATION_APPEND_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "replication_log_append_count",
        "Number of messages appended",
        linear_buckets(0f64, 2f64, 20usize).unwrap()
    ).unwrap();
    static ref APPEND_TIME_HISTOGRAM: Histogram = register_histogram!(
        "log_append_ns",
        "Nanos to append to log",
        linear_buckets(20_000f64, 1.5f64, 20usize).unwrap()
    ).unwrap();
    static ref FLUSH_TIME_HISTOGRAM: Histogram = register_histogram!(
        "log_flush_ns",
        "Nanos to flugh to disk",
        linear_buckets(20_000f64, 1.5f64, 20usize).unwrap()
    ).unwrap();
}

enum ClientRequest {
    Append(MessagesMut),
    LastOffset(LogSender<Option<Offset>>),
    Read(Offset, ReadLimit, LogSender<MessageBuf>),
}

// TODO: remove this
enum ReplicaRequest<R> {
    Replicate(Offset, LogSender<ReplicationSource<R>>),
    AppendFromReplication(Messages, LogSender<OffsetRange>),
}

/// Request sent through the `Sink` for the log
enum LogRequest<R> {
    Replica(ReplicaRequest<R>),
    Client(ClientRequest),
}

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink<L, R: LogSliceReader> {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,

    pool: Rc<RefCell<BytesPool>>,

    listener: L,
    log_slice_reader: R,
    parked_replication: Option<(Offset, LogSender<ReplicationSource<R::Result>>)>,
}

impl<L, R> LogSink<L, R>
where
    L: AppendListener,
    R: LogSliceReader,
{
    fn new(log: CommitLog, pool: Rc<RefCell<BytesPool>>, listener: L, reader: R) -> LogSink<L, R> {
        LogSink {
            log,
            last_flush: Instant::now(),
            dirty: false,
            pool,
            listener,
            log_slice_reader: reader,
            parked_replication: None,
        }
    }

    /// Trys to replicate via a log read, parking if the offset has not yet been appended.
    fn try_replicate(&mut self, offset: Offset, res: LogSender<ReplicationSource<R::Result>>) {
        let read_res = self.log.reader(
            &mut self.log_slice_reader,
            offset,
            ReadLimit::max_bytes(MAX_REPLICATION_SIZE),
        );
        match read_res {
            Ok(Some(fs)) => res.send(Either::Left(fs)),
            Ok(None) => {
                debug!("Parking replication, no offset {}", offset);
                self.parked_replication = Some((offset, res));
            }
            Err(ReadError::Io(e)) => res.send_err(e),
            Err(ReadError::CorruptLog) => {
                res.send_err_with(ErrorKind::Other, "Corrupt log detected");
            }
            Err(ReadError::NoSuchSegment) => {
                res.send_err_with(ErrorKind::Other, "read error");
            }
        }
    }

    fn log_append(&mut self, ms: Messages) -> Result<OffsetRange, Error> {
        let num_bytes = ms.bytes().len() as f64;

        let start = Instant::now();
        let range = self.log.append_with_offsets(&ms).map_err(|e| {
            error!("Unable to append to the log {}", e);
            Error::new(ErrorKind::Other, "append error")
        })?;
        let elapsed = start.elapsed().subsec_nanos() as f64;
        APPEND_TIME_HISTOGRAM.observe(elapsed);

        self.dirty = true;

        let latest_offset = range.iter().next_back().unwrap();

        APPEND_BYTES_HISTOGRAM.observe(num_bytes);
        LOG_LATEST_OFFSET.set(latest_offset as f64);
        APPEND_COUNT_HISTOGRAM.observe(range.len() as f64);

        // TODO: figure out whether the listener should be notified via roles/config
        if let Some((offset, res)) = self.parked_replication.take() {
            debug!("Sending messages to parked replication request");
            if offset == range.first() {
                trace!("Sending in memory replication");
                res.send(Either::Right(ms));
            } else {
                warn!("Invalid append, offset {} != {}", offset, range.first());
                self.try_replicate(offset, res);
            }
        } else {
            self.listener.notify_append(ms);
        }

        Ok(range)
    }
}

impl<L, R> Sink for LogSink<L, R>
where
    L: AppendListener,
    R: LogSliceReader,
{
    type SinkItem = LogRequest<R::Result>;
    type SinkError = ();

    fn start_send(&mut self, item: LogRequest<R::Result>) -> StartSend<LogRequest<R::Result>, ()> {
        use self::ClientRequest::*;
        use self::LogRequest::*;
        use self::ReplicaRequest::*;

        trace!("start_send from log");
        match item {
            Client(Append(mut ms)) => {
                set_offsets(&mut ms, self.log.next_offset());
                let ms = ms.freeze();
                self.pool.borrow_mut().push(ms.0.clone());
                self.log_append(ms).map(|_| ()).unwrap_or_default();
            }
            Replica(AppendFromReplication(ms, res)) => {
                // assert that the upstream server replicated the correct offset and
                // that the message hash values match the payloads
                {
                    trace!("Verifying hashes");
                    if rare!(ms.verify_hashes().is_err()) {
                        error!("Invalid messages detected from upstream replica");
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Attempt to append message set with invalid hashes",
                        );
                        return Ok(AsyncSink::Ready);
                    }

                    trace!("Determining if we have messages");
                    let first_msg = ms.iter().next();
                    if rare!(first_msg.is_none()) {
                        error!("No messages were specified from upstream replica");
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Attempt to append message set with no messages",
                        );
                        return Ok(AsyncSink::Ready);
                    }

                    trace!("Determining offset match");
                    let expected_offset = self.log.last_offset().map(|v| v + 1).unwrap_or(0);
                    if rare!(expected_offset != first_msg.unwrap().offset()) {
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Expected append from replication to be in sequence",
                        );
                        return Ok(AsyncSink::Ready);
                    }
                }

                trace!("Initiating log append");
                match self.log_append(ms) {
                    Ok(appended_range) => {
                        trace!("DONE APPENDING");
                        // extra tracking of metrics for appends
                        let num_msgs = appended_range.len();
                        REPLICATION_APPEND_COUNT_HISTOGRAM.observe(num_msgs as f64);

                        let start_offset = appended_range.first();
                        let next_offset = appended_range.iter().next_back().unwrap() + 1;
                        trace!(
                            "Replicated to log, starting at {}, next offset is {}",
                            start_offset,
                            next_offset
                        );
                        res.send(appended_range);
                        trace!("Full append finish");
                    }
                    Err(e) => {
                        res.send_err(e);
                    }
                }
            }
            Client(LastOffset(res)) => {
                res.send(self.log.last_offset());
            }
            Client(Read(pos, lim, res)) => {
                // TODO: allow file slice to be sent (zero copy all the things!)
                match self.log.read(pos, lim) {
                    Ok(v) => res.send(v),
                    Err(_) => res.send_err_with(ErrorKind::Other, "read error"),
                }
            }
            Replica(Replicate(offset, res)) => {
                self.try_replicate(offset, res);
            }
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.dirty {
            trace!("Log poll_complete, flushing");
            let now = Instant::now();
            if (now - self.last_flush) > Duration::from_secs(1) {
                trace!("Attempting flush");

                match self.log.flush() {
                    Err(e) => {
                        error!("Log flush error: {}", e);
                    }
                    _ => {
                        self.last_flush = now;
                        self.dirty = false;
                        trace!("Flushed");
                    }
                };

                let elapsed = now.elapsed().subsec_nanos() as f64;
                FLUSH_TIME_HISTOGRAM.observe(elapsed);
            }
        }
        Ok(Async::Ready(()))
    }
}

type SingleMessage = (u64, u64, Bytes);

/// `AsyncLog` allows asynchronous operations against the `CommitLog`.
#[derive(Clone)]
pub struct AsyncLog {
    req_sink: mpsc::UnboundedSender<ClientRequest>,
    append_sink: mpsc::UnboundedSender<SingleMessage>,
}

pub fn open<L, R>(
    cfg: LogConfig,
    listener: L,
    reader: R,
) -> (AsyncLog, ReplicatorAsyncLog<R::Result>)
where
    L: AppendListener + Send + 'static,
    R: LogSliceReader + Send + 'static,
    R::Result: Send + 'static,
{
    let (client_req_sink, client_req_stream) = mpsc::unbounded::<ClientRequest>();
    let (repl_req_sink, repl_req_stream) = mpsc::unbounded::<LogRequest<R::Result>>();
    let (append_sink, append_stream) = mpsc::unbounded::<SingleMessage>();

    let log = {
        let mut opts = LogOptions::new(&cfg.dir);
        opts.message_max_bytes(cfg.message_max_bytes);
        opts.index_max_items(cfg.index_max_items);
        opts.segment_max_bytes(cfg.segment_max_bytes);
        CommitLog::new(opts).expect("Unable to open log")
    };

    // start the metric for latest offset, if not already appended
    if let Some(off) = log.last_offset() {
        LOG_LATEST_OFFSET.set(off as f64);
    }

    trace!("Spawning log sink...");

    // TODO: revisit this
    thread::spawn(move || {
        let pool = Rc::new(RefCell::new(BytesPool::new(16_384)));
        let append_stream = BatchAppend(append_stream, pool.clone());
        LogSink::new(log, pool, listener, reader)
            .send_all(
                client_req_stream
                    .select(append_stream)
                    .map(LogRequest::Client)
                    .select(repl_req_stream),
            )
            .map(|_| error!("Log sink completed"))
            .wait()
            .unwrap()
    });

    (
        AsyncLog {
            req_sink: client_req_sink,
            append_sink,
        },
        ReplicatorAsyncLog {
            req_sink: repl_req_sink,
        },
    )
}

impl AsyncLog {
    pub fn append(&self, client_id: u64, client_req_id: u64, payload: Bytes) {
        self.append_sink
            .unbounded_send((client_id, client_req_id, payload))
            .unwrap();
    }

    pub fn last_offset(&self) -> LogFuture<Option<Offset>> {
        let (snd, f) = channel::<Option<Offset>>();
        self.req_sink
            .unbounded_send(ClientRequest::LastOffset(snd))
            .unwrap();
        f
    }

    pub fn read(&self, position: Offset, limit: ReadLimit) -> LogFuture<MessageBuf> {
        let (snd, f) = channel::<MessageBuf>();
        self.req_sink
            .unbounded_send(ClientRequest::Read(position, limit, snd))
            .unwrap();
        f
    }
}

// TODO: remove replication-specific logic
pub struct ReplicatorAsyncLog<R> {
    req_sink: mpsc::UnboundedSender<LogRequest<R>>,
}

impl<R> Clone for ReplicatorAsyncLog<R> {
    fn clone(&self) -> ReplicatorAsyncLog<R> {
        ReplicatorAsyncLog {
            req_sink: self.req_sink.clone(),
        }
    }
}

impl<R> ReplicatorAsyncLog<R> {
    pub fn replicate_from(&self, offset: Offset) -> LogFuture<ReplicationSource<R>> {
        let (snd, f) = channel::<ReplicationSource<R>>();
        self.req_sink
            .unbounded_send(LogRequest::Replica(ReplicaRequest::Replicate(offset, snd)))
            .unwrap();
        f
    }

    pub fn append_from_replication(&self, buf: Messages) -> LogFuture<OffsetRange> {
        let (snd, f) = channel::<OffsetRange>();
        self.req_sink
            .unbounded_send(LogRequest::Replica(ReplicaRequest::AppendFromReplication(
                buf, snd,
            )))
            .unwrap();
        f
    }

    pub fn last_offset(&self) -> LogFuture<Option<Offset>> {
        let (snd, f) = channel::<Option<Offset>>();
        self.req_sink
            .unbounded_send(LogRequest::Client(ClientRequest::LastOffset(snd)))
            .unwrap();
        f
    }
}

/// Notifies a listener of log append operations.
pub trait AppendListener {
    /// Notifies the listener that the log has been mutated with the
    /// offset range specified.
    fn notify_append(&mut self, appended: Messages);
}

struct BatchAppend<S>(S, Rc<RefCell<BytesPool>>);

impl<S> Stream for BatchAppend<S>
where
    S: Stream<Item = SingleMessage>,
{
    type Item = ClientRequest;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let (client, req, payload) = try_ready!(self.0.poll()).unwrap();
        let mut buf = MessagesMut(self.1.borrow_mut().take());
        buf.push(client, req, payload);

        loop {
            match self.0.poll()? {
                Async::Ready(Some((client, req, payload))) => {
                    buf.push(client, req, payload);
                }
                Async::Ready(None) => panic!("BatchAppend must be used with unbounded stream"),
                Async::NotReady => {
                    break;
                }
            }
        }

        Ok(Async::Ready(Some(ClientRequest::Append(buf))))
    }
}
