use bytes::BytesMut;
use commitlog::message::*;
use commitlog::*;
use either::Either;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend};
use messages::*;
use prometheus::{exponential_buckets, linear_buckets, Gauge, Histogram};
use std::intrinsics::unlikely;
use std::io::{Error, ErrorKind};
use std::thread;
use std::time::{Duration, Instant};
use std::usize;

macro_rules! rare {
    ($e:expr) => {
        unsafe { unlikely($e) }
    };
}

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
}

pub struct ReplicationMessages(pub BytesMut);

impl ReplicationMessages {
    pub fn next_offset(&self) -> Option<Offset> {
        self.iter().last().map(|m| m.offset() + 1)
    }
}

impl MessageSet for ReplicationMessages {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageSetMut for ReplicationMessages {
    type ByteMut = Self;
    fn bytes_mut(&mut self) -> &mut Self {
        self
    }
}

impl AsMut<[u8]> for ReplicationMessages {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

struct LogSender<T>(oneshot::Sender<Result<T, Error>>);

impl<T> LogSender<T> {
    fn send(self, res: T) {
        self.0.send(Ok(res)).unwrap_or_default();
    }

    fn send_err(self, e: Error) {
        self.0.send(Err(e)).unwrap_or_default();
    }

    fn send_err_with(self, k: ErrorKind, e: &'static str) {
        self.0.send(Err(Error::new(k, e))).unwrap_or_default();
    }
}

/// Request sent through the `Sink` for the log
enum LogRequest {
    Append(PooledMessageBuf),
    LastOffset(LogSender<Option<Offset>>),
    Read(Offset, ReadLimit, LogSender<MessageBuf>),
    Replicate(Offset, LogSender<FileSlice>),
    AppendFromReplication(ReplicationMessages, LogSender<OffsetRange>),
}

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink<L> {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,

    listener: L,
    parked_replication: Option<(Offset, LogSender<FileSlice>)>,
}

impl<L> LogSink<L>
where
    L: AppendListener,
{
    fn new(log: CommitLog, listener: L) -> LogSink<L> {
        LogSink {
            log,
            last_flush: Instant::now(),
            dirty: false,
            listener,
            parked_replication: None,
        }
    }

    /// Trys to replicate via a log read, parking if the offset has not yet been appended.
    fn try_replicate(&mut self, offset: Offset, res: LogSender<FileSlice>) {
        let mut rd = FileSliceMessageReader;
        let read_res = self
            .log
            .reader(&mut rd, offset, ReadLimit::max_bytes(MAX_REPLICATION_SIZE));
        match read_res {
            Ok(Some(fs)) => res.send(fs),
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

    fn log_append<M: MessageSetMut>(&mut self, ms: &mut M) -> Result<OffsetRange, Error> {
        let num_bytes = ms.bytes().len() as f64;
        let range = self.log.append(ms).map_err(|e| {
            error!("Unable to append to the log {}", e);
            Error::new(ErrorKind::Other, "append error")
        })?;

        self.dirty = true;

        let latest_offset = range.iter().next_back().unwrap();

        APPEND_BYTES_HISTOGRAM.observe(num_bytes);
        LOG_LATEST_OFFSET.set(latest_offset as f64);
        APPEND_COUNT_HISTOGRAM.observe(range.len() as f64);

        if let Some((offset, res)) = self.parked_replication.take() {
            debug!("Sending messages to parked replication request");
            self.try_replicate(offset, res);
        }

        Ok(range)
    }
}

impl<L> Sink for LogSink<L>
where
    L: AppendListener,
{
    type SinkItem = LogRequest;
    type SinkError = ();

    fn start_send(&mut self, item: LogRequest) -> StartSend<LogRequest, ()> {
        trace!("start_send from log");
        match item {
            LogRequest::Append(mut reqs) => {
                self.log_append(&mut reqs).map(|_| ()).unwrap_or_default();
                // TODO: only append on OK
                self.listener
                    .notify_append(ClientAppendSet(Either::Left(reqs)))
            }
            LogRequest::AppendFromReplication(mut ms, res) => {
                // assert that the upstream server replicated the correct offset and
                // that the message hash values match the payloads
                {
                    if rare!(ms.verify_hashes().is_err()) {
                        error!("Invalid messages detected from upstream replica");
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Attempt to append message set with invalid hashes",
                        );
                        return Ok(AsyncSink::Ready);
                    }

                    let first_msg = ms.iter().next();
                    if rare!(first_msg.is_none()) {
                        error!("No messages were specified from upstream replica");
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Attempt to append message set with no messages",
                        );
                        return Ok(AsyncSink::Ready);
                    }

                    let expected_offset = self.log.last_offset().map(|v| v + 1).unwrap_or(0);
                    if rare!(expected_offset != first_msg.unwrap().offset()) {
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Expected append from replication to be in sequence",
                        );
                        return Ok(AsyncSink::Ready);
                    }
                }

                let num_msgs = ms.len() as f64;
                match self.log_append(&mut ms) {
                    Ok(appended_range) => {
                        // extra tracking of metrics for appends
                        REPLICATION_APPEND_COUNT_HISTOGRAM.observe(num_msgs);

                        let start_offset = appended_range.first();
                        let next_offset = appended_range.iter().next_back().unwrap() + 1;
                        trace!(
                            "Replicated to log, starting at {}, next offset is {}",
                            start_offset,
                            next_offset
                        );
                        res.send(appended_range);

                        self.listener
                            .notify_append(ClientAppendSet(Either::Right(ms)));
                    }
                    Err(e) => {
                        res.send_err(e);
                    }
                }
            }
            LogRequest::LastOffset(res) => {
                res.send(self.log.last_offset());
            }
            LogRequest::Read(pos, lim, res) => {
                // TODO: allow file slice to be sent (zero copy all the things!)
                match self.log.read(pos, lim) {
                    Ok(v) => res.send(v),
                    Err(_) => res.send_err_with(ErrorKind::Other, "read error"),
                }
            }
            LogRequest::Replicate(offset, res) => {
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
            }
        }
        Ok(Async::NotReady)
    }
}

/// `AsyncLog` allows asynchronous operations against the `CommitLog`.
#[derive(Clone)]
pub struct AsyncLog {
    req_sink: mpsc::UnboundedSender<LogRequest>,
}

pub fn open<L>(log_dir: &str, listener: L) -> AsyncLog
where
    L: AppendListener + Send + 'static,
{
    let (req_sink, req_stream) = mpsc::unbounded::<LogRequest>();
    let log = {
        let mut opts = LogOptions::new(log_dir);
        opts.message_max_bytes(usize::MAX);
        opts.index_max_items(10_000_000);
        opts.segment_max_bytes(1_024_000_000);
        CommitLog::new(opts).expect("Unable to open log")
    };

    // start the metric for latest offset, if not already appended
    if let Some(off) = log.last_offset() {
        LOG_LATEST_OFFSET.set(off as f64);
    }

    trace!("Spawning log sink...");

    // TODO: revisit this
    thread::spawn(move || {
        LogSink::new(log, listener)
            .send_all(req_stream)
            .map(|_| error!("Log sink completed"))
            .wait()
            .unwrap()
    });

    AsyncLog { req_sink }
}

impl AsyncLog {
    pub fn append(&self, payload: PooledMessageBuf) {
        self.req_sink
            .unbounded_send(LogRequest::Append(payload))
            .unwrap();
    }

    pub fn last_offset(&self) -> LogFuture<Option<Offset>> {
        trace!("READ LATEST");
        let (snd, recv) = oneshot::channel::<Result<Option<Offset>, Error>>();
        self.req_sink
            .unbounded_send(LogRequest::LastOffset(LogSender(snd)))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn read(&self, position: Offset, limit: ReadLimit) -> LogFuture<MessageBuf> {
        let (snd, recv) = oneshot::channel::<Result<MessageBuf, Error>>();
        self.req_sink
            .unbounded_send(LogRequest::Read(position, limit, LogSender(snd)))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn replicate_from(&self, offset: Offset) -> LogFuture<FileSlice> {
        let (snd, recv) = oneshot::channel::<Result<FileSlice, Error>>();
        self.req_sink
            .unbounded_send(LogRequest::Replicate(offset, LogSender(snd)))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn append_from_replication(&self, buf: ReplicationMessages) -> LogFuture<OffsetRange> {
        let (snd, recv) = oneshot::channel::<Result<OffsetRange, Error>>();
        self.req_sink
            .unbounded_send(LogRequest::AppendFromReplication(buf, LogSender(snd)))
            .unwrap();
        LogFuture { f: recv }
    }
}

/// `LogFuture` waits for a response from the `CommitLog`.
pub struct LogFuture<R> {
    f: oneshot::Receiver<Result<R, Error>>,
}

impl<R> Future for LogFuture<R> {
    type Item = R;
    type Error = Error;

    fn poll(&mut self) -> Poll<R, Error> {
        match self.f.poll() {
            Ok(Async::Ready(Ok(v))) => Ok(Async::Ready(v)),
            Ok(Async::Ready(Err(e))) => {
                error!("Inner error {}", e);
                Err(e)
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                error!("Encountered cancellation: {}", e);
                Err(Error::new(ErrorKind::Other, "Cancelled"))
            }
        }
    }
}

pub struct ClientAppendSet(Either<PooledMessageBuf, ReplicationMessages>);

impl ClientAppendSet {
    pub fn iter(&self) -> MessageIter {
        match self.0 {
            Either::Left(ref mb) => mb.iter(),
            Either::Right(ref mb) => mb.iter(),
        }
    }
}

/// Notifies a listener of log append operations.
pub trait AppendListener {
    /// Notifies the listener that the log has been mutated with the
    /// offset range specified.
    fn notify_append(&mut self, append: ClientAppendSet);
}
