use commitlog::message::{MessageBuf, MessageSet};
use commitlog::{CommitLog, LogOptions, Offset, OffsetRange, ReadError, ReadLimit};
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
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
    Append(Messages),
    LastOffset(LogSender<Option<Offset>>),
    Read(Offset, ReadLimit, LogSender<MessageBuf>),
    Replicate(Offset, LogSender<ReplicationSource>),
    AppendFromReplication(Messages, LogSender<OffsetRange>),
}

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink<L> {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,

    listener: L,
    parked_replication: Option<(Offset, LogSender<ReplicationSource>)>,
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
    fn try_replicate(&mut self, offset: Offset, res: LogSender<ReplicationSource>) {
        let mut rd = FileSliceMessageReader;
        let read_res = self
            .log
            .reader(&mut rd, offset, ReadLimit::max_bytes(MAX_REPLICATION_SIZE));
        match read_res {
            Ok(Some(fs)) => res.send(ReplicationSource::File(fs)),
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

    fn log_append(&mut self, mut ms: Messages) -> Result<OffsetRange, Error> {
        let num_bytes = ms.bytes().len() as f64;
        let range = self.log.append(&mut ms).map_err(|e| {
            error!("Unable to append to the log {}", e);
            Error::new(ErrorKind::Other, "append error")
        })?;

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
                res.send(ReplicationSource::InMemory(ms));
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

impl<L> Sink for LogSink<L>
where
    L: AppendListener,
{
    type SinkItem = LogRequest;
    type SinkError = ();

    fn start_send(&mut self, item: LogRequest) -> StartSend<LogRequest, ()> {
        trace!("start_send from log");
        match item {
            LogRequest::Append(reqs) => {
                self.log_append(reqs).map(|_| ()).unwrap_or_default();
            }
            LogRequest::AppendFromReplication(ms, res) => {
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
        Ok(Async::Ready(()))
    }
}

type SingleMessage = (u32, u32, Vec<u8>);

/// `AsyncLog` allows asynchronous operations against the `CommitLog`.
#[derive(Clone)]
pub struct AsyncLog {
    req_sink: mpsc::UnboundedSender<LogRequest>,
    append_sink: mpsc::UnboundedSender<SingleMessage>,
}

pub fn open<L>(log_dir: &str, listener: L) -> AsyncLog
where
    L: AppendListener + Send + 'static,
{
    let (req_sink, req_stream) = mpsc::unbounded::<LogRequest>();
    let (append_sink, append_stream) = mpsc::unbounded::<SingleMessage>();
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

    let append_stream = BatchAppend(append_stream, MessageBufPool::new(5, 16_384));

    // TODO: revisit this
    thread::spawn(move || {
        LogSink::new(log, listener)
            .send_all(req_stream.select(append_stream))
            .map(|_| error!("Log sink completed"))
            .wait()
            .unwrap()
    });

    AsyncLog {
        req_sink,
        append_sink,
    }
}

impl AsyncLog {
    pub fn append(&self, client_id: u32, client_req_id: u32, payload: Vec<u8>) {
        self.append_sink
            .unbounded_send((client_id, client_req_id, payload))
            .unwrap();
    }

    pub fn last_offset(&self) -> LogFuture<Option<Offset>> {
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

    pub fn replicate_from(&self, offset: Offset) -> LogFuture<ReplicationSource> {
        let (snd, recv) = oneshot::channel::<Result<ReplicationSource, Error>>();
        self.req_sink
            .unbounded_send(LogRequest::Replicate(offset, LogSender(snd)))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn append_from_replication(&self, buf: Messages) -> LogFuture<OffsetRange> {
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

/// Notifies a listener of log append operations.
pub trait AppendListener {
    /// Notifies the listener that the log has been mutated with the
    /// offset range specified.
    fn notify_append(&mut self, appended: Messages);
}

struct BatchAppend<S>(S, MessageBufPool);

impl<S> Stream for BatchAppend<S>
where
    S: Stream<Item = SingleMessage>,
{
    type Item = LogRequest;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let (client, req, payload) = try_ready!(self.0.poll()).unwrap();
        let mut buf = self.1.take();
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

        Ok(Async::Ready(Some(LogRequest::Append(buf))))
    }
}
