use commitlog::*;
use commitlog::message::*;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use futures_cpupool::CpuPool;
use futures::sync::oneshot;
use futures::sync::mpsc;
use futures::future::Executor;
use std::usize;
use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant};
use std::intrinsics::unlikely;
use std::collections::HashMap;
use messages::*;
use bytes::BytesMut;
use prometheus::{exponential_buckets, linear_buckets, Gauge, Histogram};
use byteorder::{ByteOrder, LittleEndian};

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}

macro_rules! ignore {
    ($res:expr) => (
        $res.unwrap_or(())
    )
}

// TODO: allow configuration
static MAX_REPLICATION_SIZE: usize = 8 * 1_024;

lazy_static! {
    static ref LOG_LATEST_OFFSET: Gauge = register_gauge!(
        opts!(
            "log_last_offset",
            "The log offset of the last entry to the log.",
            labels!{"mod" => "log",}
        )
    ).unwrap();

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

pub struct ClientAppendSet {
    pub latest_offset: Offset,
    pub client_req_ids: Vec<u32>,
    pub client_id: u32,
}

/// Notifies a listener of log append operations.
pub trait AppendListener {
    /// Notifies the listener that the log has been mutated with the
    /// offset range specified.
    fn notify_append(&mut self, append: ClientAppendSet);
}

struct ReplicationMessages(BytesMut);

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

type LogSender<T> = oneshot::Sender<Result<T, Error>>;

/// Request sent through the `Sink` for the log
enum LogRequest {
    Append(AppendReq),
    LastOffset(LogSender<Option<Offset>>),
    Read(Offset, ReadLimit, LogSender<MessageBuf>),
    Replicate(Offset, LogSender<FileSlice>),
    AppendFromReplication(BytesMut, LogSender<OffsetRange>),
}

type AppendReq = PooledMessageBuf;

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
            log: log,
            last_flush: Instant::now(),
            dirty: false,
            listener: listener,
            parked_replication: None,
        }
    }

    /// Trys to replicate via a log read, parking if the offset has not yet been appended.
    fn try_replicate(&mut self, offset: Offset, res: LogSender<FileSlice>) {
        let mut rd = FileSliceMessageReader;
        let read_res = self.log
            .reader(&mut rd, offset, ReadLimit::max_bytes(MAX_REPLICATION_SIZE));
        match read_res {
            Ok(Some(fs)) => ignore!(res.send(Ok(fs))),
            Ok(None) => {
                debug!("Parking replication, no offset {}", offset);
                self.parked_replication = Some((offset, res));
            }
            Err(ReadError::Io(e)) => ignore!(res.send(Err(e))),
            Err(ReadError::CorruptLog) => {
                ignore!(res.send(Err(Error::new(ErrorKind::Other, "Corrupt log detected")),));
            }
            Err(ReadError::NoSuchSegment) => {
                ignore!(res.send(Err(Error::new(ErrorKind::Other, "read error"))));
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

        // collate by client_id
        let mut req_batches: HashMap<u32, Vec<u32>> = HashMap::new();
        for msg in ms.iter() {
            let bytes = msg.payload();
            if bytes.len() < 8 {
                warn!("Invalid log entry appended");
                continue;
            }

            let client_id = LittleEndian::read_u32(&bytes[0..4]);
            let client_req_id = LittleEndian::read_u32(&bytes[4..8]);

            let reqs = req_batches.entry(client_id).or_insert_with(Vec::new);
            reqs.push(client_req_id);
        }

        // notify the clients
        for (client_id, client_req_ids) in req_batches {
            self.listener.notify_append(ClientAppendSet {
                client_id,
                client_req_ids,
                latest_offset,
            });
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
                ignore!(self.log_append(&mut reqs).map(|_| ()));
            }
            LogRequest::AppendFromReplication(buf, res) => {
                let mut ms = ReplicationMessages(buf);

                // assert that the upstream server replicated the correct offset and
                // that the message hash values match the payloads
                {
                    if probably_not!(ms.verify_hashes().is_err()) {
                        error!("Invalid messages detected from upstream replica");
                        ignore!(res.send(Err(Error::new(
                            ErrorKind::InvalidInput,
                            "Attempt to append message set with invalid hashes",
                        ),),));
                        return Ok(AsyncSink::Ready);
                    }

                    let first_msg = ms.iter().next();
                    if probably_not!(first_msg.is_none()) {
                        error!("No messages were specified from upstream replica");

                        ignore!(res.send(Err(Error::new(
                            ErrorKind::InvalidInput,
                            "Attempt to append message set with no messages",
                        ),),));
                        return Ok(AsyncSink::Ready);
                    }

                    let expected_offset = self.log.last_offset().map(|v| v + 1).unwrap_or(0);
                    if probably_not!(expected_offset != first_msg.unwrap().offset()) {
                        ignore!(res.send(Err(Error::new(
                            ErrorKind::InvalidInput,
                            "Expected append from replication to be in sequence",
                        ),),));
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
                        ignore!(res.send(Ok(appended_range)));
                    }
                    Err(e) => {
                        ignore!(res.send(Err(e)));
                    }
                }
            }
            LogRequest::LastOffset(res) => {
                ignore!(res.send(Ok(self.log.last_offset())));
            }
            LogRequest::Read(pos, lim, res) => {
                // TODO: allow file slice to be sent (zero copy all the things!)
                ignore!(
                    res.send(
                        self.log
                            .read(pos, lim)
                            .map_err(|_| Error::new(ErrorKind::Other, "read error")),
                    )
                );
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
    append_sink: mpsc::UnboundedSender<AppendReq>,
    req_sink: mpsc::UnboundedSender<LogRequest>,
}

/// Handle that prevents the dropping of the thread for the `CommitLog` operations.
pub struct Handle {
    #[allow(dead_code)] pool: CpuPool,

    log: AsyncLog,
}

impl Handle {
    fn spawn<S, L>(log_dir: &str, stream: S, async_log: AsyncLog, listener: L) -> Handle
    where
        S: Stream<Item = LogRequest, Error = ()> + Send + 'static,
        L: AppendListener + Send + 'static,
    {
        let pool = CpuPool::new(1);
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

        pool.execute(LogSink::new(log, listener).send_all(stream).map(|_| ())).unwrap();
        Handle {
            pool: pool,
            log: async_log,
        }
    }

    pub fn log(&self) -> &AsyncLog {
        &self.log
    }
}

pub fn open<L>(log_dir: &str, listener: L) -> Handle
where
    L: AppendListener + Send + 'static,
{
    let (append_sink, append_stream) = mpsc::unbounded::<AppendReq>();
    let append_stream = append_stream.map(LogRequest::Append);

    let (req_sink, read_stream) = mpsc::unbounded::<LogRequest>();
    let req_stream = append_stream.select(read_stream);

    Handle::spawn(
        log_dir,
        req_stream,
        AsyncLog {
            append_sink: append_sink,
            req_sink: req_sink,
        },
        listener,
    )
}

impl AsyncLog {
    pub fn append(&self, payload: AppendReq) {
        self.append_sink.unbounded_send(payload).unwrap();
    }

    pub fn last_offset(&self) -> LogFuture<Option<Offset>> {
        let (snd, recv) = oneshot::channel::<Result<Option<Offset>, Error>>();
        self.req_sink.unbounded_send(LogRequest::LastOffset(snd))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn read(&self, position: Offset, limit: ReadLimit) -> LogFuture<MessageBuf> {
        let (snd, recv) = oneshot::channel::<Result<MessageBuf, Error>>();
        self.req_sink.unbounded_send(
            LogRequest::Read(position, limit, snd)
        ).unwrap();
        LogFuture { f: recv }
    }

    pub fn replicate_from(&self, offset: Offset) -> LogFuture<FileSlice> {
        let (snd, recv) = oneshot::channel::<Result<FileSlice, Error>>();
        self.req_sink.unbounded_send(
            LogRequest::Replicate(offset, snd)
        ).unwrap();
        LogFuture { f: recv }
    }

    pub fn append_from_replication(&self, buf: BytesMut) -> LogFuture<OffsetRange> {
        let (snd, recv) = oneshot::channel::<Result<OffsetRange, Error>>();
        self.req_sink.unbounded_send(
            LogRequest::AppendFromReplication(buf, snd)
        ).unwrap();
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
