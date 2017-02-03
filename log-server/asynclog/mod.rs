use commitlog::*;
use futures::{Stream, Future, Async, Poll, Sink, StartSend, AsyncSink};
use futures::future::BoxFuture;
use futures_cpupool::CpuPool;
use futures::sync::oneshot;
use tokio_core::io::{EasyBuf, EasyBufMut};
use futures::sync::mpsc;
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use pool::{Pool, Checkout, Reset};

mod queue;
mod batched_mpsc;

struct PooledBuf(MessageBuf);
impl Reset for PooledBuf {
    fn reset(&mut self) {
        unsafe {
            self.0.unsafe_clear();
        }
    }
}

pub struct Messages {
    inner: MessagesInner,
}

impl Messages {
    pub fn new(buf: MessageBuf) -> Messages {
        Messages { inner: MessagesInner::Unpooled(buf) }
    }
}

enum MessagesInner {
    Pooled(Checkout<PooledBuf>),
    Unpooled(MessageBuf),
    Readonly(EasyBuf),
}

impl Messages {
    fn push<B: AsRef<[u8]>>(&mut self, bytes: B) {
        match self.inner {
            MessagesInner::Pooled(ref mut co) => co.0.push(bytes.as_ref()),
            MessagesInner::Unpooled(ref mut buf) => buf.push(bytes.as_ref()),
            MessagesInner::Readonly(_) => panic!("Readonly Messages should not be used to append messages"),
        }
    }
}

impl MessageSet for Messages {
    fn bytes(&self) -> &[u8] {
        match self.inner {
            MessagesInner::Pooled(ref co) => co.0.bytes(),
            MessagesInner::Unpooled(ref buf) => buf.bytes(),
            MessagesInner::Readonly(ref buf) => buf.as_slice(),
        }
    }

    fn len(&self) -> usize {
        match self.inner {
            MessagesInner::Pooled(ref co) => co.0.len(),
            MessagesInner::Unpooled(ref buf) => buf.len(),
            MessagesInner::Readonly(_) => MessageSet::len(self),
        }
    }
}

impl AsMut<[u8]> for Messages {
    fn as_mut(&mut self) -> &mut [u8] {
        match self.inner {
            MessagesInner::Pooled(ref mut co) => co.0.bytes_mut(),
            MessagesInner::Unpooled(ref mut buf) => buf.bytes_mut(),
            MessagesInner::Readonly(_) => panic!("Readonly should not be used as mutable"),
        }
    }
}

impl MessageSetMut for Messages {
    type ByteMut = Messages;
    fn bytes_mut(&mut self) -> &mut Messages {
        self
    }
}

struct ReplicationMessages<'a>(EasyBufMut<'a>);

impl<'a> ReplicationMessages<'a> {
    fn new(buf: &'a mut EasyBuf) -> ReplicationMessages<'a> {
        ReplicationMessages(buf.get_mut())
    }
}

impl<'a> MessageSet for ReplicationMessages<'a> {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<'a> MessageSetMut for ReplicationMessages<'a> {
    type ByteMut = Vec<u8>;
    fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

/// Request sent through the `Sink` for the log
enum LogRequest {
    Append(Vec<AppendReq>),
    LastOffset(oneshot::Sender<Result<Offset, Error>>),
    Read(ReadPosition, ReadLimit, oneshot::Sender<Result<Messages, Error>>),
    Replicate(Offset, oneshot::Sender<Result<ReplicationResponse, Error>>),
    AppendFromReplication(EasyBuf, oneshot::Sender<Result<(), Error>>),
}

pub enum ReplicationResponse {
    /// notes that the replica is out of sync and must request replication again.
    Lagging(Messages),
    /// notes that the replica is in-sync and returns a stream for further responses.
    InSync(mpsc::UnboundedReceiver<Messages>),
}

type AppendFuture = oneshot::Sender<Result<Offset, Error>>;
type AppendReq = (EasyBuf, AppendFuture);

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,
    pool: Pool<PooledBuf>,
    replication_stream: Option<mpsc::UnboundedSender<Messages>>,
}

impl LogSink {
    fn new(log: CommitLog) -> LogSink {
        LogSink {
            log: log,
            last_flush: Instant::now(),
            dirty: false,
            pool: Pool::with_capacity(30, 0, || {
                PooledBuf(MessageBuf::from_bytes(Vec::with_capacity(16_384)).unwrap())
            }),
            replication_stream: None,
        }
    }

    fn send_to_replica(&mut self, msgs: Messages) {
        let cancel_replication = match self.replication_stream.as_ref() {
            Some(stream) => {
                <mpsc::UnboundedSender<Messages>>::send(stream, msgs).is_err()
            }
            None => false,
        };
        if cancel_replication {
            info!("Stopping replication due to a dropped receiver");
            self.replication_stream = None;
        }
    }
}

impl Sink for LogSink {
    type SinkItem = LogRequest;
    type SinkError = ();

    fn start_send(&mut self, item: LogRequest) -> StartSend<LogRequest, ()> {
        trace!("start_send");
        match item {
            LogRequest::Append(reqs) => {
                let mut futures = Vec::with_capacity(reqs.len());
                let mut buf = self.pool
                    .checkout()
                    .map(|buf| Messages { inner: MessagesInner::Pooled(buf) })
                    .unwrap_or_else(|| {
                        Messages { inner: MessagesInner::Unpooled(MessageBuf::default()) }
                    });
                for (bytes, f) in reqs {
                    buf.push(bytes);
                    futures.push(f);
                }

                match self.log.append(&mut buf) {
                    Ok(range) => {
                        for (offset, f) in range.iter().zip(futures.into_iter()) {
                            trace!("Appended offset {} to the log", offset);
                            f.complete(Ok(offset));
                        }
                        self.dirty = true;
                        self.send_to_replica(buf);
                    }
                    Err(e) => {
                        error!("Unable to append to the log {}", e);
                        for f in futures {
                            f.complete(Err(Error::new(ErrorKind::Other, "append error")));
                        }
                    }
                }
            }
            LogRequest::AppendFromReplication(mut buf, res) => {
                {
                    let mut ms = ReplicationMessages::new(&mut buf);
                    if let Err(e) = self.log.append(&mut ms) {
                        error!("Unable to append to the log {}", e);
                        res.complete(Err(Error::new(ErrorKind::Other, "append error")));
                        return Ok(AsyncSink::Ready);
                    }
                    trace!("Replicated to log");
                }
                self.dirty = true;
                self.send_to_replica(Messages { inner: MessagesInner::Readonly(buf) });
                res.complete(Ok(()));
            }
            LogRequest::LastOffset(res) => {
                res.complete(Ok(self.log.last_offset().unwrap_or(Offset(0))));
            }
            LogRequest::Read(pos, lim, res) => {
                res.complete(self.log
                    .read(pos, lim)
                    // TODO: pool
                    .map(|buf| Messages { inner: MessagesInner::Unpooled(buf) })
                    .map_err(|_| Error::new(ErrorKind::Other, "read error")));
            }
            LogRequest::Replicate(offset, res) => {
                debug!("Replicate command from {}", offset);
                let last_off = self.log.last_offset();
                let lagging_read = match last_off {
                    Some(Offset(o)) if offset.0 < o => true,
                    _ => false,
                };

                if lagging_read {
                    debug!("replicating existing part of the log immediately");
                    res.complete(self.log
                        // TODO: pool this
                        .read(ReadPosition::Offset(offset), ReadLimit::Bytes(4096))
                        .map(|buf| ReplicationResponse::Lagging(Messages::new(buf)))
                        .map_err(|_| Error::new(ErrorKind::Other, "read error")));
                } else {
                    debug!("Parking replicate command");
                    // TODO: unbounded will probably get us into trouble
                    let (snd, recv) = mpsc::unbounded::<Messages>();
                    self.replication_stream = Some(snd);
                    res.complete(Ok(ReplicationResponse::InSync(recv)));
                }
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
                        error!("Flush error: {}", e);
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
    append_sink: batched_mpsc::UnboundedSender<AppendReq>,
    req_sink: mpsc::UnboundedSender<LogRequest>,
}

/// Handle that prevents the dropping of the thread for the `CommitLog` operations.
pub struct Handle {
    #[allow(dead_code)]
    pool: CpuPool,
    #[allow(dead_code)]
    f: BoxFuture<(), ()>,
}

impl Handle {
    fn spawn<S>(log_dir: &str, stream: S) -> Handle
        where S: Stream<Item = LogRequest, Error = ()>,
              S: Send + 'static
    {
        let pool = CpuPool::new(1);
        let log = {
            let mut opts = LogOptions::new(log_dir);
            opts.index_max_items(10_000_000);
            opts.segment_max_bytes(1024_000_000);
            CommitLog::new(opts).expect("Unable to open log")
        };
        let f = pool.spawn(LogSink::new(log)
                .send_all(stream)
                .map(|_| ()))
            .boxed();
        Handle { pool: pool, f: f }
    }
}

impl AsyncLog {
    pub fn open(log_dir: &str) -> (Handle, AsyncLog) {
        let (append_sink, append_stream) = batched_mpsc::unbounded::<AppendReq>();
        let append_stream = append_stream.map(LogRequest::Append);

        let (req_sink, read_stream) = mpsc::unbounded::<LogRequest>();
        let req_stream = append_stream.select(read_stream);


        (Handle::spawn(log_dir, req_stream),
         AsyncLog {
             append_sink: append_sink,
             req_sink: req_sink,
         })
    }

    pub fn append(&self, payload: EasyBuf) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        <batched_mpsc::UnboundedSender<AppendReq>>::send(&self.append_sink, (payload, snd)).unwrap();
        LogFuture { f: recv }
    }

    pub fn last_offset(&self) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink, LogRequest::LastOffset(snd))
            .unwrap();
        LogFuture { f: recv }

    }

    pub fn read(&self, position: ReadPosition, limit: ReadLimit) -> LogFuture<Messages> {
        let (snd, recv) = oneshot::channel::<Result<Messages, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink,
                                                  LogRequest::Read(position, limit, snd))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn replicate_from(&self, offset: Offset) -> LogFuture<ReplicationResponse> {
        let (snd, recv) = oneshot::channel::<Result<ReplicationResponse, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink,
                                                  LogRequest::Replicate(offset, snd))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn append_from_replication(&self, buf: EasyBuf) -> LogFuture<()> {
        let (snd, recv) = oneshot::channel::<Result<(), Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink,
                                                  LogRequest::AppendFromReplication(buf, snd))
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
                error!("Encounrted cancellation: {}", e);
                Err(Error::new(ErrorKind::Other, "Cancelled"))
            }
        }
    }
}
