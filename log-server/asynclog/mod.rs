use commitlog::*;
use commitlog::message::*;
use futures::{Stream, Future, Async, Poll, Sink, StartSend, AsyncSink};
use futures::future::BoxFuture;
use futures_cpupool::CpuPool;
use futures::sync::oneshot;
use futures::sync::mpsc;
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use messages::*;
use asynclog::queue::MessageBatch;
use bytes::BytesMut;

mod queue;
mod batched_mpsc;

macro_rules! ignore {
    ($res:expr) => (
        $res.unwrap_or(())
    )
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

type LogSender<T, E> = oneshot::Sender<Result<T, E>>;

/// Request sent through the `Sink` for the log
enum LogRequest {
    Append(MessageBatch<AppendReq>),
    LastOffset(LogSender<Option<Offset>, Error>),
    Read(Offset, ReadLimit, LogSender<MessageBuf, Error>),
    Replicate(Offset, LogSender<FileSlice, Error>),
    AppendFromReplication(BytesMut, LogSender<OffsetRange, Error>),
}

type AppendReq = (BytesMut, LogSender<Offset, Error>);

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,

    msg_buf: MessageBuf,
    parked_replication: Option<(Offset, LogSender<FileSlice, Error>)>,
}

impl LogSink {
    fn new(log: CommitLog) -> LogSink {
        LogSink {
            log: log,
            last_flush: Instant::now(),
            dirty: false,
            msg_buf: MessageBuf::from_bytes(Vec::with_capacity(16_384)).unwrap(),
            parked_replication: None,
        }
    }

    /// Trys to replicate via a log read, parking if the offset has not yet been appended.
    fn try_replicate(&mut self, offset: Offset, res: LogSender<FileSlice, Error>) {
        let mut rd = FileSliceMessageReader;
        let read_res = self.log
            .reader(&mut rd, offset, ReadLimit::max_bytes(3072));
        match read_res {
            Ok(Some(fs)) => ignore!(res.send(Ok(fs))),
            Ok(None) => {
                debug!("Parking replication, no offset {}", offset);
                self.parked_replication = Some((offset, res));
            }
            Err(ReadError::Io(e)) => ignore!(res.send(Err(e))),
            Err(ReadError::CorruptLog) => {
                ignore!(res.send(Err(Error::new(ErrorKind::Other, "Corrupt log detected"))));
            }
            Err(ReadError::NoSuchSegment) => {
                ignore!(res.send(Err(Error::new(ErrorKind::Other, "read error"))));
            }
        }
    }

    fn send_to_replica(&mut self) {
        if let Some((offset, res)) = self.parked_replication.take() {
            debug!("Sending messages to parked replication request");
            self.try_replicate(offset, res);
        }
    }
}

impl Sink for LogSink {
    type SinkItem = LogRequest;
    type SinkError = ();

    fn start_send(&mut self, item: LogRequest) -> StartSend<LogRequest, ()> {
        trace!("start_send from log");
        match item {
            LogRequest::Append(reqs) => {
                //let mut futures = Vec::with_capacity(reqs.len());
                unsafe { self.msg_buf.unsafe_clear() };
                for &(ref bytes, _) in reqs.iter() {
                    self.msg_buf.push(bytes);
                    //futures.push(f);
                }

                let futures = reqs.into_iter().map(|v| v.1);
                match self.log.append(&mut self.msg_buf) {
                    Ok(range) => {
                        self.send_to_replica();
                        for (offset, f) in range.iter().zip(futures) {
                            trace!("Appended offset {} to the log", offset);
                            ignore!(f.send(Ok(offset)));
                        }
                        self.dirty = true;
                    }
                    Err(e) => {
                        error!("Unable to append to the log {}", e);
                        for f in futures {
                            ignore!(f.send(Err(Error::new(ErrorKind::Other, "append error"))));
                        }
                    }
                }
            }
            LogRequest::AppendFromReplication(buf, res) => {
                let appended_range = {
                    let mut ms = ReplicationMessages(buf);

                    // assert that the upstream server replicated the correct offset and
                    // that the message hash values match the payloads
                    {
                        // TODO: send error in res rather than assert
                        assert!(ms.verify_hashes().is_ok(),
                                "Attempt to append message set with invalid hashes");

                        let first_msg =
                            ms.iter()
                                .next()
                                .expect("Expected append from replication to be non-empty");
                        let expected_offset = self.log.last_offset().map(|v| v + 1).unwrap_or(0);
                        assert_eq!(expected_offset,
                                   first_msg.offset(),
                                   "Expected append from replication to be in sequence");
                    }

                    match self.log.append(&mut ms) {
                        Ok(range) => range,
                        Err(e) => {
                            error!("Unable to append to the log {}", e);
                            ignore!(res.send(Err(Error::new(ErrorKind::Other, "append error"))));
                            return Ok(AsyncSink::Ready);
                        }
                    }
                };

                let start_offset = appended_range.first();
                let next_offset = appended_range.iter().next_back().unwrap() + 1;
                trace!("Replicated to log, starting at {}, next offset is {}",
                       start_offset,
                       next_offset);
                self.dirty = true;
                self.send_to_replica();
                ignore!(res.send(Ok(appended_range)));
            }
            LogRequest::LastOffset(res) => {
                ignore!(res.send(Ok(self.log.last_offset())));
            }
            LogRequest::Read(pos, lim, res) => {
                // TODO: allow file slice to be sent (zero copy all the things!)
                ignore!(res.send(self.log
                                     .read(pos, lim)
                                     .map_err(|_| Error::new(ErrorKind::Other, "read error"))));
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
        let f = pool.spawn(LogSink::new(log).send_all(stream).map(|_| ()))
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

    pub fn append(&self, payload: BytesMut) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        <batched_mpsc::UnboundedSender<AppendReq>>::send(&self.append_sink, (payload, snd))
            .unwrap();
        LogFuture { f: recv }
    }

    pub fn last_offset(&self) -> LogFuture<Option<Offset>> {
        let (snd, recv) = oneshot::channel::<Result<Option<Offset>, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink, LogRequest::LastOffset(snd))
            .unwrap();
        LogFuture { f: recv }

    }

    pub fn read(&self, position: Offset, limit: ReadLimit) -> LogFuture<MessageBuf> {
        let (snd, recv) = oneshot::channel::<Result<MessageBuf, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink,
                                                  LogRequest::Read(position, limit, snd))
                .unwrap();
        LogFuture { f: recv }
    }

    pub fn replicate_from(&self, offset: Offset) -> LogFuture<FileSlice> {
        let (snd, recv) = oneshot::channel::<Result<FileSlice, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&self.req_sink,
                                                  LogRequest::Replicate(offset, snd))
                .unwrap();
        LogFuture { f: recv }
    }

    pub fn append_from_replication(&self, buf: BytesMut) -> LogFuture<OffsetRange> {
        let (snd, recv) = oneshot::channel::<Result<OffsetRange, Error>>();
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
