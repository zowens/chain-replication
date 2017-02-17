use commitlog::*;
use commitlog::message::*;
use futures::{Stream, Future, Async, Poll, Sink, StartSend, AsyncSink};
use futures::future::BoxFuture;
use futures_cpupool::CpuPool;
use futures::sync::oneshot;
use tokio_core::io::{EasyBuf, EasyBufMut};
use futures::sync::mpsc;
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use messages::*;

mod queue;
mod batched_mpsc;

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
    LastOffset(oneshot::Sender<Result<Option<Offset>, Error>>),
    Read(Offset, ReadLimit, oneshot::Sender<Result<MessageBuf, Error>>),
    Replicate(Offset, oneshot::Sender<Result<FileSlice, Error>>),
    AppendFromReplication(EasyBuf, oneshot::Sender<Result<OffsetRange, Error>>),
}

type AppendFuture = oneshot::Sender<Result<Offset, Error>>;
type AppendReq = (EasyBuf, AppendFuture);

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,

    msg_buf: MessageBuf,
    parked_replication: Option<(Offset, oneshot::Sender<Result<FileSlice, Error>>)>,
}

impl LogSink {
    fn new(log: CommitLog) -> LogSink {
        LogSink {
            log: log,
            last_flush: Instant::now(),
            dirty: false,
            /*pool: Pool::with_capacity(30, 0, || {
                PooledBuf(MessageBuf::from_bytes(Vec::with_capacity(16_384)).unwrap())
            }),*/
            msg_buf: MessageBuf::from_bytes(Vec::with_capacity(16_384)).unwrap(),
            parked_replication: None,
        }
    }

    fn send_to_replica(&mut self) {
        if let Some((offset, res)) = self.parked_replication.take() {
            debug!("Sending messages to replica");
            let read_res = self.log
                .reader::<FileSliceMessageReader>(offset, ReadLimit::max_bytes(3072));
            match read_res {
                Ok(Some(fs)) => res.complete(Ok(fs)),
                Ok(None) => {
                    debug!("Re-parking replication");
                    self.parked_replication = Some((offset, res));
                }
                Err(ReadError::Io(e)) => res.complete(Err(e)),
                Err(ReadError::CorruptLog) => {
                    res.complete(Err(Error::new(ErrorKind::Other, "Corrupt log detected")))
                }
                Err(ReadError::NoSuchSegment) => {
                    res.complete(Err(Error::new(ErrorKind::Other, "read error")))
                }
            }
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
                unsafe { self.msg_buf.unsafe_clear() };
                for (bytes, f) in reqs.into_iter() {
                    self.msg_buf.push(bytes);
                    futures.push(f);
                }

                match self.log.append(&mut self.msg_buf) {
                    Ok(range) => {
                        self.send_to_replica();
                        for (offset, f) in range.iter().zip(futures) {
                            trace!("Appended offset {} to the log", offset);
                            f.complete(Ok(offset));
                        }
                        self.dirty = true;
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
                let appended_range = {
                    // TODO: assert and error if we are appending a message that is not
                    // in log sequence
                    let mut ms = ReplicationMessages::new(&mut buf);

                    {
                        // TODO: check this up-front or send error up with res
                        assert!(ms.verify_hashes().is_ok(),
                                "Attempt to append message set with invalid hashes");

                        let first_msg = ms.iter()
                            .next()
                            .expect("Expected append from replication to be non-empty");
                        match self.log.last_offset() {
                            Some(o) => {
                                assert_eq!(o + 1,
                                           first_msg.offset(),
                                           "Expected append from replication to be in sequence")
                            }
                            None => {
                                assert_eq!(0,
                                           first_msg.offset(),
                                           "Expected append from replication to be in sequence")
                            }
                        }
                    }

                    match self.log.append(&mut ms) {
                        Ok(range) => range,
                        Err(e) => {
                            error!("Unable to append to the log {}", e);
                            res.complete(Err(Error::new(ErrorKind::Other, "append error")));
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
                res.complete(Ok(appended_range));
            }
            LogRequest::LastOffset(res) => {
                res.complete(Ok(self.log.last_offset()));
            }
            LogRequest::Read(pos, lim, res) => {
                res.complete(self.log
                    .read(pos, lim)
                    // TODO: pool?
                    .map_err(|_| Error::new(ErrorKind::Other, "read error")));
            }
            LogRequest::Replicate(offset, res) => {
                debug!("Replicate command from {}", offset);
                let read_result = self.log
                    .reader::<FileSliceMessageReader>(offset, ReadLimit::max_bytes(3072));

                match read_result {
                    Ok(Some(fs)) => {
                        debug!("replicating existing part of the log immediately");
                        res.complete(Ok(fs))
                    }
                    Ok(None) => {
                        debug!("Parking replicate command");
                        self.parked_replication = Some((offset, res));
                    }
                    Err(ReadError::Io(e)) => res.complete(Err(e)),
                    Err(ReadError::CorruptLog) => {
                        res.complete(Err(Error::new(ErrorKind::Other, "Corrupt log detected")))
                    }
                    Err(ReadError::NoSuchSegment) => {
                        res.complete(Err(Error::new(ErrorKind::Other, "read error")))
                    }
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

    pub fn append_from_replication(&self, buf: EasyBuf) -> LogFuture<OffsetRange> {
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
