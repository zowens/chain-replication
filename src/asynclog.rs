use commitlog::*;
use futures::{Stream, Future, Async, Poll, Sink, StartSend, AsyncSink};
use futures::future::BoxFuture;
use futures_cpupool::CpuPool;
use futures::sync::oneshot;
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use tokio_core::io::EasyBuf;
use futures::sync::mpsc;

/// Request sent through the `Sink` for the log
enum LogRequest {
    Append(Vec<AppendReq>),
    LastOffset(oneshot::Sender<Result<Offset, Error>>),
    Read(ReadPosition, ReadLimit, oneshot::Sender<Result<MessageSet, Error>>),
}

type AppendFuture = oneshot::Sender<Result<Offset, Error>>;
type AppendReq = (EasyBuf, AppendFuture);

/// Wrapper stream that attempts to batch messages.
struct MsgBatchStream<S: Stream> {
    stream: S,
}

impl<S> Stream for MsgBatchStream<S>
    where S: Stream<Item = AppendReq, Error = ()>
{
    type Item = LogRequest;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<LogRequest>, ()> {
        // make sure we have at least one message to append
        let first_val = match try_ready!(self.stream.poll()) {
            Some(v) => v,
            None => return Ok(Async::Ready(None)),
        };

        let mut reqs = Vec::new();
        // add the first message
        reqs.push(first_val);

        // look for more!
        loop {
            match self.stream.poll() {
                Ok(Async::Ready(Some(v))) => {
                    reqs.push(v);
                }
                _ => {
                    trace!("appending {} messages", reqs.len());
                    return Ok(Async::Ready(Some(LogRequest::Append(reqs))));
                }
            }
        }
    }
}

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,
    buf: MessageBuf,
}

impl LogSink {
    fn new(log: CommitLog) -> LogSink {
        LogSink {
            log: log,
            last_flush: Instant::now(),
            dirty: false,
            buf: MessageBuf::new(),
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
                for (bytes, f) in reqs {
                    self.buf.push(bytes);
                    futures.push(f);
                }

                match self.log.append(&mut self.buf) {
                    Ok(range) => {
                        for (offset, f) in range.iter().zip(futures.into_iter()) {
                            trace!("Appended offset {} to the log", offset);
                            f.complete(Ok(offset));
                        }
                        self.dirty = true;
                        self.buf.clear();
                    }
                    Err(e) => {
                        error!("Unable to append to the log {}", e);
                        for f in futures {
                            f.complete(Err(Error::new(ErrorKind::Other, "append error")));
                        }
                    }
                }
            }
            LogRequest::LastOffset(res) => {
                res.complete(Ok(self.log.last_offset().unwrap_or(Offset(0))));
            }
            LogRequest::Read(pos, lim, res) => {
                res.complete(self.log
                    .read(pos, lim)
                    .map_err(|_| Error::new(ErrorKind::Other, "read error")));
            }
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("poll_complete");
        if self.dirty {
            let now = Instant::now();
            if (now - self.last_flush) > Duration::from_secs(1) {
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
    append_sink: mpsc::UnboundedSender<AppendReq>,
    read_sink: mpsc::UnboundedSender<LogRequest>,
}

/// Handle that prevents the dropping of the thread for the `CommitLog` operations.
pub struct Handle {
    #[allow(dead_code)]
    pool: CpuPool,
    #[allow(dead_code)]
    f: BoxFuture<(), ()>,
}

impl Handle {
    fn spawn<S>(stream: S) -> Handle
        where S: Stream<Item = LogRequest, Error = ()>,
              S: Send + 'static
    {
        let pool = CpuPool::new(1);
        let log = {
            let mut opts = LogOptions::new("log");
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
    pub fn open() -> (Handle, AsyncLog) {
        let (append_sink, append_stream) = mpsc::unbounded::<AppendReq>();
        let append_stream = MsgBatchStream { stream: append_stream };

        let (read_sink, read_stream) = mpsc::unbounded::<LogRequest>();
        let req_stream = append_stream.select(read_stream);


        (Handle::spawn(req_stream),
         AsyncLog {
             append_sink: append_sink,
             read_sink: read_sink,
         })
    }

    pub fn append(&self, payload: EasyBuf) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        let mut sender = self.append_sink.clone();
        <mpsc::UnboundedSender<AppendReq>>::send(&mut sender, (payload, snd)).unwrap();
        LogFuture { f: recv }
    }

    pub fn last_offset(&self) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        let mut sender = self.read_sink.clone();
        <mpsc::UnboundedSender<LogRequest>>::send(&mut sender, LogRequest::LastOffset(snd))
            .unwrap();
        LogFuture { f: recv }

    }

    pub fn read(&self, position: ReadPosition, limit: ReadLimit) -> LogFuture<MessageSet> {
        let (snd, recv) = oneshot::channel::<Result<MessageSet, Error>>();
        let mut sender = self.read_sink.clone();
        <mpsc::UnboundedSender<LogRequest>>::send(&mut sender,
                                                  LogRequest::Read(position, limit, snd))
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
                error!("{}", e);
                Err(e)
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                error!("{}", e);
                Err(Error::new(ErrorKind::Other, "Cancelled"))
            }
        }
    }
}
