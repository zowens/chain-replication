use commitlog::*;
use futures::{Stream, Future, Async, Poll, Sink, StartSend, AsyncSink};
use futures::future::BoxFuture;
use futures_cpupool::CpuPool;
use futures::sync::oneshot;
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use tokio_core::io::EasyBuf;
use futures::sync::mpsc;

enum LogRequest {
    Append(MessageBuf, Vec<AppendFuture>),
    LastOffset(oneshot::Sender<Result<Offset, Error>>),
    Read(ReadPosition, ReadLimit, oneshot::Sender<Result<MessageSet, Error>>),
}

struct MsgBatchStream<S: Stream> {
    stream: S,
}

impl<S> Stream for MsgBatchStream<S>
    where S: Stream<Item = AppendReq, Error = ()>
{
    type Item = LogRequest;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<LogRequest>, ()> {
        let first_val = match try_ready!(self.stream.poll()) {
            Some(v) => v,
            None => return Ok(Async::Ready(None)),
        };

        let mut buf = MessageBuf::new();
        let mut futures = Vec::new();
        buf.push(first_val.0.as_slice());
        futures.push(first_val.1);

        loop {
            match self.stream.poll() {
                Ok(Async::Ready(Some(v))) => {
                    buf.push(v.0.as_slice());
                    futures.push(v.1);
                }
                _ => {
                    trace!("appending {} messages", futures.len());
                    return Ok(Async::Ready(Some(LogRequest::Append(buf, futures))));
                }
            }
        }
    }
}


struct LogSink {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,
}

impl LogSink {
    fn new(log: CommitLog) -> LogSink {
        LogSink {
            log: log,
            last_flush: Instant::now(),
            dirty: false,
        }
    }
}

impl Sink for LogSink {
    type SinkItem = LogRequest;
    type SinkError = ();

    fn start_send(&mut self, item: LogRequest) -> StartSend<LogRequest, ()> {
        trace!("start_send");
        match item {
            LogRequest::Append(buf, futures) => {
                match self.log.append(buf) {
                    Ok(range) => {
                        for (offset, f) in range.iter().zip(futures.into_iter()) {
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

#[derive(Clone)]
pub struct AsyncLog {
    append_sink: mpsc::UnboundedSender<AppendReq>,
    read_sink: mpsc::UnboundedSender<LogRequest>,
}

unsafe impl Send for AsyncLog {}
unsafe impl Sync for AsyncLog {}

type AppendFuture = oneshot::Sender<Result<Offset, Error>>;
type AppendReq = (EasyBuf, AppendFuture);

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

    pub fn append(&mut self, payload: EasyBuf) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        <mpsc::UnboundedSender<AppendReq>>::send(&mut self.append_sink, (payload, snd)).unwrap();
        LogFuture { f: recv }
    }

    pub fn last_offset(&mut self) -> LogFuture<Offset> {
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&mut self.read_sink, LogRequest::LastOffset(snd))
            .unwrap();
        LogFuture { f: recv }

    }

    pub fn read(&mut self, position: ReadPosition, limit: ReadLimit) -> LogFuture<MessageSet> {
        let (snd, recv) = oneshot::channel::<Result<MessageSet, Error>>();
        <mpsc::UnboundedSender<LogRequest>>::send(&mut self.read_sink,
                                                  LogRequest::Read(position, limit, snd))
            .unwrap();
        LogFuture { f: recv }
    }
}

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
