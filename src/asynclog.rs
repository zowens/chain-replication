use commitlog::*;
use futures::{Future, Stream, Async, Poll};
use futures::sync::oneshot;
use futures::sync::mpsc;
use metrics::metrics::{StdMeter, Meter, Metric};
use metrics::reporter::Reporter;
use super::reporter::LogReporter;
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use std::{thread, mem};
use std::sync::Mutex;
use std::sync::Arc;
use std::intrinsics::likely;

type AppendFuture = oneshot::Sender<Result<Offset, Error>>;

enum LogRequest {
    Append,
    Read(ReadPosition, ReadLimit, oneshot::Sender<Result<MessageSet, Error>>),
}

struct BufData {
    msg_buf: MessageBuf,
    futures: Vec<AppendFuture>,
}

impl BufData {
    pub fn new() -> BufData {
        BufData {
            msg_buf: MessageBuf::new(),
            futures: vec![],
        }
    }

    pub fn empty(&mut self) -> (MessageBuf, Vec<AppendFuture>) {
        let mut buf = MessageBuf::new();
        let mut futures = vec![];
        mem::swap(&mut buf, &mut self.msg_buf);
        mem::swap(&mut futures, &mut self.futures);
        (buf, futures)
    }

    pub fn push(&mut self, payload: &[u8], snd: oneshot::Sender<Result<Offset, Error>>) -> bool {
        let is_empty = self.futures.is_empty();
        self.msg_buf.push(payload);
        self.futures.push(snd);
        is_empty
    }
}

pub struct AsyncLog {
    buf: Arc<Mutex<BufData>>,
    sender: mpsc::UnboundedSender<LogRequest>,
}

unsafe impl Send for AsyncLog {}
unsafe impl Sync for AsyncLog {}

impl AsyncLog {
    pub fn open() -> AsyncLog {
        let mut reporter = LogReporter::new(10_000);
        let write_buf = Arc::new(Mutex::new(BufData::new()));
        let buf = write_buf.clone();

        let (sender, receiver) = mpsc::unbounded();

        thread::spawn(move || {
            let meter = StdMeter::new();
            reporter.add("appends", Metric::Meter(meter.clone())).unwrap();

            let mut log = {
                let mut opts = LogOptions::new("log");
                opts.index_max_items(10_000_000);
                opts.segment_max_bytes(512_000_000);
                CommitLog::new(opts).expect("Unable to open log")
            };
            let write_buf = write_buf.clone();
            let mut last_flush = Instant::now();
            let iter = receiver.wait().filter_map(|v| {
                match v {
                    Ok(v) => Some(v),
                    Err(e) => {
                        warn!("NOT OK {:?}", e);
                        None
                    }
                }
            });
            for req in iter {
                match req {
                    LogRequest::Append => {
                        trace!("Append message on receiver");
                        let (buf, futures) = {
                            let mut data = if let Ok(v) = write_buf.lock() {
                                v
                            } else {
                                error!("Unable to obtain lock");
                                panic!("Unable to obtain lock");
                            };
                            data.empty()
                        };

                        if unsafe { likely(buf.len() > 0) } {
                            trace!("Appending {} messages to the log", buf.len());
                            match log.append(buf) {
                                Ok(range) => {
                                    meter.mark(futures.len() as i64);
                                    for (offset, f) in range.iter().zip(futures.into_iter()) {
                                        trace!("Appended offset {} to the log", offset);
                                        f.complete(Ok(offset));
                                    }
                                }
                                Err(e) => {
                                    error!("Unable to append to the log {}", e);
                                    for f in futures.into_iter() {
                                        f.complete(Err(Error::new(ErrorKind::Other,
                                                                  "append error")));
                                    }
                                }
                            }
                            let now = Instant::now();
                            if (now - last_flush) > Duration::from_secs(1) {
                                match log.flush() {
                                    Ok(()) => {
                                        last_flush = now;
                                    }
                                    Err(e) => {
                                        error!("Error flushing the log: {}", e);
                                    }
                                }
                            }
                        } else {
                            info!("No messages to append to the log");
                        }
                    }
                    LogRequest::Read(pos, lim, res) => {
                        res.complete(log.read(pos, lim)
                            .map_err(|_| Error::new(ErrorKind::Other, "read error")));
                    }
                }
            }
            ()
        });

        AsyncLog {
            buf: buf,
            sender: sender,
        }
    }

    pub fn append(&self, payload: &[u8]) -> LogFuture<Offset> {
        trace!("Appending message of size {}", payload.len());
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        let is_first = {
            let mut buf = self.buf.lock().unwrap();
            buf.push(payload, snd)
        };

        if is_first {
            trace!("First message in the queue");
            let mut sender: mpsc::UnboundedSender<LogRequest> = self.sender.clone();
            <mpsc::UnboundedSender<LogRequest>>::send(&mut sender, LogRequest::Append).unwrap();
        }
        LogFuture { f: recv }
    }

    pub fn read(&self, position: ReadPosition, limit: ReadLimit) -> LogFuture<MessageSet> {
        let (snd, recv) = oneshot::channel::<Result<MessageSet, Error>>();
        let mut sender: mpsc::UnboundedSender<LogRequest> = self.sender.clone();
        <mpsc::UnboundedSender<LogRequest>>::send(&mut sender,
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
