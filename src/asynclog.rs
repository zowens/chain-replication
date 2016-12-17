use commitlog::*;
use futures::{Future, Stream, Sink, Async, Poll};
use futures::future::{result, BoxFuture, AndThen};
use futures::sync::oneshot;
use futures::sync::mpsc;
use futures::sink::{Send as SinkSend};
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use std::{thread, mem};
use std::sync::Mutex;
use std::sync::Arc;
use std::intrinsics::likely;

type AppendFuture = oneshot::Sender<Result<Offset, Error>>;

enum LogRequest {
    Append,
    Flush,
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

}

pub struct AsyncLog {
    buf: Arc<Mutex<BufData>>,
    sender: mpsc::Sender<LogRequest>,
    thread: thread::JoinHandle<()>,
}

unsafe impl Send for AsyncLog { }
unsafe impl Sync for AsyncLog { }

impl AsyncLog {
    pub fn open() -> AsyncLog {
        let write_buf = Arc::new(Mutex::new(BufData::new()));
        let buf = write_buf.clone();

        let (sender, mut receiver) = mpsc::channel(1024);

        let th = thread::spawn(move || {
            let mut log = {
                let mut opts = LogOptions::new("log");
                opts.index_max_items(1_000_000);
                CommitLog::new(opts).expect("Unable to open log")
            };
            let mut write_buf = write_buf.clone();
            let mut last_flush = Instant::now();
            for req in receiver.wait().filter_map(|v| v.ok()) {
                match req {
                    LogRequest::Append => {
                        let (buf, mut futures) = {
                            let mut data = write_buf.lock().unwrap();
                            data.empty()
                        };

                        if unsafe { likely(buf.len() > 0) } {
                            trace!("Appending {} messages to the log", buf.len());
                            match log.append(buf) {
                                Ok(range) => {
                                    for (offset, f) in range.iter().zip(futures.into_iter()) {
                                        f.complete(Ok(offset));
                                    }
                                },
                                Err(e) => {
                                    for f in futures.into_iter() {
                                        f.complete(Err(Error::new(ErrorKind::Other, "append error")));
                                    }
                                }
                            }
                            let now = Instant::now();
                            if (now - last_flush) > Duration::from_secs(1) {
                                match log.flush() {
                                    Ok(()) => {
                                        last_flush = now;
                                    },
                                    Err(e) => {
                                        error!("Error flushing the log: {}", e);
                                    },
                                }
                            }
                        } else {
                            info!("No messages to append to the log");
                        }
                    }
                    LogRequest::Read(pos, lim, res) => {
                        res.complete(log.read(pos, lim).map_err(|e| Error::new(ErrorKind::Other, "read error")));
                    }
                }
            }
            ()
        });

        AsyncLog {
            buf: buf,
            sender: sender,
            thread: th,
        }
    }

    pub fn append(&self, payload: &[u8]) -> LogFuture<Offset> {
        trace!("Appending message of size {}", payload.len());
        let (snd, recv) = oneshot::channel::<Result<Offset, Error>>();
        let is_first = {
            let mut buf = self.buf.lock().unwrap();
            let is_empty = buf.futures.is_empty();
            buf.msg_buf.push(payload);
            buf.futures.push(snd);
            is_empty
        };

        let recv: GenErr<oneshot::Receiver<Result<Offset, Error>>> = GenErr{ f: recv };
        if is_first {
            let log_send: GenErr<SinkSend<mpsc::Sender<LogRequest>>> = GenErr {
                f: self.sender.clone().send(LogRequest::Append),
            };

            LogFuture {
                state: LogFutureState::Sending(log_send, recv),
            }
        } else {
            LogFuture {
                state: LogFutureState::Receiving(recv),
            }
        }
    }

    pub fn read(&self, position: ReadPosition, limit: ReadLimit) -> LogFuture<MessageSet> {
        self.send_request(|v| LogRequest::Read(position, limit, v))
    }

    fn send_request<F, R>(&self, f: F) -> LogFuture<R>
        where F: FnOnce(oneshot::Sender<Result<R, Error>>) -> LogRequest,
              R: 'static + Send
    {
        let (snd, recv) = oneshot::channel::<Result<R, Error>>();

        let recv: GenErr<oneshot::Receiver<Result<R, Error>>> = GenErr{ f: recv };
        let log_send: GenErr<SinkSend<mpsc::Sender<LogRequest>>> = GenErr {
            f: self.sender.clone().send(f(snd))
        };
        LogFuture {
            state: LogFutureState::Sending(log_send, recv)
        }
    }
}


struct GenErr<F> where F: Future {
    f: F
}

impl<R, F> Future for GenErr<F>
    where F: Future<Item=R>
{
    type Item = R;
    type Error = Error;

    fn poll(&mut self) -> Poll<R, Error> {
        match self.f.poll() {
            Ok(Async::Ready(v)) => Ok(Async::Ready(v)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => Err(Error::new(ErrorKind::Other, "")),
        }
    }
}

enum LogFutureState<R> {
    Sending(GenErr<SinkSend<mpsc::Sender<LogRequest>>>, GenErr<oneshot::Receiver<Result<R, Error>>>),
    Receiving(GenErr<oneshot::Receiver<Result<R, Error>>>),
    Done,
}

#[inline]
fn collapse<R>(r: Poll<Result<R, Error>, Error>) -> Poll<R, Error> {
    match r {
        Ok(Async::Ready(Ok(v))) => Ok(Async::Ready(v)),
        Ok(Async::Ready(Err(e))) => Err(e),
        Ok(Async::NotReady) => Ok(Async::NotReady),
        Err(e) => Err(e),
    }
}

impl<R> LogFutureState<R>
{
    fn poll(&mut self) -> Poll<R, Error> {
        match *self {
            LogFutureState::Sending(ref mut a, _) => {
                match a.poll() {
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Ok(Async::Ready(t)) => (),
                    Err(e) => return Err(e),
                }
            }
            LogFutureState::Receiving(ref mut b) => return collapse(b.poll()),
            LogFutureState::Done => panic!("cannot poll a chained future twice"),
        };
        let mut res = match mem::replace(self, LogFutureState::Done) {
            LogFutureState::Sending(_, c) => c,
            _ => panic!(),
        };
        let ret = collapse(res.poll());
        *self = LogFutureState::Receiving(res);
        ret
    }
}

pub struct LogFuture<R> {
    state: LogFutureState<R>
}

impl<R> Future for LogFuture<R> {
    type Item = R;
    type Error = Error;

    fn poll(&mut self) -> Poll<R, Error> {
        self.state.poll()
    }
}
