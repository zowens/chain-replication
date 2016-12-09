use commitlog::*;
use futures::{Future, Stream, Sink, Async, Poll};
use futures::future::{result, BoxFuture, AndThen};
use futures::sync::oneshot;
use futures::sync::mpsc;
use futures::sink::{Send as SinkSend};
use std::io::{Error, ErrorKind};
use std::time::{Instant, Duration};
use std::{thread, mem};

enum LogRequest {
    Append(Vec<u8>, oneshot::Sender<Result<Offset, Error>>),
    Read(ReadPosition, ReadLimit, oneshot::Sender<Result<Vec<Message>, Error>>),
}

pub struct AsyncLog {
    sender: mpsc::Sender<LogRequest>,
    thread: thread::JoinHandle<()>,
}

unsafe impl Send for AsyncLog { }
unsafe impl Sync for AsyncLog { }

impl AsyncLog {
    pub fn open() -> AsyncLog {
        let (sender, mut receiver) = mpsc::channel(1024);
        let th = thread::spawn(move || {
            let mut opts = LogOptions::new("log");
            opts.index_max_items(1_000_000);

            let mut log = match CommitLog::new(opts) {
                Ok(v) => v,
                Err(e) => {
                    receiver.close();
                    return;
                }
            };

            let mut last_flush = Instant::now();
            for req in receiver.wait().filter_map(|v| v.ok()) {
                match req {
                    LogRequest::Append(msg, res) => {
                        res.complete(log.append(&msg).map_err(|e| Error::new(ErrorKind::Other, "append error")));
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

                    }
                    LogRequest::Read(pos, lim, res) => {
                        res.complete(log.read(pos, lim).map_err(|e| Error::new(ErrorKind::Other, "read error")));
                    }
                }
            }
            ()
        });

        AsyncLog {
            sender: sender,
            thread: th,
        }
    }

    pub fn append(&self, payload: Vec<u8>) -> LogFuture<Offset> {
        self.send_request(|v| LogRequest::Append(payload, v))
    }

    pub fn read(&self, position: ReadPosition, limit: ReadLimit) -> LogFuture<Vec<Message>> {
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
