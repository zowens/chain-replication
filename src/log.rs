use commitlog::*;
use futures::{Future, Stream, Sink};
use futures::future::{result, BoxFuture};
use futures::sync::oneshot;
use futures::sync::mpsc;
use std::thread;
use std::io::{Error, ErrorKind};

enum LogRequest {
    Append(Vec<u8>, oneshot::Sender<Result<Offset, AppendError>>),
    Read(ReadPosition, ReadLimit, oneshot::Sender<Result<Vec<Message>, ReadError>>),
    Flush(oneshot::Sender<Result<(), Error>>),
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
            let mut log = match CommitLog::new(LogOptions::new("log")) {
                Ok(v) => v,
                Err(e) => {
                    receiver.close();
                    return;
                }
            };

            for req in receiver.wait().filter_map(|v| v.ok()) {
                match req {
                    LogRequest::Append(msg, res) => {
                        res.complete(log.append(&msg));
                    }
                    LogRequest::Read(pos, lim, res) => {
                        res.complete(log.read(pos, lim));
                    }
                    LogRequest::Flush(res) => {
                        res.complete(log.flush());
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

    pub fn append(&self, payload: Vec<u8>) -> BoxFuture<Offset, Error> {
        self.send_request(|v| LogRequest::Append(payload, v))
    }

    pub fn read(&self, position: ReadPosition, limit: ReadLimit) -> BoxFuture<Vec<Message>, Error> {
        self.send_request(|v| LogRequest::Read(position, limit, v))
    }

    pub fn flush(&self) -> BoxFuture<(), Error> {
        self.send_request(|v| LogRequest::Flush(v))
    }

    fn send_request<F, R, E>(&self, f: F) -> BoxFuture<R, Error>
        where F: FnOnce(oneshot::Sender<Result<R, E>>) -> LogRequest,
              R: 'static + Send,
              E: 'static + Send

    {
        let (snd, recv) = oneshot::channel::<Result<R, E>>();
        self.sender.clone()
            .send(f(snd))
            .map_err(|_| Error::new(ErrorKind::Other, "Log closed"))
            .and_then(|_| recv.map_err(|_| Error::new(ErrorKind::Other, "Recv err")))
            .and_then(|res| {
                result(res.map_err(|_| Error::new(ErrorKind::Other, "Log Error")))
            })
            .boxed()
    }
}
