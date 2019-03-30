use futures::{Async, Future, Poll};
use std::io::{Error, ErrorKind};
use tokio_sync::oneshot;

pub struct LogSender<T> {
    s: oneshot::Sender<Result<T, Error>>,
}

impl<T> LogSender<T> {
    #[inline]
    pub fn send(self, res: T) {
        self.s.send(Ok(res)).unwrap_or_default();
    }

    #[inline]
    pub fn send_err(self, e: Error) {
        self.s.send(Err(e)).unwrap_or_default();
    }

    #[inline]
    pub fn send_err_with(self, k: ErrorKind, e: &'static str) {
        self.s.send(Err(Error::new(k, e))).unwrap_or_default();
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
                error!("Encountered cancellation: {:?}", e);
                Err(Error::new(ErrorKind::Other, "Cancelled"))
            }
        }
    }
}

pub fn channel<T>() -> (LogSender<T>, LogFuture<T>) {
    let (s, f) = oneshot::channel::<Result<T, Error>>();
    (LogSender { s }, LogFuture { f })
}
