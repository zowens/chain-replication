use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use http::Response;
use rand::{OsRng, RngCore};
use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::sync::{
    atomic::{AtomicUsize, Ordering}, Arc,
};
use tokio::spawn;
use tower_grpc::client::server_streaming::ResponseFuture;
use tower_grpc::codegen::client::grpc;
use tower_h2::{Body, Data, HttpService};

use proto::{client::LogStorage, Reply, ReplyRequest};

pub type Receiver = oneshot::Receiver<()>;
pub type Sender = oneshot::Sender<()>;

enum PoolRequest {
    Add { request_id: u64, sender: Sender },
    Complete { request_ids: Vec<u64> },
}

struct WaitingPool {
    // TODO: benchmark with BTreeMap
    //
    // Intuition is that most requests are appended
    // sequentially
    reqs: Cell<HashMap<u64, Sender>>,
}

impl WaitingPool {
    fn new() -> WaitingPool {
        WaitingPool {
            reqs: Cell::new(HashMap::new()),
        }
    }
}

impl Sink for WaitingPool {
    type SinkItem = PoolRequest;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match item {
            PoolRequest::Add { request_id, sender } => {
                self.reqs.get_mut().insert(request_id, sender);
            }
            PoolRequest::Complete { request_ids } => {
                let mut map = self.reqs.get_mut();
                for req_id in request_ids {
                    if let Some(snd) = map.remove(&req_id) {
                        snd.send(()).unwrap_or_else(|_| ());
                    }
                }
            }
        }
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

#[derive(Clone)]
pub struct RequestManager {
    req_sender: mpsc::UnboundedSender<(u64, Sender)>,
    next_req_id: Arc<AtomicUsize>,
    client_id: u64,
}

impl RequestManager {
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn push_req(&mut self) -> (u64, Receiver) {
        // TODO: convert req ID to u64
        let req_id = self.next_req_id.fetch_add(1, Ordering::SeqCst) as u64;

        let (snd, recv) = oneshot::channel();

        // TODO: what do we do on error here?
        self.req_sender.unbounded_send((req_id, snd)).unwrap();

        (req_id, recv)
    }

    pub fn start<T: HttpService>(client: &mut LogStorage<T>) -> ReplyStartFuture<T>
    where
        grpc::unary::Once<ReplyRequest>: grpc::Encodable<T::RequestBody>,
        T::Future: Future<Item = Response<T::ResponseBody>>,
        T::ResponseBody: Body<Data = Data>,
    {
        // TODO: this + configuration should come from master/configurator process
        let client_id = OsRng::new().unwrap().next_u64();

        let future = client.replies(grpc::Request::new(ReplyRequest { client_id }));
        ReplyStartFuture { future, client_id }
    }
}

/// Future that waits for replies to be started for this client
pub struct ReplyStartFuture<T: HttpService> {
    future: ResponseFuture<Reply, T::Future>,
    client_id: u64,
}

impl<T: HttpService> Future for ReplyStartFuture<T>
where
    T::Future: Future<Item = Response<T::ResponseBody>>,
    T::ResponseBody: Body<Data = Data> + Send + 'static,
{
    type Item = RequestManager;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let response = try_ready!(
            self.future
                .poll()
                .map_err(|_e| io::Error::new(io::ErrorKind::Other, "Unable to open reply stream"))
        );

        let (snd, recv) = mpsc::unbounded::<(u64, Sender)>();

        // TODO: reconnect, reconfiguration, failures, etc.
        {
            let completions = response
                .into_inner()
                .map(|req| PoolRequest::Complete {
                    request_ids: req.client_request_ids,
                })
                .map_err(|_| ());
            let adds =
                recv.map(|add| PoolRequest::Add {
                    request_id: add.0,
                    sender: add.1,
                }).map_err(|_| ());

            spawn(
                completions
                    .select(adds)
                    .forward(WaitingPool::new())
                    .map(|_| ()),
            );
        }

        Ok(Async::Ready(RequestManager {
            req_sender: snd,
            next_req_id: Arc::new(AtomicUsize::new(0)),
            client_id: self.client_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::{spawn, Notify, NotifyHandle};
    use test::Bencher;

    #[test]
    fn waitingpool_remove_does_not_crash() {
        let mut waiting_pool = WaitingPool::new();
        waiting_pool
            .start_send(PoolRequest::Complete {
                request_ids: vec![0u64],
            })
            .unwrap();
    }

    #[test]
    fn waitingpool_insert_then_remove() {
        let mut waiting_pool = WaitingPool::new();
        let (snd, recv) = oneshot::channel();

        let mut recv = spawn(recv);

        waiting_pool
            .start_send(PoolRequest::Add {
                request_id: 0,
                sender: snd,
            })
            .unwrap();

        // ensure waiting
        assert_eq!(
            Ok(Async::NotReady),
            recv.poll_future_notify(&notify_noop(), 1)
        );

        waiting_pool
            .start_send(PoolRequest::Complete {
                request_ids: vec![0u64],
            })
            .unwrap();

        // ensure triggered
        assert_eq!(
            Ok(Async::Ready(())),
            recv.poll_future_notify(&notify_noop(), 1)
        );
    }

    fn notify_noop() -> NotifyHandle {
        struct Noop;
        impl Notify for Noop {
            fn notify(&self, _id: usize) {}
        }

        const NOOP: &'static Noop = &Noop;

        NotifyHandle::from(NOOP)
    }

    #[bench]
    fn waitingpool_insert_remove_one(b: &mut Bencher) {
        b.iter(|| {
            let mut waiting_pool = WaitingPool::new();

            for i in 0u64..100u64 {
                let (snd, recv) = oneshot::channel();
                let mut recv = spawn(recv);

                waiting_pool
                    .start_send(PoolRequest::Add {
                        request_id: i,
                        sender: snd,
                    })
                    .unwrap();
                assert_eq!(
                    Ok(Async::NotReady),
                    recv.poll_future_notify(&notify_noop(), 1)
                );
                waiting_pool
                    .start_send(PoolRequest::Complete {
                        request_ids: vec![i],
                    })
                    .unwrap();
                assert_eq!(
                    Ok(Async::Ready(())),
                    recv.poll_future_notify(&notify_noop(), 1)
                );
            }
        });
    }

    #[bench]
    fn waitingpool_insert_remove_batch(b: &mut Bencher) {
        b.iter(|| {
            let mut waiting_pool = WaitingPool::new();

            let mut recvs = Vec::with_capacity(100);
            for i in 0u64..100u64 {
                let (snd, recv) = oneshot::channel();
                recvs.push(spawn(recv));
                waiting_pool
                    .start_send(PoolRequest::Add {
                        request_id: i,
                        sender: snd,
                    })
                    .unwrap();
            }

            waiting_pool
                .start_send(PoolRequest::Complete {
                    request_ids: (0..100u64).collect(),
                })
                .unwrap();

            for mut recv in recvs {
                assert_eq!(
                    Ok(Async::Ready(())),
                    recv.poll_future_notify(&notify_noop(), 1)
                );
            }
        });
    }
}
