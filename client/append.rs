use fnv::FnvHashMap;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use http::Response;
use rand::{OsRng, RngCore};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use tokio::executor::current_thread::spawn;
use tower_grpc::client::server_streaming::ResponseFuture;
use tower_grpc::codegen::client::grpc;
use tower_h2::{Body, Data, HttpService};

use proto::{client::LogStorage, Reply, ReplyRequest};

const START_REQUEST_SIZE: usize = 64;

pub type Receiver = oneshot::Receiver<()>;
pub type Sender = oneshot::Sender<()>;

struct RequestMapState(FnvHashMap<u64, Sender>, u64);

impl Default for RequestMapState {
    fn default() -> RequestMapState {
        RequestMapState(
            FnvHashMap::with_capacity_and_hasher(START_REQUEST_SIZE, Default::default()),
            0,
        )
    }
}

type RequestMap = Rc<RefCell<RequestMapState>>;

struct Completor(RequestMap);

impl Sink for Completor {
    type SinkItem = Vec<u64>;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let mut p = self.0.borrow_mut();
        for req_id in item {
            if let Some(v) = p.0.remove(&req_id) {
                v.send(()).unwrap_or(());
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
    requests: RequestMap,
    client_id: u64,
}

impl RequestManager {
    #[inline]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn push_req(&mut self) -> (u64, Receiver) {
        let mut p = self.requests.borrow_mut();
        let req_id = p.1;
        p.1 += 1;

        let (snd, recv) = oneshot::channel();
        p.0.insert(req_id, snd);
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

        let map = Rc::new(RefCell::new(RequestMapState::default()));

        // TODO: reconnect, reconfiguration, failures, etc.
        spawn(
            response
                .into_inner()
                .map(|reply| reply.client_request_ids)
                .map_err(|_| ())
                .forward(Completor(map.clone()))
                .map(|_| ()),
        );

        Ok(Async::Ready(RequestManager {
            requests: map,
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
        let map = Rc::new(RefCell::new(RequestMapState::default()));
        let mut waiting_pool = Completor(map);
        waiting_pool.start_send(vec![0u64]).unwrap();
    }

    #[test]
    fn waitingpool_insert_then_remove() {
        let map = Rc::new(RefCell::new(RequestMapState::default()));

        let mut waiting_pool = Completor(map.clone());
        let mut mgr = RequestManager {
            requests: map,
            client_id: 0,
        };

        let (req_id, recv) = mgr.push_req();
        let mut recv = spawn(recv);

        // ensure waiting
        assert_eq!(
            Ok(Async::NotReady),
            recv.poll_future_notify(&notify_noop(), 1)
        );

        waiting_pool.start_send(vec![req_id]).unwrap();

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
            let map = Rc::new(RefCell::new(RequestMapState::default()));
            let mut waiting_pool = Completor(map.clone());
            let mut mgr = RequestManager {
                requests: map,
                client_id: 0,
            };

            for _ in 0u64..100u64 {
                let (req_id, recv) = mgr.push_req();
                let mut recv = spawn(recv);

                assert_eq!(
                    Ok(Async::NotReady),
                    recv.poll_future_notify(&notify_noop(), 1)
                );
                waiting_pool.start_send(vec![req_id]).unwrap();
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
            let map = Rc::new(RefCell::new(RequestMapState::default()));
            let mut waiting_pool = Completor(map.clone());
            let mut mgr = RequestManager {
                requests: map,
                client_id: 0,
            };

            let mut recvs = Vec::with_capacity(100);
            let mut request_ids = Vec::with_capacity(100);
            for _ in 0u64..100u64 {
                let (req_id, recv) = mgr.push_req();
                recvs.push(spawn(recv));
                request_ids.push(req_id);
            }

            waiting_pool.start_send(request_ids).unwrap();

            for mut recv in recvs {
                assert_eq!(
                    Ok(Async::Ready(())),
                    recv.poll_future_notify(&notify_noop(), 1)
                );
            }
        });
    }
}
