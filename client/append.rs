use crate::protocol::{LogStorageClient, ReplyRequest};
use fnv::FnvHashMap;
use futures::channel::oneshot;
use futures::StreamExt;
use futures::TryStreamExt;
use rand::random;
use std::io;
use std::sync::Arc;
use tokio::{spawn, sync::Mutex};

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

#[derive(Clone, Default)]
struct RequestMap(Arc<Mutex<RequestMapState>>);
impl RequestMap {
    async fn send(&mut self, ids: Vec<u64>) {
        let mut state = self.0.lock().await;
        for req_id in ids {
            if let Some(v) = state.0.remove(&req_id) {
                v.send(()).unwrap_or(());
            }
        }
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

    pub async fn push_req(&mut self) -> (u64, Receiver) {
        let mut p = self.requests.0.lock().await;
        let req_id = p.1;
        p.1 += 1;

        let (snd, recv) = oneshot::channel();
        p.0.insert(req_id, snd);
        (req_id, recv)
    }

    pub fn start(client: &LogStorageClient) -> io::Result<RequestManager> {
        // TODO: this + configuration should come from master/configurator process
        let client_id = random::<u64>();

        let map = RequestMap::default();

        let mut reply_req = ReplyRequest::new();
        reply_req.set_client_id(client_id);
        let reply_stream = client.replies(&reply_req).map_err(|e| {
            error!("ERROR with reply stream: {}", e);
            io::Error::new(io::ErrorKind::Other, "Error opening stream")
        })?;

        let map_clone = map.clone();
        spawn(
            reply_stream
                .map_ok(|reply| reply.client_request_ids)
                .map(|replies| replies.unwrap_or_default())
                .for_each(move |requests| {
                    let mut map_clone = map.clone();
                    async move {
                        map_clone.send(requests).await;
                    }
                }),
        );

        Ok(RequestManager {
            requests: map_clone,
            client_id,
        })
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
