use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;

use rand::{OsRng, Rng};
use futures::{Async, Future, Poll, Stream};
use futures::unsync::oneshot;
use tokio_core::reactor::Handle;
use tokio_service::Service;
use tokio_proto::TcpClient;
use tokio_proto::streaming::Message;

use proto::*;

pub type Receiver = oneshot::Receiver<()>;
pub type Sender = oneshot::Sender<()>;

struct WaitingPool {
    next_req_id: u32,

    // TODO: benchmark with BTreeMap
    //
    // Intuition is that most requests are appended
    // sequentially
    reqs: HashMap<u32, Sender>,
}

impl WaitingPool {
    fn new() -> WaitingPool {
        WaitingPool {
            next_req_id: 0,
            reqs: HashMap::new(),
        }
    }

    fn push_req(&mut self) -> (u32, Receiver) {
        let req = self.next_req_id;
        self.next_req_id += 1;

        let (snd, recv) = oneshot::channel();
        self.reqs.insert(req, snd);
        (req, recv)
    }

    fn set_replies(&mut self, client_reqs: Vec<u32>) {
        for req_id in client_reqs {
            if let Some(snd) = self.reqs.remove(&req_id) {
                snd.send(()).unwrap_or_else(|_| ());
            }
        }
    }
}

#[derive(Clone)]
pub struct ReplyManager {
    pool: Rc<RefCell<WaitingPool>>,
    client_id: u32,
}

impl ReplyManager {
    pub fn new(handle: &Handle, tail: &SocketAddr) -> ReplyManager {
        let pool = Rc::new(RefCell::new(WaitingPool::new()));

        // TODO: this + configuration should come from master/configurator process
        let client_id = OsRng::new().unwrap().next_u32();

        {
            let p = pool.clone();
            let cli = TcpClient::new(LogServerProto);
            let f = cli.connect(tail, handle)
                .and_then(move |client| {
                    info!("Reply connection connected");
                    client
                    .call(Message::WithoutBody(Request::RequestTailReply {
                        client_id,
                        last_known_offset: 0,
                    }))
                    .and_then(move |msg| match msg {
                        Message::WithBody(Response::TailReplyStarted, body) => {
                            info!("Starting reply generation");
                            ReplyCompletionFuture {
                                _conn: client,
                                reply_stream: body,
                                pool: p,
                            }
                        }
                        _ => unreachable!()
                    })
                })
                // TODO: log this at least
                .map_err(|_| ());
            handle.spawn(f);
        }
        ReplyManager { pool, client_id }
    }

    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    pub fn push_req(&mut self) -> (u32, Receiver) {
        self.pool.borrow_mut().push_req()
    }
}

struct ReplyCompletionFuture {
    _conn: ProtoConnection,
    reply_stream: ReplyStream,
    pool: Rc<RefCell<WaitingPool>>,
}

impl Future for ReplyCompletionFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        loop {
            match try_ready!(self.reply_stream.poll()) {
                Some(ReplyResponse::AppendedMessages { client_reqs, .. }) => {
                    self.pool.borrow_mut().set_replies(client_reqs);
                }
                None => {
                    // TODO: re-connect instead of stopping
                    info!("Reply stream completed");
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
