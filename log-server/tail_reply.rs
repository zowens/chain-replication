use std::collections::HashMap;
use std::collections::hash_map;
use std::io;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use futures::sync::mpsc;
use tokio_proto::streaming::Body;
use tokio_core::reactor::Handle;
use asynclog::{AppendListener, ClientAppendSet};
use proto::ResChunk;

type ReplySender = mpsc::Sender<Result<ResChunk, io::Error>>;
type ReplyStream = Body<ResChunk, io::Error>;

pub struct TailReplyListener {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl AppendListener for TailReplyListener {
    fn notify_append(&mut self, append: ClientAppendSet) {
        self.sender
            .unbounded_send(TailReplyMsg::Notify(append))
            .unwrap();
    }
}

#[derive(Clone)]
pub struct TailReplyRegistrar {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl TailReplyRegistrar {
    pub fn listen(&self, client_id: u32) -> ReplyStream {
        let (snd, recv) = Body::pair();
        self.sender
            .unbounded_send(TailReplyMsg::Register(client_id, snd))
            .unwrap();
        recv
    }
}


enum TailReplyMsg {
    Register(u32, ReplySender),
    Notify(ClientAppendSet),
}

pub struct TailReplyManager {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl TailReplyManager {
    pub fn new(handle: Handle) -> TailReplyManager {
        let (snd, recv) = mpsc::unbounded();
        handle.spawn(TailReplySender {
            recv,
            registered: HashMap::new(),
        });

        TailReplyManager { sender: snd }
    }

    pub fn listener(&self) -> TailReplyListener {
        TailReplyListener {
            sender: self.sender.clone(),
        }
    }

    pub fn registrar(&self) -> TailReplyRegistrar {
        TailReplyRegistrar {
            sender: self.sender.clone(),
        }
    }
}

struct TailReplySender {
    recv: mpsc::UnboundedReceiver<TailReplyMsg>,
    registered: HashMap<u32, ReplySender>,
}

impl Future for TailReplySender {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            match try_ready!(self.recv.poll()) {
                Some(TailReplyMsg::Register(client_id, msg)) => {
                    trace!("Registered client {}", client_id);
                    self.registered.insert(client_id, msg);
                }
                Some(
                    TailReplyMsg::Notify(ClientAppendSet {
                        client_id,
                        client_req_ids,
                        latest_offset,
                    }),
                ) => if let hash_map::Entry::Occupied(mut entry) = self.registered.entry(client_id)
                {
                    let send_res = entry.get_mut().start_send(Ok(ResChunk::AppendedMessages {
                        offset: latest_offset,
                        client_reqs: client_req_ids,
                    }));
                    match send_res {
                        Ok(AsyncSink::Ready) => {
                            trace!("Tail reply sent to client {}", client_id);
                        }
                        Ok(AsyncSink::NotReady(_)) => {
                            warn!("Client not ready :/");
                        }
                        Err(_) => {
                            trace!("Tail dropped");
                            entry.remove();
                        }
                    }
                },
                None => {
                    warn!("Tail reply stream completed");
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
