use asynclog::{AppendListener, ClientAppendSet};
use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use std::collections::{hash_map, HashMap};
use tokio::spawn;

// TODO: bound sending
type ReplySender = mpsc::UnboundedSender<ClientAppendSet>;

/// Stream of replies for a single client
pub struct ReplyStream(mpsc::UnboundedReceiver<ClientAppendSet>);

impl Stream for ReplyStream {
    type Item = ClientAppendSet;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<ClientAppendSet>, ()> {
        self.0.poll()
    }
}

/// Listener for `AsyncLog` appends
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

/// Registrar for client for notifications
#[derive(Clone)]
pub struct TailReplyRegistrar {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl TailReplyRegistrar {
    /// Listens for for client changes
    pub fn listen(&self, client_id: u32) -> ReplyStream {
        let (snd, recv) = mpsc::unbounded();
        self.sender
            .unbounded_send(TailReplyMsg::Register(client_id, snd))
            .unwrap();
        ReplyStream(recv)
    }
}

enum TailReplyMsg {
    Register(u32, ReplySender),
    Notify(ClientAppendSet),
}

/// Opens a listener and tail reply pair
pub fn new() -> (TailReplyListener, TailReplyRegistrar) {
    let (sender, receiver) = mpsc::unbounded();

    spawn(TailReplySender {
        receiver,
        registered: HashMap::new(),
    });

    let listener = TailReplyListener {
        sender: sender.clone(),
    };
    let registrar = TailReplyRegistrar { sender };
    (listener, registrar)
}

struct TailReplySender {
    receiver: mpsc::UnboundedReceiver<TailReplyMsg>,
    registered: HashMap<u32, ReplySender>,
}

impl Future for TailReplySender {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            match try_ready!(self.receiver.poll()) {
                Some(TailReplyMsg::Register(client_id, msg)) => {
                    trace!("Registered client {}", client_id);
                    self.registered.insert(client_id, msg);
                }
                Some(TailReplyMsg::Notify(append_set)) => {
                    let client_id = append_set.client_id;
                    if let hash_map::Entry::Occupied(mut entry) = self.registered.entry(client_id) {
                        let send_res = entry.get_mut().start_send(append_set);
                        match send_res {
                            Ok(AsyncSink::Ready) => {
                                trace!("Tail reply sent to client {}", client_id);
                            }
                            Ok(AsyncSink::NotReady(_)) => {
                                // TODO: what should we do here...?
                                warn!("Client not ready, dropping notification");
                            }
                            Err(_) => {
                                trace!("Tail dropped");
                                entry.remove();
                            }
                        }
                    }
                }
                None => {
                    warn!("Tail reply stream completed");
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
