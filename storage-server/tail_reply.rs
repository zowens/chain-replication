use asynclog::AppendListener;
use byteorder::{ByteOrder, LittleEndian};
use commitlog::message::MessageSet;
use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use messages::Messages;
use std::collections::{hash_map, HashMap};
use tokio::spawn;

// TODO: bound sending
type ReplySender = mpsc::UnboundedSender<Vec<u32>>;

/// Stream of replies for a single client
pub struct ReplyStream(mpsc::UnboundedReceiver<Vec<u32>>);

impl Stream for ReplyStream {
    type Item = Vec<u32>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Vec<u32>>, ()> {
        self.0.poll()
    }
}

/// Listener for `AsyncLog` appends
pub struct TailReplyListener {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl AppendListener for TailReplyListener {
    fn notify_append(&mut self, append: Messages) {
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
    Notify(Messages),
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

impl TailReplySender {
    fn notify_clients(&mut self, append_set: Messages) {
        let mut req_batches: HashMap<u32, Vec<u32>> = HashMap::new();

        // batch by client_id
        for msg in append_set.iter() {
            let bytes = msg.metadata();
            if bytes.len() != 8 {
                warn!("Invalid log entry appended");
                continue;
            }

            let client_id = LittleEndian::read_u32(&bytes[0..4]);
            if self.registered.contains_key(&client_id) {
                let client_req_id = LittleEndian::read_u32(&bytes[4..8]);
                let reqs = req_batches.entry(client_id).or_insert_with(Vec::new);
                reqs.push(client_req_id);
            }
        }

        // notify the clients
        for (client_id, client_req_ids) in req_batches {
            if let hash_map::Entry::Occupied(mut entry) = self.registered.entry(client_id) {
                let send_res = entry.get_mut().start_send(client_req_ids);
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
    }
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
                    self.notify_clients(append_set);
                }
                None => {
                    warn!("Tail reply stream completed");
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
