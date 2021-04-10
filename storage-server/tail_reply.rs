use crate::asynclog::AppendListener;
use crate::asynclog::Messages;
use byteorder::{ByteOrder, LittleEndian};
use commitlog::message::MessageSet;
use fnv::FnvHashMap;
use futures::{ready, Future, Stream};
use std::collections::hash_map;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::sync::mpsc;

// TODO: bound sending
type ReplySender = mpsc::UnboundedSender<Vec<u64>>;

/// Stream of replies for a single client
pub struct ReplyStream(mpsc::UnboundedReceiver<Vec<u64>>);

impl Stream for ReplyStream {
    type Item = Vec<u64>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Vec<u64>>> {
        self.0.poll_recv(cx)
    }
}

/// Listener for `AsyncLog` appends
pub struct TailReplyListener {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl AppendListener for TailReplyListener {
    fn notify_append(&mut self, append: &Messages) {
        self.sender
            .send(TailReplyMsg::Notify(append.clone()))
            .unwrap_or_default();
    }
}

/// Registrar for client for notifications
#[derive(Clone)]
pub struct TailReplyRegistrar {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl TailReplyRegistrar {
    /// Listens for for client changes
    pub fn listen(&self, client_id: u64) -> ReplyStream {
        let (snd, recv) = mpsc::unbounded_channel();
        self.sender
            .send(TailReplyMsg::Register(client_id, snd))
            .unwrap_or_default();
        ReplyStream(recv)
    }
}

enum TailReplyMsg {
    Register(u64, ReplySender),
    Notify(Messages),
}

/// Opens a listener and tail reply pair
pub fn new() -> (TailReplyListener, TailReplyRegistrar) {
    let (sender, receiver) = mpsc::unbounded_channel();

    spawn(TailReplySender {
        receiver,
        registered: FnvHashMap::default(),
    });

    let listener = TailReplyListener {
        sender: sender.clone(),
    };
    let registrar = TailReplyRegistrar { sender };
    (listener, registrar)
}

struct TailReplySender {
    receiver: mpsc::UnboundedReceiver<TailReplyMsg>,
    registered: FnvHashMap<u64, ReplySender>,
}

impl TailReplySender {
    fn notify_clients(&mut self, append_set: Messages) {
        let mut req_batches: FnvHashMap<u64, Vec<u64>> = FnvHashMap::default();

        // batch by client_id
        for msg in append_set.iter() {
            let bytes = msg.metadata();
            if bytes.len() != 16 {
                warn!("Invalid log entry appended");
                continue;
            }

            let client_id = LittleEndian::read_u64(&bytes[0..8]);
            if self.registered.contains_key(&client_id) {
                let client_req_id = LittleEndian::read_u64(&bytes[8..16]);
                let reqs = req_batches.entry(client_id).or_insert_with(Vec::new);
                reqs.push(client_req_id);
            }
        }

        // notify the clients
        for (client_id, client_req_ids) in req_batches {
            if let hash_map::Entry::Occupied(mut entry) = self.registered.entry(client_id) {
                let send_res = entry.get_mut().send(client_req_ids);
                match send_res {
                    Ok(_) => {
                        trace!("Tail reply sent to client {}", client_id);
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
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match ready!(self.receiver.poll_recv(cx)) {
                Some(TailReplyMsg::Register(client_id, listener)) => {
                    trace!("Registered client {}", client_id);
                    self.registered.insert(client_id, listener);
                }
                Some(TailReplyMsg::Notify(append_set)) => {
                    self.notify_clients(append_set);
                }
                None => {
                    warn!("Tail reply stream completed");
                    return Poll::Ready(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asynclog::{Messages, MessagesMut};
    use bytes::BytesMut;
    use futures::executor::{spawn, Notify, NotifyHandle, Spawn};
    use test::Bencher;

    #[test]
    fn register_clients() {
        let (reg, _, sender) = fake_registrar();
        let mut stream = spawn(sender);

        reg.listen(0);
        reg.listen(1);

        assert_eq!(0, stream.get_ref().registered.len());

        // pool the stream to register
        let handle = notify_noop();
        assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());

        assert_eq!(2, stream.get_ref().registered.len());
    }

    #[test]
    fn notify_clients() {
        let handle = notify_noop();

        let (reg, mut listener, sender) = fake_registrar();
        let mut stream = spawn(sender);

        let mut client_1 = spawn(reg.listen(0));
        let mut client_2 = spawn(reg.listen(1));

        // pool the stream to register
        assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());
        assert_eq!(2, stream.get_ref().registered.len());

        // notify
        let m = msgs(vec![(0, 10), (1, 100), (1, 200), (0, 20), (0, 30)]);
        listener.notify_append(m);

        // pool the stream to notify
        assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());

        assert_eq!(vec![vec![10, 20, 30]], poll_client_ids(&mut client_1));
        assert_eq!(vec![vec![100, 200]], poll_client_ids(&mut client_2));
    }

    #[test]
    fn notify_unknown_client() {
        let handle = notify_noop();

        let (reg, mut listener, sender) = fake_registrar();
        let mut stream = spawn(sender);

        let mut client_1 = spawn(reg.listen(0));
        let mut client_2 = spawn(reg.listen(1));

        // pool the stream to register
        assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());
        assert_eq!(2, stream.get_ref().registered.len());

        // notify
        let m = msgs(vec![(3, 10)]);
        listener.notify_append(m);

        // pool the stream to notify
        assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());
        assert!(poll_client_ids(&mut client_1).is_empty());
        assert!(poll_client_ids(&mut client_2).is_empty());
    }

    #[bench]
    fn bench_notify_clients(b: &mut Bencher) {
        let mbuf = msgs(vec![(0, 10), (1, 100), (1, 200), (0, 20), (0, 30)]);
        b.iter(move || {
            let (reg, mut listener, sender) = fake_registrar();
            let handle = notify_noop();
            let mut stream = spawn(sender);
            let mut client_1 = spawn(reg.listen(0));
            let mut client_2 = spawn(reg.listen(1));

            // pool the stream to register
            assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());

            // notify
            listener.notify_append(mbuf.clone());

            // pool the stream to notify
            assert!(!stream.poll_future_notify(&handle, 120).unwrap().is_ready());

            poll_client_ids(&mut client_1);
            poll_client_ids(&mut client_2);
        });
    }

    fn poll_client_ids(s: &mut Spawn<ReplyStream>) -> Vec<Vec<u64>> {
        let mut req_id_batches = Vec::new();
        let handle = notify_noop();
        while let Poll::Ready(Some(v)) = s.poll_stream_notify(&handle, 0).unwrap() {
            req_id_batches.push(v);
        }
        req_id_batches
    }

    fn fake_registrar() -> (TailReplyRegistrar, TailReplyListener, TailReplySender) {
        let (sender, receiver) = mpsc::unbounded_channel();

        let reply_sndr = TailReplySender {
            receiver,
            registered: FnvHashMap::default(),
        };

        let listener = TailReplyListener {
            sender: sender.clone(),
        };

        let registrar = TailReplyRegistrar { sender };
        (registrar, listener, reply_sndr)
    }

    fn msgs(msgs: Vec<(u64, u64)>) -> Messages {
        let mut buf = MessagesMut(BytesMut::with_capacity(1024));
        for (client_id, req_id) in &msgs {
            buf.push(*client_id, *req_id, b"123").unwrap();
        }
        buf.freeze()
    }

    fn notify_noop() -> NotifyHandle {
        struct Noop;

        impl Notify for Noop {
            fn notify(&self, _id: usize) {}
        }

        const NOOP: &'static Noop = &Noop;

        NotifyHandle::from(NOOP)
    }
}
