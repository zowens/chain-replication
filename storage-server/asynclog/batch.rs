use super::{
    bufpool::BytesPool,
    messages::{MessagesMut, SingleMessage},
};
use commitlog::message::MessageSet;
use futures::{ready, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

pub struct BatchMessageStream {
    receiver: mpsc::UnboundedReceiver<SingleMessage>,
    buf_pool: BytesPool,
    blocked_message: Option<SingleMessage>,
}

impl BatchMessageStream
{
    pub fn new(
        receiver: mpsc::UnboundedReceiver<SingleMessage>,
        buf_pool: BytesPool) -> BatchMessageStream {
        BatchMessageStream {
            receiver,
            buf_pool,
            blocked_message: None,
        }
    }
}

impl Stream for BatchMessageStream
{
    type Item = MessagesMut;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // utilize a buffered message or pull one from the stream
        let (client, req, payload) = if let Some(m) = this.blocked_message.take() {
            m
        } else if let Some(m) = ready!(this.receiver.poll_recv(cx)) {
            m
        } else {
            return Poll::Ready(None);
        };

        // initialize a buffer from the byte pool
        let (mut buf, capacity) = {
            let buf = this.buf_pool.take();
            (MessagesMut(buf), this.buf_pool.buffer_capacity())
        };

        // try to push the first message
        if rare!(buf.push(client, req, &payload).is_err()) {
            warn!(
                "Ignoring message clientId={}, reqId={} due to size {} > buffer capacity {}",
                client,
                req,
                payload.len(),
                capacity
            );
        }

        // add more messages to the buffer, up to the capacity
        while let Poll::Ready(Some((client, req, payload))) = this.receiver.poll_recv(cx) {
            if rare!(payload.len() > capacity) {
                warn!("Ignoring message clientId={}, reqId={} due to size {} > buffer capacity {}", client, req, payload.len(), capacity);
                continue;
            }

            // try to push the message, if there is no capacity
            // save the message for another round of poll
            if rare!(buf.push(client, req, &payload).is_err()) {
                debug!(
                    "Buffer is full, corking message clientId={}, reqId={}",
                    client, req
                );

                assert!(this.blocked_message.is_none());
                self.blocked_message.replace((client, req, payload));
                break;
            }
        }
        debug!("Done batching messages");

        if rare!(buf.bytes().is_empty()) {
            debug!("No messages in the buffer, returning to the pool");
            self.buf_pool.push(buf.0);
            Poll::Pending
        } else {
            Poll::Ready(Some(buf))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;

    macro_rules! unwrap_async {
        ($e:expr) => {
            match $e {
                Ok(Async::Ready(Some(v))) => v,
                Ok(Async::Ready(None)) => panic!("Unexpect stream end"),
                Ok(Async::NotReady) => panic!("Unexpect NotReady"),
                Err(_) => panic!("Unexpected error"),
            }
        };
    }

    #[test]
    fn not_ready_when_empty() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));
        let mut batch_stream = BatchMessageStream::new(FakeStream(VecDeque::new()), pool);
        assert!(batch_stream.poll().unwrap().is_not_ready());
    }

    #[test]
    fn ready_with_single_message() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));
        let msgs = vec![Bytes::from("1234")];
        let mut batch_stream = BatchMessageStream::new(FakeStream(msgs.into()), pool);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(1, v.len());
    }

    #[test]
    fn ready_with_batched_messages() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));
        let msgs = vec![Bytes::from("1234"), Bytes::from("456"), Bytes::from("789")];
        let mut batch_stream = BatchMessageStream::new(FakeStream(msgs.into()), pool);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(3, v.len());
    }

    #[test]
    fn ignores_large_first_message() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));

        let large_message = (0..2048).map(|_| 0xeeu8).collect::<Vec<u8>>();
        let msgs = vec![
            Bytes::from(large_message),
            Bytes::from("456"),
            Bytes::from("789"),
        ];
        let mut batch_stream = BatchMessageStream::new(FakeStream(msgs.into()), pool);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(2, v.len());
    }

    #[test]
    fn ignores_large_middle_message() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));

        let large_message = (0..2048).map(|_| 0xeeu8).collect::<Vec<u8>>();
        let msgs = vec![
            Bytes::from("456"),
            Bytes::from(large_message),
            Bytes::from("789"),
        ];
        let mut batch_stream = BatchMessageStream::new(FakeStream(msgs.into()), pool);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(2, v.len());
    }

    #[test]
    fn batches_up_to_capacity() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));

        let msg: Bytes = (0..472).map(|_| 0xeeu8).collect::<Vec<u8>>().into();
        let msgs = vec![msg.clone(), msg.clone(), msg.clone()];
        let mut batch_stream = BatchMessageStream::new(FakeStream(msgs.into()), pool);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(2, v.len());

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(1, v.len());
    }

    #[test]
    fn batches_up_to_capacity_with_additional_messages() {
        let pool = Rc::new(RefCell::new(BytesPool::new(1024)));

        let msg: Bytes = (0..472).map(|_| 0xeeu8).collect::<Vec<u8>>().into();
        let msgs = vec![msg.clone(), msg.clone(), msg.clone()];
        let mut batch_stream = BatchMessageStream::new(FakeStream(msgs.into()), pool);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(2, v.len());

        batch_stream.stream.0.push_front(msg);

        let v = unwrap_async!(batch_stream.poll());
        assert_eq!(2, v.len());
    }

    struct FakeStream(VecDeque<Bytes>);

    impl Stream for FakeStream {
        type Item = SingleMessage;
        type Error = ();

        fn poll(&mut self) -> Poll<Option<SingleMessage>, ()> {
            match self.0.pop_back() {
                Some(v) => Ok(Async::Ready(Some((5, 5, v)))),
                None => Ok(Async::NotReady),
            }
        }
    }

}
