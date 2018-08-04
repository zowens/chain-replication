use super::{
    bufpool::BytesPool,
    messages::{MessagesMut, SingleMessage},
};
use commitlog::message::MessageSet;
use futures::{Async, Poll, Stream};
use std::cell::RefCell;
use std::rc::Rc;

pub struct BatchMessageStream<S> {
    stream: S,
    buf_pool: Rc<RefCell<BytesPool>>,
    blocked_message: Option<SingleMessage>,
}

impl<S> BatchMessageStream<S>
where
    S: Stream<Item = SingleMessage>,
{
    pub fn new(stream: S, buf_pool: Rc<RefCell<BytesPool>>) -> BatchMessageStream<S> {
        BatchMessageStream {
            stream,
            buf_pool,
            blocked_message: None,
        }
    }
}

impl<S> Stream for BatchMessageStream<S>
where
    S: Stream<Item = SingleMessage>,
{
    type Item = MessagesMut;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // utilize a buffered message or pull one from the stream
        let (client, req, payload) = match self.blocked_message.take() {
            Some(m) => m,
            None => try_ready!(self.stream.poll()).unwrap(),
        };

        // initialize a buffer from the byte pool
        let (mut buf, capacity) = {
            let mut pool = self.buf_pool.borrow_mut();
            (MessagesMut(pool.take()), pool.buffer_capacity())
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
        loop {
            match self.stream.poll()? {
                Async::Ready(Some((client, req, payload))) => {
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

                        assert!(self.blocked_message.is_none());
                        self.blocked_message = Some((client, req, payload));
                        break;
                    }
                }
                Async::Ready(None) => return Ok(Async::Ready(None)),
                Async::NotReady => {
                    debug!("Done batching messages");
                    break;
                }
            }
        }

        if rare!(buf.bytes().len() == 0) {
            debug!("No messages in the buffer, returning to the pool");
            self.buf_pool.borrow_mut().push(buf.freeze().0);
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(Some(buf)))
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
