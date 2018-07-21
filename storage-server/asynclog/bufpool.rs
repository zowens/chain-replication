use bytes::{Bytes, BytesMut};
use prometheus::Counter;
use std::collections::VecDeque;

lazy_static! {
    static ref UNPOOLED_BUFFER_CREATE: Counter = register_counter!(
        "msg_unpooled_buffer",
        "Number of buffers created due to pool depletion"
    ).unwrap();
}

/// Pool for writable bytes
pub struct BytesPool {
    buf_capacity: usize,
    unused: VecDeque<Bytes>,
}

impl BytesPool {
    pub fn new(buf_capacity: usize) -> BytesPool {
        BytesPool {
            buf_capacity,
            unused: VecDeque::new(),
        }
    }

    pub fn take(&mut self) -> BytesMut {
        // try to pull from the front, if that doesn't
        // work assume that the rest aren't drained either
        // TODO: would it be more effective to go through the whole queue?
        let front_bytes = self.unused.pop_front().map(|bytes| bytes.try_mut());
        match front_bytes {
            Some(Ok(mut bytes_mut)) => {
                bytes_mut.clear();
                bytes_mut
            }
            Some(Err(bytes)) => {
                UNPOOLED_BUFFER_CREATE.inc();
                self.unused.push_front(bytes);
                BytesMut::with_capacity(self.buf_capacity)
            }
            None => BytesMut::with_capacity(self.buf_capacity),
        }
    }

    pub fn push(&mut self, buf: Bytes) {
        self.unused.push_back(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn take_fresh() {
        let mut pool = BytesPool::new(1024);
        let buf = pool.take();
        assert_eq!(1024, buf.capacity());
        assert_eq!(0, buf.len());
    }

    #[test]
    fn take_retrned_buf() {
        let mut pool = BytesPool::new(1024);
        let mut buf = pool.take();
        buf.put_slice(b"test");
        assert_eq!(1024, buf.capacity());
        assert_eq!(4, buf.len());

        let data_ptr = buf.as_ptr();

        pool.push(buf.freeze());

        let buf = pool.take();
        assert_eq!(1024, buf.capacity());
        assert_eq!(0, buf.len());
        assert_eq!(data_ptr, buf.as_ptr());
    }

    #[test]
    fn take_fresh_buf_with_returned_not_fully_returned() {
        let mut pool = BytesPool::new(1024);
        let mut buf = pool.take();
        buf.put_slice(b"test");
        assert_eq!(1024, buf.capacity());
        assert_eq!(4, buf.len());

        let data_ptr = buf.as_ptr();

        // the bytes are cloned, so it cannot be reused mutably
        let first_buf = buf.freeze();
        pool.push(first_buf.clone());

        let buf = pool.take();
        assert_eq!(1024, buf.capacity());
        assert_eq!(0, buf.len());
        assert!(data_ptr != buf.as_ptr());
    }
}
