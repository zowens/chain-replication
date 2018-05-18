use asynclog::AsyncLog;
use commitlog::message::MessageSet;
use futures::{Async, Future, Poll};
use messages::{MessageBufPool, PooledMessageBuf};
use std::cell::RefCell;
use std::intrinsics::unlikely;
use std::mem;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::executor::current_thread::spawn;
use tokio::timer::Delay;

/// Maximum bytes to buffer
const MAX_BUFFER_BYTES: usize = 16_384;

/// Number of milliseconds to wait before appending
const BATCH_WAIT_MS: u64 = 1;

/// MessageBatcher will buffer messages on a per-core basis when either the linger
/// time has lapsed or the maximum number of bytes has been reached.
#[derive(Clone)]
pub struct MessageBatcher {
    inner: Rc<RefCell<Inner>>,
}

impl MessageBatcher {
    /// Creates a message batching instance. This is a per-thread batch which waits for either
    /// the maximum number of bytes OR a time period before appending to the log.
    pub fn new(log: AsyncLog) -> MessageBatcher {
        let mut pool = MessageBufPool::new(5, MAX_BUFFER_BYTES);
        let buf = pool.take();
        MessageBatcher {
            inner: Rc::new(RefCell::new(Inner {
                buf,
                pool,
                log,
                send_epoc: 0,
            })),
        }
    }

    /// Adds a message to the batch
    pub fn push(&self, client_id: u32, client_req_id: u32, msg: Vec<u8>) {
        let mut inner = self.inner.borrow_mut();

        let existing_len = inner.buf.bytes().len();

        // send immediately if we're exceeding capacity
        if existing_len + msg.len() > MAX_BUFFER_BYTES {
            trace!("Buffer exceeded, sending immediately");
            inner.send();
        }

        inner.buf.push(client_id, client_req_id, msg);

        if existing_len == 0 {
            trace!("Spawning timeout to send to the log");
            let expected_send_epoc = inner.send_epoc;

            drop(inner);
            spawn(LingerFuture {
                delay: Delay::new(Instant::now() + Duration::from_millis(BATCH_WAIT_MS)),
                batcher: self.inner.clone(),
                expected_send_epoc,
            });
        }
    }
}

struct Inner {
    buf: PooledMessageBuf,
    log: AsyncLog,
    pool: MessageBufPool,
    send_epoc: usize,
}

impl Inner {
    fn send(&mut self) {
        trace!("Sending batch through the log");

        if unsafe { unlikely(self.buf.is_empty()) } {
            return;
        }

        self.send_epoc += 1;
        let mut buf = self.pool.take();
        mem::swap(&mut buf, &mut self.buf);
        self.log.append(buf);
    }
}

struct LingerFuture {
    delay: Delay,
    batcher: Rc<RefCell<Inner>>,
    expected_send_epoc: usize,
}

impl Future for LingerFuture {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<(), ()> {
        trace!("Polling timeout future");
        match self.delay.poll() {
            Ok(Async::Ready(())) => {}
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(e) => {
                error!("Timer error: {}", e);
                return Err(());
            }
        }

        let mut inner = self.batcher.borrow_mut();
        if unsafe { unlikely(inner.send_epoc != self.expected_send_epoc) } {
            return Ok(Async::Ready(()));
        }

        trace!("Timeout reached, sending batch through the log");
        inner.send();
        Ok(Async::Ready(()))
    }
}
