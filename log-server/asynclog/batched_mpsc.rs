//! A multi-producer, single-consumer, futures-aware, FIFO queue with back pressure.
//!
//! A channel can be used as a communication primitive between tasks running on
//! `futures-rs` executors. Channel creation provides `Receiver` and `Sender`
//! handles. `Receiver` implements `Stream` and allows a task to read values
//! out of the channel. If there is no message to read from the channel, the
//! current task will be notified when a new value is sent. `Sender` implements
//! the `Sink` trait and allows a task to send messages into the channel. If
//! the channel is at capacity, then send will be rejected and the task will be
//! notified when additional capacity is available.
//!
//! # Disconnection
//!
//! When all `Sender` handles have been dropped, it is no longer possible to
//! send values into the channel. This is considered the termination event of
//! the stream. As such, `Sender::poll` will return `Ok(Ready(None))`.
//!
//! If the receiver handle is dropped, then messages can no longer be read out
//! of the channel. In this case, a `send` will result in an error.
//!
//! # Clean Shutdown
//!
//! If the `Receiver` is simply dropped, then it is possible for there to be
//! messages still in the channel that will not be processed. As such, it is
//! usually desirable to perform a "clean" shutdown. To do this, the receiver
//! will first call `close`, which will prevent any further messages to be sent
//! into the channel. Then, the receiver consumes the channel to completion, at
//! which point the receiver can be dropped.

// At the core, the channel uses an atomic FIFO queue for message passing. This
// queue is used as the primary coordination primitive. In order to enforce
// capacity limits and handle back pressure, a secondary FIFO queue is used to
// send parked task handles.
//
// The general idea is that the channel is created with a `buffer` size of `n`.
// The channel capacity is `n + num-senders`. Each sender gets one "guaranteed"
// slot to hold a message. This allows `Sender` to know for a fact that a send
// will succeed *before* starting to do the actual work of sending the value.
// Since most of this work is lock-free, once the work starts, it is impossible
// to safely revert.
//
// If the sender is unable to process a send operation, then the the curren
// task is parked and the handle is sent on the parked task queue.
//
// Note that the implementation guarantees that the channel capacity will never
// exceed the configured limit, however there is no *strict* guarantee that the
// receiver will wake up a parked task *immediately* when a slot becomes
// available. However, it will almost always unpark a task when a slot becomes
// available and it is *guaranteed* that a sender will be unparked when the
// message that caused the sender to become parked is read out of the channel.
//
// The steps for sending a message are roughly:
//
// 1) Increment the channel message count
// 2) If the channel is at capacity, push the task handle onto the wait queue
// 3) Push the message onto the message queue.
//
// The steps for receiving a message are roughly:
//
// 1) Pop a message from the message queue
// 2) Pop a task handle from the wait queue
// 3) Decrement the channel message count.
//
// It's important for the order of operations on lock-free structures to happen
// in reverse order between the sender and receiver. This makes the message
// queue the primary coordination structure and establishes the necessary
// happens-before semantics required for the acquire / release semantics used
// by the queue structure.

use std::fmt;
use std::error::Error;
use std::any::Any;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::usize;

use super::queue::{BatchQueue, BatchPopResult};
use futures::task::{self, Task};
use futures::{Async, AsyncSink, Poll, StartSend, Sink, Stream};
use asynclog::queue::MessageBatch;

/// The transmission end of a channel which is used to send values.
///
/// This is created by the `channel` method.
pub struct UnboundedSender<T> {
    // Channel state shared between the sender and receiver.
    inner: Arc<Inner<T>>,
}

fn _assert_kinds() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_clone<T: Clone>() {}
    _assert_send::<UnboundedSender<u32>>();
    _assert_sync::<UnboundedSender<u32>>();
    _assert_clone::<UnboundedSender<u32>>();
}


/// The receiving end of a channel which implements the `Stream` trait.
///
/// This is a concrete implementation of a stream which can be used to represent
/// a stream of values being computed elsewhere. This is created by the
/// `channel` method.
pub struct UnboundedReceiver<T> {
    inner: Arc<Inner<T>>,
}

/// Error type for sending, used when the receiving end of a channel is
/// dropped
pub struct SendError<T>(T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("SendError")
            .field(&"...")
            .finish()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "send failed because receiver is gone")
    }
}

impl<T: Any> Error for SendError<T> {
    fn description(&self) -> &str {
        "send failed because receiver is gone"
    }
}

struct Inner<T> {
    // Internal channel state. Consists of the number of messages stored in the
    // channel as well as a flag signalling that the channel is closed.
    state: AtomicUsize,

    // Atomic, FIFO queue used to send messages to the receiver
    message_queue: BatchQueue<T>,

    // Number of senders in existence
    num_senders: AtomicUsize,

    // Handle to the receiver's task.
    recv_task: Mutex<ReceiverTask>,
}

// Struct representation of `Inner::state`.
#[derive(Debug, Clone, Copy)]
struct State {
    // `true` when the channel is open
    is_open: bool,

    // Number of messages in the channel
    num_messages: usize,
}

struct ReceiverTask {
    unparked: bool,
    task: Option<Task>,
}

// Returned from Receiver::try_park()
enum TryPark {
    Parked,
    Closed,
    NotEmpty,
}

// The `is_open` flag is stored in the left-most bit of `Inner::state`
const OPEN_MASK: usize = 1 << 31;

// When a new channel is created, it is created in the open state with no
// pending messages.
const INIT_STATE: usize = OPEN_MASK;

// The maximum number of messages that a channel can track is `usize::MAX > 1`
const MAX_CAPACITY: usize = !(OPEN_MASK);

// The maximum requested buffer size must be less than the maximum capacity of
// a channel. This is because each sender gets a guaranteed slot.
const MAX_BUFFER: usize = MAX_CAPACITY >> 1;

/// Creates an in-memory channel implementation of the `Stream` trait with
/// unbounded capacity.
///
/// This method creates a concrete implementation of the `Stream` trait which
/// can be used to send values across threads in a streaming fashion. A `send`
/// on this channel will always succeed as long as the receive half has not
/// been closed. If the receiver falls behind, messages will be buffered
/// internally.
///
/// **Note** that the amount of available system memory is an implicit bound to
/// the channel. Using an `unbounded` channel has the ability of causing the
/// process to run out of memory. In this case, the process will be aborted.
#[allow(dead_code)]
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicUsize::new(INIT_STATE),
        message_queue: BatchQueue::new(),
        num_senders: AtomicUsize::new(1),
        recv_task: Mutex::new(ReceiverTask {
            unparked: false,
            task: None,
        }),
    });

    let tx = UnboundedSender { inner: inner.clone() };

    let rx = UnboundedReceiver { inner: inner };

    (tx, rx)
}

/*
 *
 * ===== impl Sender =====
 *
 */

impl<T> UnboundedSender<T> {
    // Do the send without failing
    fn do_send(&mut self, msg: Option<T>) -> Result<(), SendError<T>> {
        // First, increment the number of messages contained by the channel.
        // This operation will also atomically determine if the sender task
        // should be parked.
        //
        // None is returned in the case that the channel has been closed by the
        // receiver. This happens when `Receiver::close` is called or the
        // receiver is dropped.
        match self.inc_num_messages(msg.is_none()) {
            Some(()) => {}
            None => {
                // The receiver has closed the channel. Only abort if actually
                // sending a message. It is important that the stream
                // termination (None) is always sent. This technically means
                // that it is possible for the queue to contain the following
                // number of messages:
                //
                //     num-senders + buffer + 1
                //
                if let Some(msg) = msg {
                    return Err(SendError(msg));
                } else {
                    return Ok(());
                }
            }
        };

        self.queue_push_and_signal(msg);

        Ok(())
    }

    // Do the send without parking current task.
    //
    // To be called from unbounded sender.
    fn do_send_nb(&self, msg: T) -> Result<(), SendError<T>> {
        if self.inc_num_messages(false).is_none() {
            return Err(SendError(msg));
        };

        self.queue_push_and_signal(Some(msg));

        Ok(())
    }

    // Push message to the queue and signal to the receiver
    fn queue_push_and_signal(&self, msg: Option<T>) {
        // Push the message onto the message queue
        if let Some(v) = msg {
            self.inner.message_queue.push(v);
        } else {
            self.inner.message_queue.signal_close();
        }

        // Signal to the receiver that a message has been enqueued. If the
        // receiver is parked, this will unpark the task.
        self.signal();
    }

    // Increment the number of queued messages.
    //
    // Returns None if the channel is closed, Some otherwise
    fn inc_num_messages(&self, close: bool) -> Option<()> {
        let mut curr = self.inner.state.load(SeqCst);

        loop {
            let mut state = decode_state(curr);

            // The receiver end closed the channel.
            if !state.is_open {
                return None;
            }

            // This probably is never hit? Odds are the process will run out of
            // memory first. It may be worth to return something else in this
            // case?
            assert!(state.num_messages < MAX_CAPACITY,
                    "buffer space exhausted; sending this messages would overflow the state");

            state.num_messages += 1;

            // The channel is closed by all sender handles being dropped.
            if close {
                state.is_open = false;
            }

            let next = encode_state(&state);
            match self.inner.state.compare_exchange(curr, next, SeqCst, SeqCst) {
                Ok(_) => return Some(()),
                Err(actual) => curr = actual,
            }
        }
    }

    // Signal to the receiver task that a message has been enqueued
    fn signal(&self) {
        // TODO
        // This logic can probably be improved by guarding the lock with an
        // atomic.
        //
        // Do this step first so that the lock is dropped when
        // `unpark` is called
        let task = {
            let mut recv_task = self.inner.recv_task.lock().unwrap();

            // If the receiver has already been unparked, then there is nothing
            // more to do
            if recv_task.unparked {
                return;
            }

            // Setting this flag enables the receiving end to detect that
            // an unpark event happened in order to avoid unecessarily
            // parking.
            recv_task.unparked = true;
            recv_task.task.take()
        };

        if let Some(task) = task {
            task.unpark();
        }
    }
}

impl<T> Sink for UnboundedSender<T> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        // The channel has capacity to accept the message, so send it.
        try!(self.do_send(Some(msg)));

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T> UnboundedSender<T> {
    /// Sends the provided message along this channel.
    ///
    /// This is an unbounded sender, so this function differs from `Sink::send`
    /// by ensuring the return type reflects that the channel is always ready to
    /// receive messages.
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.do_send_nb(msg)
    }
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> UnboundedSender<T> {
        // Since this atomic op isn't actually guarding any memory and we don't
        // care about any orderings besides the ordering on the single atomic
        // variable, a relaxed ordering is acceptable.
        let mut curr = self.inner.num_senders.load(SeqCst);

        loop {
            // If the maximum number of senders has been reached, then fail
            if curr == self.inner.max_senders() {
                panic!("cannot clone `Sender` -- too many outstanding senders");
            }

            debug_assert!(curr < self.inner.max_senders());

            let next = curr + 1;
            let actual = self.inner.num_senders.compare_and_swap(curr, next, SeqCst);

            // The ABA problem doesn't matter here. We only care that the
            // number of senders never exceeds the maximum.
            if actual == curr {
                return UnboundedSender { inner: self.inner.clone() };
            }

            curr = actual;
        }
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        // Ordering between variables don't matter here
        let prev = self.inner.num_senders.fetch_sub(1, SeqCst);

        if prev == 1 {
            let _ = self.do_send(None);
        }
    }
}

/*
 *
 * ===== impl Receiver =====
 *
 */

impl<T> UnboundedReceiver<T> {
    /// Closes the receiving half
    ///
    /// This prevents any further messages from being sent on the channel while
    /// still enabling the receiver to drain messages that are buffered.
    pub fn close(&mut self) {
        let mut curr = self.inner.state.load(SeqCst);

        loop {
            let mut state = decode_state(curr);

            if !state.is_open {
                break;
            }

            state.is_open = false;

            let next = encode_state(&state);
            match self.inner.state.compare_exchange(curr, next, SeqCst, SeqCst) {
                Ok(_) => break,
                Err(actual) => curr = actual,
            }
        }
    }

    fn next_message(&mut self) -> Async<Option<MessageBatch<T>>> {
        // Pop off a message
        match unsafe { self.inner.message_queue.pop() } {
            BatchPopResult::Data(msg) => Async::Ready(Some(msg)),
            BatchPopResult::Empty => {
                // The queue is empty, return NotReady
                Async::NotReady
            }
            BatchPopResult::Closed => Async::Ready(None),
        }
    }

    // Try to park the receiver task
    fn try_park(&self) -> TryPark {
        let curr = self.inner.state.load(SeqCst);
        let state = decode_state(curr);

        // If the channel is closed, then there is no need to park.
        if !state.is_open && state.num_messages == 0 {
            return TryPark::Closed;
        }

        // First, track the task in the `recv_task` slot
        let mut recv_task = self.inner.recv_task.lock().unwrap();

        if recv_task.unparked {
            // Consume the `unpark` signal without actually parking
            recv_task.unparked = false;
            return TryPark::NotEmpty;
        }

        recv_task.task = Some(task::park());
        TryPark::Parked
    }

    fn dec_num_messages(&self, n: usize) {
        let mut curr = self.inner.state.load(SeqCst);

        loop {
            let mut state = decode_state(curr);

            state.num_messages -= n;

            let next = encode_state(&state);
            match self.inner.state.compare_exchange(curr, next, SeqCst, SeqCst) {
                Ok(_) => break,
                Err(actual) => curr = actual,
            }
        }
    }
}

impl<T> Stream for UnboundedReceiver<T> {
    type Item = MessageBatch<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<MessageBatch<T>>, ()> {
        loop {
            // Try to read a message off of the message queue.
            let msg = match self.next_message() {
                Async::Ready(msg) => msg,
                Async::NotReady => {
                    // There are no messages to read, in this case, attempt to
                    // park. The act of parking will verify that the channel is
                    // still empty after the park operation has completed.
                    match self.try_park() {
                        TryPark::Parked => {
                            // The task was parked, and the channel is still
                            // empty, return NotReady.
                            return Ok(Async::NotReady);
                        }
                        TryPark::Closed => {
                            // The channel is closed, there will be no further
                            // messages.
                            return Ok(Async::Ready(None));
                        }
                        TryPark::NotEmpty => {
                            // A message has been sent while attempting to
                            // park. Loop again, the next iteration is
                            // guaranteed to get the message.
                            continue;
                        }
                    }
                }
            };

            // Decrement number of messages
            self.dec_num_messages(msg.as_ref().map(|v| v.len()).unwrap_or(1));

            // Return the message
            return Ok(Async::Ready(msg));
        }
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        // Drain the channel of all pending messages
        self.close();
        while self.next_message().is_ready() {
            // ...
        }
    }
}


/*
 *
 * ===== impl Inner =====
 *
 */

impl<T> Inner<T> {
    // The return value is such that the total number of messages that can be
    // enqueued into the channel will never exceed MAX_CAPACITY
    fn max_senders(&self) -> usize {
        MAX_BUFFER
    }
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/*
 *
 * ===== Helpers =====
 *
 */

fn decode_state(num: usize) -> State {
    State {
        is_open: num & OPEN_MASK == OPEN_MASK,
        num_messages: num & MAX_CAPACITY,
    }
}

fn encode_state(state: &State) -> usize {
    let mut num = state.num_messages;

    if state.is_open {
        num |= OPEN_MASK;
    }

    num
}
