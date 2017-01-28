/* Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Dmitry Vyukov.
 */

//! A mostly lock-free multi-producer, single consumer queue.
//!
//! This module contains an implementation of a concurrent MPSC queue. This
//! queue can be used to share data between threads, and is also used as the
//! building block of channels in rust.
//!
//! Note that the current implementation of this queue has a caveat of the `pop`
//! method, and see the method for more information about it. Due to this
//! caveat, this queue may not be appropriate for all use-cases.

// http://www.1024cores.net/home/lock-free-algorithms
//                         /queues/non-intrusive-mpsc-node-based-queue

// NOTE: this implementation is lifted from the standard library and only
//       slightly modified

use std::prelude::v1::*;

use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

pub enum BatchPopResult<T> {
    Data(T),
    Empty,
    Closed,
}

struct BatchNode<T> {
    next: *mut BatchNode<T>,
    value: Option<T>,
}

/// The multi-producer single-consumer structure. This is not cloneable, but it
/// may be safely shared so long as it is guaranteed that there is only one
/// popper at a time (many pushers are allowed).
pub struct BatchQueue<T> {
    head: AtomicPtr<BatchNode<T>>,
}

unsafe impl<T: Send> Send for BatchQueue<T> {}
unsafe impl<T: Send> Sync for BatchQueue<T> {}

impl<T> BatchNode<T> {
    unsafe fn new(v: Option<T>) -> *mut BatchNode<T> {
        Box::into_raw(Box::new(BatchNode {
            next: ptr::null_mut(),
            value: v,
        }))
    }
}

impl<T> BatchQueue<T> {
    /// Creates a new queue that is safe to share among multiple producers and
    /// one consumer.
    pub fn new() -> BatchQueue<T> {
        BatchQueue { head: AtomicPtr::default() }
    }

    /// Pushes a new value onto this queue.
    pub fn push(&self, t: T) {
        trace!("--- pushing value");
        unsafe {
            let n = BatchNode::new(Some(t));
            loop {
                let head = self.head.load(Ordering::Acquire);
                (*n).next = head;
                // TODO: Check if we're closed
                if head == self.head.compare_and_swap(head, n, Ordering::AcqRel) {
                    trace!("---> value pushed into the queue");
                    return;
                }
            }
        }
    }

    pub fn signal_close(&self) {
        unsafe {
            let n = BatchNode::new(None);
            loop {
                let head = self.head.load(Ordering::Acquire);
                (*n).next = head;
                if head == self.head.compare_and_swap(head, n, Ordering::AcqRel) {
                    return;
                }
            }
        }
    }

    /// Pops some data from this queue.
    ///
    /// This function is unsafe because only one thread can call it at a time.
    pub unsafe fn pop(&self) -> BatchPopResult<Vec<T>> {
        let mut hd = self.head.swap(ptr::null_mut(), Ordering::AcqRel);
        if hd.is_null() {
            trace!("Queue is empty");
            return BatchPopResult::Empty;
        }

        let mut vals = Vec::new();
        while !hd.is_null() {
            let is_open = (*hd).value.is_some();
            if is_open {
                vals.push((*hd).value.take().unwrap());
                let ret = (*hd).next;
                drop(Box::from_raw(hd));
                hd = ret;
            } else {
                // TODO: not such a good solution... queue does not stay closed
                drop(Box::from_raw(hd));
                return BatchPopResult::Closed;

            }
        }

        vals.reverse();
        BatchPopResult::Data(vals)
    }
}

impl<T> Drop for BatchQueue<T> {
    fn drop(&mut self) {
        unsafe {
            let mut cur = self.head.load(Ordering::Acquire);
            while !cur.is_null() {
                let next = (*cur).next;
                drop(Box::from_raw(cur));
                cur = next;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time};
    use std::sync::Arc;

    #[test]
    pub fn test_single_thread_push_pop() {
        let queue = BatchQueue::new();
        queue.push(5);
        queue.push(10);
        queue.push(15);

        unsafe {
            let vs = queue.pop();
            assert!(vs.is_some());
            assert_eq!(vec![5, 10, 15], vs.unwrap());
        }

        unsafe {
            let vs = queue.pop();
            assert!(vs.is_none());
        }

        queue.push(20);

        unsafe {
            let vs = queue.pop();
            assert!(vs.is_some());
            assert_eq!(vec![20], vs.unwrap());
        }

        // test drop w/o consume
        queue.push(25);
    }

    #[test]
    pub fn test_multiple_producers() {
        let queue = Arc::new(BatchQueue::new());

        let p1 = {
            let queue = queue.clone();
            thread::spawn(move || for i in 0..100u32 {
                if i % 4 == 0 {
                    thread::sleep(time::Duration::from_millis(1));
                }

                queue.push(i);
            })
        };

        let p2 = {
            let queue = queue.clone();
            thread::spawn(move || for i in 100..200u32 {
                if i % 3 == 0 {
                    thread::sleep(time::Duration::from_millis(1));
                }

                queue.push(i);
            })
        };

        let c = {
            let queue = queue.clone();
            thread::spawn(move || {
                let mut seen_mix = false;
                let mut last_c1 = None;
                let mut last_c2 = None;
                let mut c1values = 0;
                let mut c2values = 0;

                while c1values + c2values < 200 {
                    if let Some(vs) = unsafe { queue.pop() } {
                        for i in vs {
                            if i < 100 {
                                c1values += 1;

                                // assert linerizable by thread
                                match last_c1 {
                                    Some(v) => assert_eq!(v + 1, i),
                                    None => assert_eq!(i, 0),
                                }
                                last_c1 = Some(i);
                            } else {
                                c2values += 1;

                                // assert linerizable by thread
                                match last_c2 {
                                    Some(v) => assert_eq!(v + 1, i),
                                    None => assert_eq!(i, 100),
                                }
                                last_c2 = Some(i);
                            }
                        }

                        seen_mix = seen_mix ||
                                   (c1values > 0 && c2values > 0 && c1values < 100 &&
                                    c2values < 100);
                    }
                }
                // assert that we have seen interleaving
                assert!(seen_mix);
            })
        };

        p1.join().unwrap();
        p2.join().unwrap();
        c.join().unwrap();
    }
}
