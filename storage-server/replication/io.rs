use super::protocol::{ReplicationResponseHeader, ServerProtocol};
use bytes::BytesMut;
use either::Either;
use futures::{Async, AsyncSink, Poll, Sink, StartSend};
use messages::*;
use std::collections::VecDeque;
use std::io::{self, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use tokio_io::codec::{Encoder, FramedRead};
use tokio_io::io::{ReadHalf, WriteHalf};
use tokio_io::{AsyncRead, AsyncWrite};

const BACKPRESSURE_BOUNDARY: usize = 8 * 1024;

type WriteSource = Either<BytesMut, FileSlice>;

pub type ReadStream<T> = FramedRead<ReadHalf<T>, ServerProtocol>;

pub struct WriteSink<T> {
    codec: ServerProtocol,
    w: WriteHalf<T>,
    wfd: RawFd,

    wr: VecDeque<WriteSource>,
    wr_bytes: usize,
}

impl<T: AsyncWrite + AsRawFd> Sink for WriteSink<T> {
    type SinkItem = FileSlice;
    type SinkError = io::Error;

    fn start_send(&mut self, item: FileSlice) -> StartSend<FileSlice, io::Error> {
        // If the buffer is already over 8KiB, then attempt to flush it. If after flushing it's
        // *still* over 8KiB, then apply backpressure (reject the send).
        if self.wr_bytes > BACKPRESSURE_BOUNDARY {
            try!(self.poll_complete());
            if self.wr_bytes > BACKPRESSURE_BOUNDARY {
                return Ok(AsyncSink::NotReady(item));
            }
        }

        let mut hdr = BytesMut::with_capacity(5);
        let header = ReplicationResponseHeader {
            messages_bytes_len: item.remaining_bytes() as u32,
        };
        self.codec.encode(header, &mut hdr)?;

        self.wr_bytes += hdr.len() + item.remaining_bytes();
        self.wr.push_back(Either::Left(hdr));
        self.wr.push_back(Either::Right(item));

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        trace!("flushing framed transport");

        while let Some(source) = self.wr.pop_front() {
            match source {
                Either::Left(mut hdr) => {
                    if hdr.is_empty() {
                        continue;
                    }

                    let n = match self.w.write(&hdr) {
                        Ok(0) => {
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "failed to write frame to transport",
                            ))
                        }
                        Ok(n) => n,
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            self.wr.push_front(Either::Left(hdr));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => return Err(e),
                    };

                    self.wr_bytes -= n;

                    if n < hdr.len() {
                        // remove written data
                        hdr.split_to(n);
                        // only some of the data has been written, push it back to the front
                        trace!("Partial header written, {} bytes remaining", hdr.len());
                        self.wr.push_front(Either::Left(hdr))
                    }
                }
                Either::Right(mut fs) => {
                    let pre_write_bytes = fs.remaining_bytes();
                    debug!(
                        "Attempting write. Offset={}, bytes={}",
                        fs.file_offset(),
                        pre_write_bytes
                    );
                    match fs.send(self.wfd) {
                        Ok(()) => {
                            self.wr_bytes -= pre_write_bytes - fs.remaining_bytes();

                            if !fs.completed() {
                                trace!("sendfile not complete, returning to the pool");
                                self.wr.push_front(Either::Right(fs));
                            }
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            trace!("File send would block, returning to the stack");
                            self.wr.push_front(Either::Right(fs));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        // Try flushing the underlying IO
        self.w.poll_flush()
    }
}

pub fn replication_framed<T>(io: T) -> (ReadStream<T>, WriteSink<T>)
where
    T: AsyncRead + AsyncWrite + AsRawFd,
{
    let rawfd = io.as_raw_fd();
    let (r, w) = io.split();

    let rs = FramedRead::new(r, ServerProtocol);

    let ws = WriteSink {
        codec: ServerProtocol,
        w,
        wfd: rawfd,
        wr: VecDeque::with_capacity(10),
        wr_bytes: 0,
    };

    (rs, ws)
}
