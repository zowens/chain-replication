use std::io::{self, Write};
use std::str;
use std::os::unix::io::{AsRawFd, RawFd};

use bytes::BytesMut;
use proto::{ReplicationRequest, ReplicationServerProtocol, ReplicationResponseHeader};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::io::{ReadHalf, WriteHalf};
use tokio_io::codec::{Encoder, FramedRead};
use futures::{Poll, Stream, Sink, Async, AsyncSink, StartSend};

use messages::*;

pub struct ReplicationFramed<T> {
    rd: FramedRead<ReadHalf<T>, ReplicationServerProtocol>,

    codec: ReplicationServerProtocol,
    w: WriteHalf<T>,
    wfd: RawFd,
    wr: Vec<(BytesMut, FileSlice)>,
}

impl<T: AsyncRead + AsRawFd> Stream for ReplicationFramed<T> {
    type Item = ReplicationRequest;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<ReplicationRequest>, io::Error> {
        self.rd.poll()
    }
}

impl<T: AsyncRead + AsyncWrite + AsRawFd> Sink for ReplicationFramed<T> {
    type SinkItem = FileSlice;
    type SinkError = io::Error;

    fn start_send(&mut self, item: FileSlice) -> StartSend<FileSlice, io::Error> {
        // If the buffer is already over 8KiB, then attempt to flush it. If after flushing it's
        // *still* over 8KiB, then apply backpressure (reject the send).
        if self.write_buffer_size() > 8 * 1024 {
            try!(self.poll_complete());
            if self.write_buffer_size() > 8 * 1024 {
                return Ok(AsyncSink::NotReady(item));
            }
        }

        // TODO: pool this
        let mut hdr = Vec::with_capacity(5).into();
        try!(self.codec
                 .encode(ReplicationResponseHeader(item.remaining_bytes()), &mut hdr));
        self.wr.push((hdr, item));
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        trace!("flushing framed transport");

        while let Some((mut hdr, mut fs)) = self.wr.pop() {
            // write the header for the file slice
            if !hdr.is_empty() {
                trace!("Writing header");
                let n = try_nb!(self.w.write(&hdr));
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero,
                                              "failed to write frame to transport"));
                }
                hdr.split_to(n);

                if !hdr.is_empty() {
                    warn!("Partial header written, {} bytes remaining", hdr.len());
                    self.wr.push((hdr, fs));
                    continue;
                }
            }

            // write the file slice
            debug!("Attempting write. Offset={}, bytes={}",
                   fs.file_offset(),
                   fs.remaining_bytes());
            match fs.send(self.wfd) {
                Ok(()) => {
                    if !fs.completed() {
                        debug!("Write of file slice not complete, returning to the pool");
                        self.wr.push((hdr, fs));
                    } else {
                        trace!("Write of file slice complete");
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    trace!("File send would block, returning to the stack");
                    self.wr.push((hdr, fs));
                    return Ok(Async::NotReady);
                }
                Err(e) => return Err(e),
            }
        }

        // Try flushing the underlying IO
        try_nb!(self.w.flush());

        trace!("framed transport flushed");
        Ok(Async::Ready(()))
    }
}

impl<T: AsyncRead + AsyncWrite + AsRawFd> ReplicationFramed<T> {
    pub fn new(io: T) -> ReplicationFramed<T> {
        let rawfd = io.as_raw_fd();
        let (r, w) = io.split();
        ReplicationFramed {
            rd: FramedRead::new(r, ReplicationServerProtocol),

            codec: ReplicationServerProtocol,
            w: w,
            wfd: rawfd,
            wr: Vec::with_capacity(8),
        }
    }

    fn write_buffer_size(&self) -> usize {
        self.wr
            .iter()
            .map(|v| v.0.len() + v.1.remaining_bytes())
            .sum()
    }
}
