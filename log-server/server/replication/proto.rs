use std::io;
use std::str;
use std::os::unix::io::AsRawFd;

use proto::{ReplicationRequest, ReplicationServerProtocol, ReplicationResponseHeader};
use tokio_core::io::{Io, EasyBuf, Codec};
use futures::{Poll, Stream, Sink, Async, AsyncSink, StartSend};

use messages::*;

pub struct ReplicationFramed<T> {
    codec: ReplicationServerProtocol,
    upstream: T,
    eof: bool,
    is_readable: bool,
    rd: EasyBuf,
    wr: Vec<(Vec<u8>, FileSlice)>,
}

impl<T: Io + AsRawFd> Stream for ReplicationFramed<T> {
    type Item = ReplicationRequest;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<ReplicationRequest>, io::Error> {
        loop {
            // If the read buffer has any pending data, then it could be
            // possible that `decode` will return a new frame. We leave it to
            // the decoder to optimize detecting that more data is required.
            if self.is_readable {
                if self.eof {
                    if self.rd.len() == 0 {
                        return Ok(None.into());
                    } else {
                        let frame = self.codec.decode_eof(&mut self.rd)?;
                        return Ok(Async::Ready(Some(frame)));
                    }
                }
                trace!("attempting to decode a frame");
                if let Some(frame) = self.codec.decode(&mut self.rd)? {
                    trace!("frame decoded from buffer");
                    return Ok(Async::Ready(Some(frame)));
                }
                self.is_readable = false;
            }

            assert!(!self.eof);

            // Otherwise, try to read more data and try again
            //
            // TODO: shouldn't read_to_end, that may read a lot
            let before = self.rd.len();
            let ret = self.upstream.read_to_end(&mut self.rd.get_mut());
            match ret {
                Ok(_n) => self.eof = true,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if self.rd.len() == before {
                        return Ok(Async::NotReady);
                    }
                }
                Err(e) => return Err(e),
            }
            self.is_readable = true;
        }
    }
}

impl<T: Io + AsRawFd> Sink for ReplicationFramed<T> {
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
        let mut hdr = Vec::with_capacity(5);
        try!(self.codec.encode(ReplicationResponseHeader(item.remaining_bytes()), &mut hdr));
        self.wr.push((hdr, item));
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        trace!("flushing framed transport");

        while let Some((mut hdr, mut fs)) = self.wr.pop() {
            // write the header for the file slice
            if !hdr.is_empty() {
                trace!("Writing header");
                let n = try_nb!(self.upstream.write(&hdr));
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero,
                                              "failed to write frame to transport"));
                }
                hdr.drain(..n);

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
            match fs.send(self.upstream.as_raw_fd()) {
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
        try_nb!(self.upstream.flush());

        trace!("framed transport flushed");
        Ok(Async::Ready(()))
    }
}

impl<T: Io + AsRawFd> ReplicationFramed<T> {
    pub fn new(io: T) -> ReplicationFramed<T> {
        ReplicationFramed {
            codec: ReplicationServerProtocol,
            upstream: io,
            eof: false,
            is_readable: false,
            rd: EasyBuf::new(),
            wr: Vec::with_capacity(8),
        }
    }

    fn write_buffer_size(&self) -> usize {
        self.wr.iter().map(|v| v.0.len() + v.1.remaining_bytes()).sum()
    }
}
