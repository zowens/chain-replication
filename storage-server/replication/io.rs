use super::log_reader::FileSlice;
use super::protocol::{ReplicationResponseHeader, ServerProtocol};
use asynclog::{Messages, ReplicationSource};
use bytes::{Buf, BytesMut};
use commitlog::message::MessageSet;
use either::Either;
use futures::{Async, AsyncSink, Poll, Sink, StartSend};
use std::collections::VecDeque;
use std::io::{self, Cursor, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use tokio_codec::{Encoder, FramedRead};
use tokio_io::io::{ReadHalf, WriteHalf};
use tokio_io::{AsyncRead, AsyncWrite};

const BACKPRESSURE_BOUNDARY: usize = 8 * 1024;

enum WriteSource {
    Header(BytesMut),
    File(FileSlice),
    InMemory(Cursor<Messages>),
}

pub type ReadStream<T> = FramedRead<ReadHalf<T>, ServerProtocol>;

pub struct WriteSink<T> {
    w: WriteHalf<T>,
    wfd: RawFd,

    wr: VecDeque<WriteSource>,
    wr_bytes: usize,
}

#[inline]
fn create_header(bytes: usize, latest_offset: u64) -> BytesMut {
    let mut hdr = BytesMut::with_capacity(5);
    let header = ReplicationResponseHeader {
        messages_bytes_len: bytes as u32,
        latest_log_offset: latest_offset,
    };
    let mut codec = ServerProtocol;
    codec.encode(header, &mut hdr).unwrap();
    hdr
}

impl<T: AsyncWrite + AsRawFd> Sink for WriteSink<T> {
    type SinkItem = ReplicationSource<FileSlice>;
    type SinkError = io::Error;

    fn start_send(
        &mut self,
        item: ReplicationSource<FileSlice>,
    ) -> StartSend<ReplicationSource<FileSlice>, io::Error> {
        // If the buffer is already over 8KiB, then attempt to flush it. If after flushing it's
        // *still* over 8KiB, then apply backpressure (reject the send).
        if self.wr_bytes > BACKPRESSURE_BOUNDARY {
            trace!("Exisiting bytes over backpressure boundary, forcing poll_complete");
            try!(self.poll_complete());
            // TODO: test this...
            if self.wr_bytes > BACKPRESSURE_BOUNDARY {
                trace!("Forcing backpressure, too many bytes");
                return Ok(AsyncSink::NotReady(item));
            }
        }

        let ReplicationSource {
            messages,
            latest_log_offset,
        } = item;

        match messages {
            Either::Left(fs) => {
                trace!("Pushing file replication");
                let bytes = fs.remaining_bytes();
                let hdr = create_header(bytes, latest_log_offset);
                self.wr_bytes += hdr.len() + bytes;
                self.wr.push_back(WriteSource::Header(hdr));
                self.wr.push_back(WriteSource::File(fs));
            }
            Either::Right(msgs) => {
                trace!("Pushing InMemory replication");
                let bytes = msgs.bytes().len();
                let hdr = create_header(bytes, latest_log_offset);
                self.wr_bytes += hdr.len() + bytes;
                self.wr.push_back(WriteSource::Header(hdr));
                self.wr.push_back(WriteSource::InMemory(Cursor::new(msgs)));
            }
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        trace!("flushing framed transport");

        while let Some(source) = self.wr.pop_front() {
            match source {
                WriteSource::Header(mut hdr) => {
                    trace!("POP [WriteSource::Header]");
                    if hdr.is_empty() {
                        continue;
                    }

                    let n = match self.w.write(&hdr) {
                        Ok(0) => {
                            trace!("[WriteSource::Header] write0 error");
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "failed to write frame to transport",
                            ));
                        }
                        Ok(n) => n,
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            trace!("[WriteSource::Header] WOULD_BLOCK");
                            self.wr.push_front(WriteSource::Header(hdr));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => return Err(e),
                    };

                    self.wr_bytes -= n;

                    if n < hdr.len() {
                        // remove written data
                        hdr.split_to(n);
                        // only some of the data has been written, push it back to the front
                        trace!("[WriteSource::Header] {} bytes remaining", hdr.len());
                        self.wr.push_front(WriteSource::Header(hdr))
                    }
                }
                WriteSource::File(mut fs) => {
                    trace!("POP [WriteSource::File]");
                    let pre_write_bytes = fs.remaining_bytes();
                    debug!(
                        "[WriteSource::File] Attempting write. Offset={}, bytes={}",
                        fs.file_offset(),
                        pre_write_bytes
                    );
                    match fs.send(self.wfd) {
                        Ok(()) => {
                            self.wr_bytes -= pre_write_bytes - fs.remaining_bytes();

                            if !fs.completed() {
                                trace!("[WriteSource::File] sendfile not complete, returning to the pool");
                                self.wr.push_front(WriteSource::File(fs));
                            } else {
                                trace!("[WriteSource::File] sendfile complete");
                            }
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            trace!("[WriteSource::File] WOULD_BLOCK, returning to queue");
                            self.wr.push_front(WriteSource::File(fs));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => {
                            error!(
                                "[WriteSource::File] Error encountered write from file: {}",
                                e
                            );
                            return Err(e);
                        }
                    }
                }
                WriteSource::InMemory(mut cursor) => {
                    trace!("POP [WriteSource::InMemory]");
                    match self.w.write_buf(&mut cursor) {
                        Ok(Async::Ready(0)) => {
                            trace!("[WriteSource::InMemory] wrote 0 bytes");
                        }
                        Ok(Async::Ready(n)) => {
                            self.wr_bytes -= n;

                            if cursor.has_remaining() {
                                trace!("[WriteSource::InMemory] Cursor has remining bytes, wrote {}, wr_bytes={}", n, self.wr_bytes);
                                self.wr.push_front(WriteSource::InMemory(cursor));
                            } else {
                                trace!("[WriteSource::InMemory] write complete");
                            }
                        }
                        Ok(Async::NotReady) => {
                            trace!("[WriteSource::InMemory] not ready");
                            self.wr.push_front(WriteSource::InMemory(cursor));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => {
                            error!("[WriteSource::InMemory] Error from in memory: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        trace!("DONE with flushing, flushing underlying transport");

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
        w,
        wfd: rawfd,
        wr: VecDeque::with_capacity(10),
        wr_bytes: 0,
    };

    (rs, ws)
}
