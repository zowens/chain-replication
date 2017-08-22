use std::io;
use std::net::SocketAddr;

use futures::Future;
use tokio_proto::pipeline::ServerProto;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_service::{NewService, Service};

use proto::ReplicationRequest;
use asynclog::{AsyncLog, LogFuture};
use messages::FileSlice;

use self::proto::ReplicationFramed;
use tcp::TcpServer;

pub fn spawn(
    log: &AsyncLog,
    addr: SocketAddr,
    handle: &Handle,
) -> impl Future<Item = (), Error = io::Error> {
    TcpServer::new(
        ReplicationServerProto,
        ReplicationServiceCreator::new(log.clone()),
    ).spawn(addr, handle)
}


#[derive(Default)]
struct ReplicationServerProto;

impl ServerProto<TcpStream> for ReplicationServerProto {
    type Request = ReplicationRequest;
    type Response = FileSlice;
    type Transport = ReplicationFramed<TcpStream>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(ReplicationFramed::new(io))
    }
}

struct ReplicationServiceCreator {
    log: AsyncLog,
}

impl ReplicationServiceCreator {
    pub fn new(log: AsyncLog) -> ReplicationServiceCreator {
        ReplicationServiceCreator { log: log }
    }
}

impl NewService for ReplicationServiceCreator {
    type Request = ReplicationRequest;
    type Response = FileSlice;
    type Error = io::Error;
    type Instance = ReplicationService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(ReplicationService(self.log.clone()))
    }
}


struct ReplicationService(AsyncLog);

impl Service for ReplicationService {
    type Request = ReplicationRequest;
    type Response = FileSlice;
    type Error = io::Error;
    type Future = LogFuture<FileSlice>;

    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Servicing replication request");
        match req {
            ReplicationRequest::StartFrom(offset) => self.0.replicate_from(offset),
        }
    }
}

// TODO: move this somewhere else
mod proto {
    use std::io::{self, Write};
    use std::collections::VecDeque;
    use std::str;
    use std::os::unix::io::{AsRawFd, RawFd};

    use bytes::BytesMut;
    use proto::{ReplicationRequest, ReplicationResponseHeader, ReplicationServerProtocol};
    use tokio_io::{AsyncRead, AsyncWrite};
    use tokio_io::io::{ReadHalf, WriteHalf};
    use tokio_io::codec::{Encoder, FramedRead};
    use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};

    use messages::*;

    const BACKPRESSURE_BOUNDARY: usize = 8 * 1024;

    enum WriteSource {
        Bytes(BytesMut),
        File(FileSlice),
    }

    pub struct ReplicationFramed<T> {
        rd: FramedRead<ReadHalf<T>, ReplicationServerProtocol>,

        codec: ReplicationServerProtocol,
        w: WriteHalf<T>,
        wfd: RawFd,

        wr: VecDeque<WriteSource>,
        wr_bytes: usize,
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
            if self.wr_bytes > BACKPRESSURE_BOUNDARY {
                try!(self.poll_complete());
                if self.wr_bytes > BACKPRESSURE_BOUNDARY {
                    return Ok(AsyncSink::NotReady(item));
                }
            }

            let mut hdr = BytesMut::with_capacity(5);
            try!(
                self.codec
                    .encode(ReplicationResponseHeader(item.remaining_bytes()), &mut hdr,)
            );
            self.wr_bytes += hdr.len() + item.remaining_bytes();
            self.wr.push_back(WriteSource::Bytes(hdr));
            self.wr.push_back(WriteSource::File(item));
            Ok(AsyncSink::Ready)
        }

        fn poll_complete(&mut self) -> Poll<(), io::Error> {
            trace!("flushing framed transport");

            while let Some(source) = self.wr.pop_front() {
                match source {
                    WriteSource::Bytes(mut hdr) => {
                        if !hdr.is_empty() {
                            let n = match self.w.write(&hdr) {
                                Ok(0) => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::WriteZero,
                                        "failed to write frame to transport",
                                    ))
                                }
                                Ok(n) => n,
                                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    self.wr.push_front(WriteSource::Bytes(hdr));
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
                                self.wr.push_front(WriteSource::Bytes(hdr))
                            }
                        }
                    }
                    WriteSource::File(mut fs) => {
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
                                    self.wr.push_front(WriteSource::File(fs));
                                }
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                trace!("File send would block, returning to the stack");
                                self.wr.push_front(WriteSource::File(fs));
                                return Ok(Async::NotReady);
                            }
                            Err(e) => return Err(e),
                        }
                    }
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
                wr: VecDeque::with_capacity(10),
                wr_bytes: 0,
            }
        }
    }
}
