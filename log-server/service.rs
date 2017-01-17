use std::io;
use commitlog::*;
use futures;
use futures::{Stream, Future, Async, Poll};
use std::net::SocketAddr;
use tokio_core::io::{Io, Framed};
use tokio_core::net::{TcpStream, TcpListener, Incoming};
use tokio_core::reactor::Handle;
use tokio_proto::BindServer;
use tokio_proto::multiplex::ServerProto;
use tokio_service::Service;
use super::asynclog::{AsyncLog, LogFuture};
use super::protocol::*;
use net2;

union_future!(ResFuture<Res, io::Error>,
              Offset => LogFuture<Offset>,
              Messages => LogFuture<MessageBuf>);


struct LogService(AsyncLog);

impl Service for LogService {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = ResFuture;

    fn call(&self, req: Req) -> Self::Future {
        match req {
            Req::Append(val) => self.0.append(val).into(),
            Req::ReplicateFrom(offset) => self.0.replicate_from(offset).into(),
            Req::Read(off) => {
                self.0
                    .read(ReadPosition::Offset(Offset(off)), ReadLimit::Messages(10))
                    .into()
            }
            Req::LastOffset => self.0.last_offset().into(),
        }
    }
}


#[derive(Default)]
struct LogProto;

impl ServerProto<TcpStream> for LogProto {
    type Request = Req;
    type Response = Res;
    type Transport = Framed<TcpStream, Protocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(Protocol))
    }
}

pub fn spawn_service(addr: SocketAddr, handle: &Handle, log: AsyncLog) -> ServiceFuture {
    let listener = listener(&addr, 1, handle).unwrap();
    ServiceFuture {
        log: log,
        incoming: listener.incoming(),
        handle: handle.clone(),
    }
}

pub struct ServiceFuture {
    log: AsyncLog,
    incoming: Incoming,
    handle: Handle,
}

impl Future for ServiceFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // grab ready connections
        loop {
            match try_ready!(self.incoming.poll()) {
                Some(stream) => {
                    // bind the connection
                    LogProto.bind_server(&self.handle, stream.0, LogService(self.log.clone()));
                }
                None => return Ok(Async::Ready(())),
            }
        }
    }
}


fn listener(addr: &SocketAddr, workers: usize, handle: &Handle) -> io::Result<TcpListener> {
    let listener = match *addr {
        SocketAddr::V4(_) => try!(net2::TcpBuilder::new_v4()),
        SocketAddr::V6(_) => try!(net2::TcpBuilder::new_v6()),
    };
    try!(configure_tcp(workers, &listener));
    try!(listener.reuse_address(true));
    try!(listener.bind(addr));
    listener.listen(1024).and_then(|l| TcpListener::from_listener(l, addr, handle))
}

#[cfg(unix)]
fn configure_tcp(workers: usize, tcp: &net2::TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    if workers > 1 {
        try!(tcp.reuse_port(true));
    }

    Ok(())
}

#[cfg(windows)]
fn configure_tcp(workers: usize, _tcp: &net2::TcpBuilder) -> io::Result<()> {
    Ok(())
}
