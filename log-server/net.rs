use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Future, Async, Poll};
use futures::stream::Stream;
use net2;
use tokio_core::net::{Incoming, TcpStream, TcpListener};
use tokio_core::reactor::Handle;
use tokio_proto::BindServer;
use tokio_service::NewService;

pub struct TcpServer<Kind, P, S> {
    _kind: PhantomData<Kind>,
    proto: P,
    new_service: S,
    multi_threaded: bool,
}

impl<Kind, P, S> TcpServer<Kind, P, S>
    where P: BindServer<Kind, TcpStream> + Send + Sync + 'static,
          S: NewService<Request = P::ServiceRequest,
                        Response = P::ServiceResponse,
                        Error = P::ServiceError> + 'static,
          P::ServiceError: From<io::Error>
{
    pub fn new(protocol: P, new_service: S) -> TcpServer<Kind, P, S> {
        TcpServer {
            _kind: PhantomData,
            proto: protocol,
            new_service: new_service,
            multi_threaded: false,
        }
    }

    pub fn spawn(self, addr: SocketAddr, handle: &Handle) -> TcpServerFuture<Kind, P, S> {
        let listener = listener(&addr, self.multi_threaded, &handle).unwrap();
        TcpServerFuture {
            _kind: PhantomData,
            proto: self.proto,
            new_service: self.new_service,
            handle: handle.clone(),
            incoming: listener.incoming(),
        }
    }
}

pub struct TcpServerFuture<Kind, P, S> {
    _kind: PhantomData<Kind>,
    proto: P,
    new_service: S,
    handle: Handle,
    incoming: Incoming,
}

impl<Kind, P, S> Future for TcpServerFuture<Kind, P, S>
    where P: BindServer<Kind, TcpStream> + Send + Sync + 'static,
          S: NewService<Request = P::ServiceRequest,
                        Response = P::ServiceResponse,
                        Error = P::ServiceError> + 'static,
          P::ServiceError: From<io::Error>
{
    type Item = ();
    type Error = P::ServiceError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match try_ready!(self.incoming.poll()) {
                Some(socket) => {
                    let service = self.new_service.new_service()?;
                    self.proto.bind_server(&self.handle, socket.0, service);
                }
                None => return Ok(Async::Ready(())),
            }
        }
    }
}

fn listener(addr: &SocketAddr, multi_threaded: bool, handle: &Handle) -> io::Result<TcpListener> {
    let listener = match *addr {
        SocketAddr::V4(_) => try!(net2::TcpBuilder::new_v4()),
        SocketAddr::V6(_) => try!(net2::TcpBuilder::new_v6()),
    };
    try!(configure_tcp(multi_threaded, &listener));
    try!(listener.reuse_address(true));
    try!(listener.bind(addr));
    listener.listen(1024).and_then(|l| TcpListener::from_listener(l, addr, handle))
}

#[cfg(unix)]
fn configure_tcp(multi_threaded: bool, tcp: &net2::TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    if multi_threaded {
        try!(tcp.reuse_port(true));
    }

    Ok(())
}

#[cfg(windows)]
fn configure_tcp(_multi_threaded: bool, _tcp: &net2::TcpBuilder) -> io::Result<()> {
    Ok(())
}
