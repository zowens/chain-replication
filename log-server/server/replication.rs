use std::io;

use futures::stream::{empty, Empty};
use futures::future::{ok, FutureResult};
use tokio_proto::streaming::{Message, Body};
use tokio_proto::streaming::multiplex::ServerProto;
use tokio_core::net::TcpStream;
use tokio_core::io::{Framed, Io};
use tokio_service::{NewService, Service};

use proto::{ReplicationRequestHeaders, ReplicationResponseHeaders, ReplicationServerProtocol};
use asynclog::{Messages, AsyncLog};

#[derive(Default)]
pub struct ReplicationServerProto;

impl ServerProto<TcpStream> for ReplicationServerProto {
    type Request = ReplicationRequestHeaders;
    type RequestBody = ();
    type Response = ReplicationResponseHeaders;
    type ResponseBody = Messages;
    type Error = io::Error;
    type Transport = Framed<TcpStream, ReplicationServerProtocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(ReplicationServerProtocol::default()))
    }
}

pub struct ReplicationServiceCreator {
    log: AsyncLog,
}

impl ReplicationServiceCreator {
    pub fn new(log: AsyncLog) -> ReplicationServiceCreator {
        ReplicationServiceCreator { log: log }
    }
}

impl NewService for ReplicationServiceCreator {
    type Request = Message<ReplicationRequestHeaders, Body<(), io::Error>>;
    type Response = Message<ReplicationResponseHeaders, ReplicationStream>;
    type Error = io::Error;
    type Instance = ReplicationService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(ReplicationService(self.log.clone()))
    }
}


pub struct ReplicationService(AsyncLog);

// TODO: figure this out
pub type ReplicationStream = Empty<Messages, io::Error>;

impl Service for ReplicationService {
    type Request = Message<ReplicationRequestHeaders, Body<(), io::Error>>;
    type Response = Message<ReplicationResponseHeaders, ReplicationStream>;
    type Error = io::Error;
    type Future = StartReplicationFuture;

    fn call(&self, _req: Self::Request) -> Self::Future {
        info!("Servicing replication request");
        ok(Message::WithBody(ReplicationResponseHeaders::Replicate, empty()))
    }
}

pub type StartReplicationFuture = FutureResult<Message<ReplicationResponseHeaders,
                                                       ReplicationStream>,
                                               io::Error>;
