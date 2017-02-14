use std::io;

use futures::Future;
use futures::future::Map;
use tokio_proto::pipeline::ServerProto;
use tokio_core::net::TcpStream;
use tokio_core::io::{Framed, Io};
use tokio_service::{NewService, Service};

use proto::{ReplicationRequest, ReplicationResponse, ReplicationServerProtocol};
use asynclog::{Messages, AsyncLog, LogFuture};

#[derive(Default)]
pub struct ReplicationServerProto;

impl ServerProto<TcpStream> for ReplicationServerProto {
    type Request = ReplicationRequest;
    type Response = ReplicationResponse<Messages>;
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
    type Request = ReplicationRequest;
    type Response = ReplicationResponse<Messages>;
    type Error = io::Error;
    type Instance = ReplicationService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(ReplicationService(self.log.clone()))
    }
}


pub struct ReplicationService(AsyncLog);

impl Service for ReplicationService {
    type Request = ReplicationRequest;
    type Response = ReplicationResponse<Messages>;
    type Error = io::Error;
    type Future = ReplicationFuture;

    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Servicing replication request");
        match req {
            ReplicationRequest::StartFrom(offset) => {
                self.0.replicate_from(offset)
                    .map(ReplicationResponse::Messages)
            }
        }
    }
}

pub type ReplicationFuture = Map<LogFuture<Messages>, fn(Messages) -> ReplicationResponse<Messages>>;
