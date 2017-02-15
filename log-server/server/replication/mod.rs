use std::io;

use tokio_proto::pipeline::ServerProto;
use tokio_core::net::TcpStream;
use tokio_service::{NewService, Service};

use proto::ReplicationRequest;
use asynclog::{AsyncLog, LogFuture};
use messages::FileSlice;

mod proto;
use self::proto::ReplicationFramed;

#[derive(Default)]
pub struct ReplicationServerProto;

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
    type Response = FileSlice;
    type Error = io::Error;
    type Instance = ReplicationService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(ReplicationService(self.log.clone()))
    }
}


pub struct ReplicationService(AsyncLog);

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
