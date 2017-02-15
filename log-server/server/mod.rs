mod frontend;
mod replication;
pub mod net;

use std::net::SocketAddr;

use tokio_core::reactor::Handle;
use tokio_proto::multiplex::Multiplex;
use tokio_proto::pipeline::Pipeline;

use asynclog::AsyncLog;
use self::net::*;
use self::frontend::{LogProto, LogServiceCreator};
use self::replication::{ReplicationServerProto, ReplicationServiceCreator};

pub fn spawn_replication
    (log: &AsyncLog,
     addr: SocketAddr,
     handle: &Handle)
     -> TcpServerFuture<Pipeline, ReplicationServerProto, ReplicationServiceCreator> {
    TcpServer::new(ReplicationServerProto,
                   ReplicationServiceCreator::new(log.clone()))
        .spawn(addr, handle)

}

pub fn spawn_frontend(log: &AsyncLog,
                      addr: SocketAddr,
                      handle: &Handle)
                      -> TcpServerFuture<Multiplex, LogProto, LogServiceCreator> {
    TcpServer::new(frontend::LogProto,
                   frontend::LogServiceCreator::new(log.clone()))
        .spawn(addr, handle)
}
