mod frontend;
mod replication;
mod net;

use std::net::SocketAddr;

use tokio_core::reactor::Handle;
use tokio_proto::multiplex::Multiplex;
use tokio_proto::streaming::multiplex::StreamingMultiplex;

use asynclog::AsyncLog;
use self::net::*;
use self::frontend::{LogProto, LogServiceCreator};
use self::replication::{ReplicationServerProto, ReplicationServiceCreator, ReplicationStream};

pub fn spawn_replication(log: &AsyncLog, addr: SocketAddr, handle: &Handle)
    -> TcpServerFuture<StreamingMultiplex<ReplicationStream>, ReplicationServerProto, ReplicationServiceCreator>
{
    TcpServer::new(ReplicationServerProto,
                   ReplicationServiceCreator::new(log.clone()))
        .spawn(addr, handle)

}

pub fn spawn_frontend(log: &AsyncLog, addr: SocketAddr, handle: &Handle)
    -> TcpServerFuture<Multiplex, LogProto, LogServiceCreator>
{
    TcpServer::new(frontend::LogProto,
                        frontend::LogServiceCreator::new(log.clone()))
        .spawn(addr, handle)
}
