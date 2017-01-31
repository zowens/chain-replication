mod frontend;
mod replication;
mod net;

use std::net::SocketAddr;

use futures::Future;
use tokio_core::reactor::Core;

use asynclog::AsyncLog;

pub fn start(core: &mut Core, addr: SocketAddr, replica_addr: SocketAddr) {
    let (_handle, log) = AsyncLog::open();
    let server = net::TcpServer::new(frontend::LogProto,
                                     frontend::LogServiceCreator::new(log.clone()));
    let replication_server =
        net::TcpServer::new(replication::ReplicationServerProto,
                            replication::ReplicationServiceCreator::new(log));

    let handle = core.handle();
    core.run(server.spawn(addr, &handle)
             .join(replication_server.spawn(replica_addr, &handle)))
        .unwrap();
}
