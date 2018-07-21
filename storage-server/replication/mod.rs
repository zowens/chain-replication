use asynclog::ReplicatorAsyncLog;
use futures::{Future, Stream};
use std::net::SocketAddr;
use tokio;
use tokio::net::TcpListener;

mod client;
mod io;
mod poll;
mod protocol;

// TODO: remove pub
pub mod log_reader;

pub use self::log_reader::{FileSlice, FileSliceMessageReader};
pub use self::poll::Replication;

pub fn server(
    addr: &SocketAddr,
    log: ReplicatorAsyncLog<FileSlice>,
) -> impl Future<Item = (), Error = ()> {
    let listener =
        TcpListener::bind(addr).expect("unable to bind TCP listener for replication server");
    listener
        .incoming()
        .map_err(|e| error!("accept failed = {:?}", e))
        .for_each(move |sock| {
            let log = log.clone();

            if let Err(e) = sock.set_nodelay(true) {
                warn!("Unable to set nodelay on socket: {}", e);
            }

            let (request_stream, write_buf) = self::io::replication_framed(sock);

            let handle_conn = request_stream
                .and_then(move |req| log.replicate_from(req.starting_offset))
                .forward(write_buf)
                .map_err(|e| error!("Connection error: {}", e))
                .map(|_| trace!("Connection closed"));

            tokio::spawn(handle_conn)
        })
}
