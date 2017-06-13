use futures::Future;
use proto::*;

pub struct TailRepliesFuture {

}


/*impl Future for TailRepliesFuture {
    type Item = ();
}*/

pub fn spawn(client_id: u64, handle: Handle, tail_addr: SocketAddr) {
    let tcp_cli: TcpClient<StreamingMultiplex<EmptyStream>, LogServerProto> =
        TcpClient::new(LogServerProto);

    // connect to the tail
    tcp_cli.connect(&tail_addr, &handle)
        .and_then(|conn| {
            let req = Request::ClientHello {
                client_id,
                // TODO: remove hardcode
                last_known_offset: 0,
            };
            conn.call(req)
        })

}
