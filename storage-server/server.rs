use asynclog::AsyncLog;
use bytes::Bytes;
use commitlog::{message::MessageSet, ReadLimit};
use config::FrontendConfig;
use futures::{Async, Future, Poll, Sink, Stream};
use grpcio::{
    self, Environment, RpcContext, Server as GrpcServer, ServerBuilder, ServerStreamingSink,
    UnarySink, WriteFlags,
};
use protocol::*;
use std::sync::Arc;
use tail_reply::TailReplyRegistrar;

#[derive(Clone)]
struct Service(AsyncLog, TailReplyRegistrar);

impl LogStorage for Service {
    fn append(&self, ctx: RpcContext, req: AppendRequest, sink: UnarySink<AppendAck>) {
        self.0
            .append(req.client_id, req.client_request_id, req.payload);
        ctx.spawn(sink.success(AppendAck::new()).map_err(|_| ()));
    }

    fn replies(&self, ctx: RpcContext, req: ReplyRequest, sink: ServerStreamingSink<Reply>) {
        let wf = WriteFlags::default()
            .force_no_compress(true)
            .buffer_hint(false);

        let stream = self
            .1
            .listen(req.client_id)
            .map(move |m| {
                let mut reply = Reply::new();
                reply.set_client_request_ids(m);
                (reply, wf)
            }).map_err(|_| grpcio::Error::RemoteStopped);

        let f = sink.send_all(stream).map(|_| ()).map_err(|_| ());
        ctx.spawn(f);
    }

    fn latest_offset(
        &self,
        ctx: RpcContext,
        _req: LatestOffsetQuery,
        sink: UnarySink<LatestOffsetResult>,
    ) {
        let f = self.0.last_offset().map_err(|_| ()).and_then(move |off| {
            let mut res = LatestOffsetResult::new();
            if let Some(off) = off {
                res.set_offset(off);
            }
            sink.success(res).map_err(|_| ())
        });
        ctx.spawn(f);
    }

    fn query_log(&self, ctx: RpcContext, req: QueryRequest, sink: UnarySink<QueryResult>) {
        let read_limit = ReadLimit::max_bytes(req.max_bytes as usize);
        let f = self
            .0
            .read(req.start_offset, read_limit)
            .map_err(|_| ())
            .and_then(move |b| {
                let mut res = QueryResult::new();
                for m in b.iter() {
                    let mut entry = LogEntry::new();
                    entry.set_offset(m.offset());
                    entry.set_payload(Bytes::from(m.payload()));
                    res.mut_entries().push(entry);
                }

                sink.success(res).map_err(|_| ())
            });
        ctx.spawn(f);
    }
}

pub fn server(
    cfg: &FrontendConfig,
    log: AsyncLog,
    tail: TailReplyRegistrar,
) -> impl Future<Item = (), Error = ()> {
    grpcio::redirect_log();

    let service = create_log_storage(Service(log, tail));
    let env = Arc::new(Environment::new(1));

    let host = cfg.server_addr.ip().to_string();
    let port = cfg.server_addr.port();

    info!("STARTING GRPC SERVER: {}:{}", host, port);

    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(host, port)
        .build()
        .unwrap();
    server.start();

    for &(ref host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }

    WaitFuture(server)
}

struct WaitFuture(GrpcServer);

impl Future for WaitFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        Ok(Async::NotReady)
    }
}
