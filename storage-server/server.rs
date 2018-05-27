use asynclog::{AsyncLog, LogFuture};
use commitlog::{message::MessageBuf, Offset, ReadLimit};
use futures::{
    self, future::{ok, FutureResult}, Async, Future, Poll, Stream,
};
use h2;
use message_batch::MessageBatcher;
use std::io;
use std::marker::PhantomData;
use tail_reply::{ReplyStream, TailReplyRegistrar};
use tokio::{executor::current_thread::spawn, net::TcpListener};
use tower_grpc::{self, Error, Request, Response};
use tower_h2::{server::Background, Server};
use tower_service;

use self::storage::server::*;
use self::storage::*;
use config::FrontendConfig;

#[derive(Clone)]
struct Service(MessageBatcher, AsyncLog, TailReplyRegistrar);

impl LogStorage for Service {
    type AppendFuture = FutureResult<Response<AppendAck>, Error>;
    type RepliesStream = TailReplyMap;
    type RepliesFuture = FutureResult<Response<Self::RepliesStream>, Error>;
    type LatestOffsetFuture = TowerMap<LogFuture<Option<Offset>>, LatestOffsetResult>;
    type QueryLogFuture = TowerMap<LogFuture<MessageBuf>, QueryResult>;

    fn append(&mut self, request: Request<AppendRequest>) -> Self::AppendFuture {
        let request = request.into_inner();
        self.0.push(
            request.client_id,
            request.client_request_id,
            request.payload,
        );
        ok(Response::new(AppendAck {}))
    }

    fn replies(&mut self, request: Request<ReplyRequest>) -> Self::RepliesFuture {
        let reply_stream = self.2.listen(request.get_ref().client_id);
        ok(Response::new(TailReplyMap(reply_stream)))
    }

    fn latest_offset(&mut self, _request: Request<LatestOffsetQuery>) -> Self::LatestOffsetFuture {
        TowerMap::new(self.1.last_offset())
    }

    fn query_log(&mut self, request: Request<QueryRequest>) -> Self::QueryLogFuture {
        let &QueryRequest {
            start_offset,
            max_bytes,
        } = request.get_ref();
        let read_limit = ReadLimit::max_bytes(max_bytes as usize);
        TowerMap::new(self.1.read(start_offset, read_limit))
    }
}

pub fn server(
    cfg: &FrontendConfig,
    log: AsyncLog,
    tail: TailReplyRegistrar,
) -> impl Future<Item = (), Error = ()> {
    let listener = TcpListener::bind(&cfg.server_addr)
        .expect("unable to bind TCP listener for replication server");

    let message_batcher = MessageBatcher::new(log.clone(), cfg.batch_wait_ms);
    let new_service = LogStorageServer::new(Service(message_batcher, log, tail));
    let executor = ExecutorAdapter;
    let h2 = Server::new(new_service, Default::default(), executor);
    listener
        .incoming()
        .fold(h2, |h2, sock| {
            if let Err(e) = sock.set_nodelay(true) {
                error!("Error setting nodelay: {}", e);
                return Err(e);
            }

            let serve = h2.serve(sock).map_err(|e| error!("h2 error: {:?}", e));
            spawn(serve);
            Ok(h2)
        })
        .map_err(|e| error!("Storage server error: {}", e))
        .map(|_| ())
}

#[derive(Clone)]
struct ExecutorAdapter;
type BackgroundFuture = Background<
    <LogStorageServer<Service> as tower_service::Service>::Future,
    self::storage::server::log_storage::ResponseBody<Service>,
>;
impl futures::future::Executor<BackgroundFuture> for ExecutorAdapter {
    fn execute(
        &self,
        f: BackgroundFuture,
    ) -> Result<(), futures::future::ExecuteError<BackgroundFuture>> {
        // TODO: remove this hacky-ness around the various Executor traits
        spawn(Box::new(f));
        Ok(())
    }
}

struct TowerMap<F: Future, O> {
    future: F,
    _o: PhantomData<O>,
}

impl<F: Future, O> TowerMap<F, O> {
    pub fn new(future: F) -> TowerMap<F, O> {
        TowerMap {
            future,
            _o: PhantomData,
        }
    }
}

impl<F, O> Future for TowerMap<F, O>
where
    F: Future<Error = io::Error>,
    F::Item: Into<O>,
{
    type Item = Response<O>;
    type Error = tower_grpc::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.future.poll() {
            Ok(Async::Ready(v)) => Ok(Async::Ready(Response::new(v.into()))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                let e: h2::Error = e.into();
                Err(e.into())
            }
        }
    }
}

struct TailReplyMap(ReplyStream);

impl Stream for TailReplyMap {
    type Item = Reply;
    type Error = tower_grpc::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.0.poll() {
            Ok(Async::Ready(Some(client_request_ids))) => {
                Ok(Async::Ready(Some(Reply { client_request_ids })))
            }
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(()) => Err(Error::Inner(())),
        }
    }
}

#[allow(dead_code)]
mod storage {
    use commitlog::message::{MessageBuf, MessageSet};
    use commitlog::Offset;

    include!(concat!(env!("OUT_DIR"), "/chainreplication.rs"));

    impl From<Option<Offset>> for LatestOffsetResult {
        fn from(offset: Option<Offset>) -> LatestOffsetResult {
            LatestOffsetResult {
                latest_offset: offset.map(latest_offset_result::LatestOffset::Offset),
            }
        }
    }

    impl From<MessageBuf> for QueryResult {
        fn from(buf: MessageBuf) -> QueryResult {
            QueryResult {
                entries: buf
                    .iter()
                    .map(|m| LogEntry {
                        offset: m.offset(),
                        payload: m.payload().to_vec(),
                    })
                    .collect(),
            }
        }
    }
}
