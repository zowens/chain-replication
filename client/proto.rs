#![allow(dead_code)]
use futures::{Async, Future, Poll, Stream};
use std::io;
use tower_grpc::client::server_streaming::ResponseFuture as StreamResponseFuture;
use tower_grpc::client::unary::ResponseFuture;
use tower_grpc::Streaming;
use tower_h2::{self, RecvBody};

include!(concat!(env!("OUT_DIR"), "/chainreplication.rs"));

macro_rules! wrap_future {
    ($name:ident, $rpc_ty:ty, $result_ty:ty, $res_var:ident, $map:expr) => {
        pub struct $name(ResponseFuture<$rpc_ty, tower_h2::client::ResponseFuture, RecvBody>);

        impl Future for $name {
            type Item = $result_ty;
            type Error = io::Error;

            fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
                match self.0.poll() {
                    Ok(Async::Ready(res)) => {
                        let $res_var = res.into_inner();
                        Ok(Async::Ready($map))
                    }
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(e) => {
                        // TODO: better error mapping here
                        error!("Error with server: {:?}", e);
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid payload",
                        ))
                    }
                }
            }
        }

        impl From<ResponseFuture<$rpc_ty, tower_h2::client::ResponseFuture, RecvBody>> for $name {
            fn from(
                f: ResponseFuture<$rpc_ty, tower_h2::client::ResponseFuture, RecvBody>,
            ) -> $name {
                $name(f)
            }
        }
    };
}

wrap_future!(
    LatestOffsetFuture,
    LatestOffsetResult,
    Option<u64>,
    res,
    res.latest_offset
        .map(|latest_offset_result::LatestOffset::Offset(v)| v)
);

wrap_future!(
    QueryFuture,
    QueryResult,
    Vec<(u64, Vec<u8>)>,
    res,
    res.entries
        .into_iter()
        .map(|LogEntry { offset, payload }| (offset, payload))
        .collect()
);

wrap_future!(AppendSentFuture, AppendAck, (), _res, ());

enum ReplyStreamInner {
    Starting(StreamResponseFuture<Reply, tower_h2::client::ResponseFuture>),
    Streaming(Streaming<Reply>),
}

pub struct ReplyStream(ReplyStreamInner);

impl Stream for ReplyStream {
    type Item = Reply;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Reply>, Self::Error> {
        loop {
            let streaming = match self.0 {
                ReplyStreamInner::Starting(ref mut f) => try_ready!(f.poll().map_err(|e| {
                    error!("Error with server: {:?}", e);
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid payload")
                })),
                ReplyStreamInner::Streaming(ref mut s) => match s.poll() {
                    Ok(Async::Ready(s)) => return Ok(Async::Ready(s)),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => {
                        error!("Error with server: {:?}", e);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid payload",
                        ));
                    }
                },
            };
            debug!("Now streaming replies");
            self.0 = ReplyStreamInner::Streaming(streaming.into_inner());
        }
    }
}

impl From<StreamResponseFuture<Reply, tower_h2::client::ResponseFuture>> for ReplyStream {
    fn from(f: StreamResponseFuture<Reply, tower_h2::client::ResponseFuture>) -> ReplyStream {
        ReplyStream(ReplyStreamInner::Starting(f))
    }
}
