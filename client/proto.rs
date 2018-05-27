#![allow(dead_code)]
use futures::{Async, Future, Poll};
use std::io;
use tower_grpc::client::unary::ResponseFuture;
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
