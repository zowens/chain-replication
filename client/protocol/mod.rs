mod storage;
mod storage_grpc;
mod manage;
mod manage_grpc;

pub use self::storage::*;
pub use self::storage_grpc::LogStorageClient;
pub use self::manage::*;
pub use self::manage_grpc::ConfigurationClient;
use bytes::Bytes;
use futures::{Async, Future, Poll, Stream};
use grpcio;
use std::io;

macro_rules! wrap_future {
    ($name:ident, $rpc_ty:ty, $result_ty:ty, $res_var:ident, $map:expr) => {
        pub struct $name(grpcio::Result<grpcio::ClientUnaryReceiver<$rpc_ty>>);

        impl $name {
            pub(crate) fn new(res: grpcio::Result<grpcio::ClientUnaryReceiver<$rpc_ty>>) -> $name {
                $name(res)
            }
        }

        impl Future for $name {
            type Item = $result_ty;
            type Error = io::Error;

            fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
                match &mut self.0 {
                    Ok(ref mut f) => match f.poll() {
                        Ok(Async::Ready($res_var)) => Ok(Async::Ready($map)),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Err(e) => {
                            error!("Error with server: {:?}", e);
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Invalid payload",
                            ))
                        }
                    },
                    Err(e) => {
                        error!("Error with server: {:?}", e);
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid payload",
                        ))
                    }
                }
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
        .map(|LatestOffsetResult_oneof_latest_offset::offset(v)| v)
);

wrap_future!(
    QueryFuture,
    QueryResult,
    Vec<(u64, Bytes)>,
    res,
    res.entries
        .into_vec()
        .into_iter()
        .map(
            |LogEntry {
                 offset, payload, ..
             }| (offset, payload)
        )
        .collect()
);

wrap_future!(AppendSentFuture, AppendAck, (), _res, ());

pub struct ReplyStream(grpcio::ClientSStreamReceiver<Reply>);

impl Stream for ReplyStream {
    type Item = Reply;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Reply>, io::Error> {
        match self.0.poll() {
            Ok(Async::Ready(v)) => Ok(Async::Ready(v)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                error!("Error with server: {:?}", e);
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid payload",
                ))
            }
        }
    }
}

impl From<grpcio::ClientSStreamReceiver<Reply>> for ReplyStream {
    fn from(s: grpcio::ClientSStreamReceiver<Reply>) -> ReplyStream {
        ReplyStream(s)
    }
}

wrap_future!(
    ClientConfigurationFuture,
    ClientConfiguration,
    ClientConfiguration,
    res,
    res);

