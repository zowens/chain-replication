mod storage;
mod storage_grpc;

pub use self::storage::*;
pub use self::storage_grpc::{create_log_storage, LogStorage};
