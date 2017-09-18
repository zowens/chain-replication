use commitlog::message::{MessageSet};
use bytes::BytesMut;

pub struct Messages(BytesMut);

impl Messages {
    pub(crate) fn new(bytes: BytesMut) -> Messages {
        Messages(bytes)
    }
}

impl MessageSet for Messages {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}
