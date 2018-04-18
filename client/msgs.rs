use bytes::BytesMut;
use commitlog::message::MessageSet;

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
