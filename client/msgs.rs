use commitlog::message::{Message as LogMsg, MessageSet};
use bytes::BytesMut;

struct ReadonlyMessageSet(BytesMut);

impl MessageSet for ReadonlyMessageSet {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

pub struct Messages(ReadonlyMessageSet);

impl Messages {
    pub(crate) fn new(bytes: BytesMut) -> Messages {
        Messages(ReadonlyMessageSet(bytes))
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Message<'a>> {
        self.0.iter().map(Message)
    }
}

pub struct Message<'a>(LogMsg<'a>);

impl<'a> Message<'a> {
    pub fn offset(&self) -> u64 {
        self.0.offset()
    }

    pub fn payload(&self) -> &[u8] {
        // hide the client_id and client_req_id
        &self.0.payload()[8..]
    }
}
