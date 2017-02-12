use std::io;

use futures::{Future, Stream, Poll, Async};
use futures::future::{ok, err, FutureResult};
use futures::sync::mpsc;
use tokio_proto::streaming::{Message, Body};
use tokio_proto::streaming::multiplex::ServerProto;
use tokio_core::net::TcpStream;
use tokio_core::io::{Framed, Io};
use tokio_service::{NewService, Service};
use commitlog::message::MessageSet;

use proto::{ReplicationRequestHeaders, ReplicationResponseHeaders, ReplicationServerProtocol};
use asynclog::{Messages, AsyncLog, ReplicationResponse, LogFuture};

#[derive(Default)]
pub struct ReplicationServerProto;

impl ServerProto<TcpStream> for ReplicationServerProto {
    type Request = ReplicationRequestHeaders;
    type RequestBody = ();
    type Response = ReplicationResponseHeaders;
    type ResponseBody = Messages;
    type Error = io::Error;
    type Transport = Framed<TcpStream, ReplicationServerProtocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        try!(io.set_nodelay(true));
        Ok(io.framed(ReplicationServerProtocol::default()))
    }
}

pub struct ReplicationServiceCreator {
    log: AsyncLog,
}

impl ReplicationServiceCreator {
    pub fn new(log: AsyncLog) -> ReplicationServiceCreator {
        ReplicationServiceCreator { log: log }
    }
}

impl NewService for ReplicationServiceCreator {
    type Request = Message<ReplicationRequestHeaders, Body<(), io::Error>>;
    type Response = Message<ReplicationResponseHeaders, ReplicationStream>;
    type Error = io::Error;
    type Instance = ReplicationService;
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(ReplicationService(self.log.clone()))
    }
}


pub struct ReplicationService(AsyncLog);

impl Service for ReplicationService {
    type Request = Message<ReplicationRequestHeaders, Body<(), io::Error>>;
    type Response = Message<ReplicationResponseHeaders, ReplicationStream>;
    type Error = io::Error;
    type Future = StartReplicationFuture;

    fn call(&self, req: Self::Request) -> Self::Future {
        info!("Servicing replication request");

        let offset = match req {
            Message::WithoutBody(ReplicationRequestHeaders::StartFrom(off)) => off,
            Message::WithBody(_, _) => {
                return err(io::Error::new(io::ErrorKind::InvalidInput, "Unexpected body"))
            }
        };

        ok(Message::WithBody(ReplicationResponseHeaders::Replicate,
                             ReplicationStream {
                                 state: MessageStreamState::Reading(self.0.replicate_from(offset)),
                                 log: self.0.clone(),
                             }))
    }
}

pub type StartReplicationFuture = FutureResult<Message<ReplicationResponseHeaders,
                                                       ReplicationStream>,
                                               io::Error>;

pub struct ReplicationStream {
    state: MessageStreamState,
    log: AsyncLog,
}

enum MessageStreamState {
    Reading(LogFuture<ReplicationResponse>),
    StreamingReplication(mpsc::UnboundedReceiver<Messages>),
}

impl Stream for ReplicationStream {
    type Item = Messages;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Messages>, io::Error> {
        loop {
            let resp = match self.state {
                MessageStreamState::Reading(ref mut f) => {
                    trace!("Polling future for reading");
                    try_ready!(f.poll())
                }
                MessageStreamState::StreamingReplication(ref mut s) => {
                    trace!("Polling stream for in-sync replication");
                    return s.poll()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Internal error"));
                }
            };

            match resp {
                ReplicationResponse::Lagging(messages) => {
                    let next_offset = match messages.iter().last() {
                        Some(msg) => msg.offset() + 1,
                        None => {
                            error!("No messages appeared in read log");
                            return Err(io::Error::new(io::ErrorKind::Other,
                                                      "Internal error: read 0 messages"));
                        }
                    };
                    trace!("Not caugh up, initiating additional read");
                    self.state = MessageStreamState::Reading(self.log.replicate_from(next_offset));
                    return Ok(Async::Ready(Some(messages)));
                }
                ReplicationResponse::InSync(stream) => {
                    trace!("Now in sync, setting up stream for replication");
                    self.state = MessageStreamState::StreamingReplication(stream);
                }
            }
        }
    }
}
