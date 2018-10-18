use super::log_reader::FileSlice;
use super::poll::UpstreamReplication;
use asynclog::ReplicatorAsyncLog;
use configuration::{NodeConfigFuture, NodeManager};
use either::Either;
use futures::future::Either as EitherFut;
use futures::future::Select2;
use futures::stream::StreamFuture;
use futures::{Async, Future, Poll, Stream};
use std::mem;
use std::time::Instant;
use tokio::timer::Delay;

// use tokio::timer::Deadline

enum ControllerState {
    WaitingForAssignment(RepollFuture, ReplicatorAsyncLog<FileSlice>),
    Replicating(Select2<RepollFuture, UpstreamReplication>),
    Empty,
}

pub struct ReplicationController {
    manager: NodeManager,
    state: ControllerState,
}

impl ReplicationController {
    pub fn new(manager: NodeManager, log: ReplicatorAsyncLog<FileSlice>) -> ReplicationController {
        let repoll = Repoll::new(&manager).into_future();
        let state = {
            let current_config = manager.current();
            match current_config.upstream_addr() {
                Some(addr) => ControllerState::Replicating(
                    repoll.select2(UpstreamReplication::new(&addr, log)),
                ),
                None => ControllerState::WaitingForAssignment(repoll, log),
            }
        };
        ReplicationController { manager, state }
    }
}

impl Future for ReplicationController {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            let state = mem::replace(&mut self.state, ControllerState::Empty);
            self.state = match state {
                ControllerState::Empty => unreachable!("Reached Empty state"),
                ControllerState::WaitingForAssignment(mut repoll, log) => match repoll.poll() {
                    Ok(Async::Ready((_, repoll_stream))) => {
                        match self.manager.current().upstream_addr() {
                            Some(addr) => {
                                info!("Assigned upstream address={}", addr);
                                ControllerState::Replicating(
                                    repoll_stream
                                        .into_future()
                                        .select2(UpstreamReplication::new(&addr, log)),
                                )
                            }
                            None => ControllerState::WaitingForAssignment(repoll, log),
                        }
                    }
                    Ok(Async::NotReady) => {
                        self.state = ControllerState::WaitingForAssignment(repoll, log);
                        return Ok(Async::NotReady);
                    }
                    Err(_) => return Err(()),
                },
                ControllerState::Replicating(mut select_future) => match select_future.poll() {
                    Ok(Async::Ready(EitherFut::A(((_, repoll_stream), replication)))) => {
                        let repoll = repoll_stream.into_future();

                        // check if upstream needs to be changed
                        match self.manager.current().upstream_addr() {
                            Some(addr) if addr == replication.address() => {
                                trace!("No change in upstream replication address");
                                ControllerState::Replicating(repoll.select2(replication))
                            }
                            Some(addr) => {
                                info!("Replication changed to address={}", addr);
                                ControllerState::Replicating(repoll.select2(
                                    UpstreamReplication::new(&addr, replication.into_async_log()),
                                ))
                            }
                            None => {
                                info!("Removing upstream replication, upgraded to head node");
                                ControllerState::WaitingForAssignment(
                                    repoll,
                                    replication.into_async_log(),
                                )
                            }
                        }
                    }
                    Ok(Async::Ready(EitherFut::B(_))) => {
                        error!("Error replicating, future ended");
                        unreachable!("Replication should not have ended");
                    }
                    Ok(Async::NotReady) => {
                        self.state = ControllerState::Replicating(select_future);
                        return Ok(Async::NotReady);
                    }
                    Err(_) => return Err(()),
                },
            }
        }
    }
}

type RepollFuture = StreamFuture<Repoll>;

struct Repoll {
    manager: NodeManager,
    state: Either<Delay, NodeConfigFuture>,
}

impl Repoll {
    fn new(manager: &NodeManager) -> Repoll {
        let delay = manager.current().wait_duration();
        Repoll {
            manager: manager.clone(),
            state: Either::Left(Delay::new(Instant::now() + delay)),
        }
    }
}

impl Stream for Repoll {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<()>, ()> {
        loop {
            let next_state = match self.state {
                Either::Left(ref mut delay) => match delay.poll() {
                    Ok(Async::Ready(_)) => Either::Right(self.manager.repoll()),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(_) => return Err(()),
                },
                Either::Right(ref mut reconfig) => match reconfig.poll() {
                    Ok(Async::Ready(_)) => {
                        let delay = self.manager.current().wait_duration();
                        Either::Left(Delay::new(Instant::now() + delay))
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(_) => return Err(()),
                },
            };
            self.state = next_state;
        }
    }
}
