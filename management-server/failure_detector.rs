use chain::Chain;
use futures::{Async, Future, Poll};
use std::time::{Duration, Instant};
use tokio::timer::Delay;

const WAIT_DURATION: Duration = Duration::from_secs(1);

/// Failure detector that runs every second to run the
/// node reaping process on the chain
pub struct FailureDetector {
    wait: Delay,
    chain: Chain,
}

impl FailureDetector {
    pub fn new(chain: Chain) -> FailureDetector {
        FailureDetector {
            wait: Delay::new(Instant::now() + WAIT_DURATION),
            chain,
        }
    }
}

impl Future for FailureDetector {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            match self.wait.poll() {
                Ok(Async::Ready(_)) => {}
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(_) => return Err(()),
            };
            self.chain.node_reaper();
            let new_inst = Instant::now() + WAIT_DURATION;
            self.wait.reset(new_inst);
        }
    }
}
