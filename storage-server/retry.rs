use futures::{Async, Future};
use rand::{distributions::Uniform, thread_rng, Rng};
use std::cmp::min;
use std::time::{Duration, Instant};
use std::usize;
use tokio::timer::Delay;

pub struct RetryBehavior {
    max_retries: usize,
    jitter: bool,
    wait_duration: Duration,
    max_backoff: Duration,
    retry: usize,
}

impl RetryBehavior {
    pub fn forever() -> RetryBehavior {
        let mut b = RetryBehavior::default();
        b.max_retries = usize::MAX;
        b
    }

    pub fn limited(retries: usize) -> RetryBehavior {
        let mut b = RetryBehavior::default();
        b.max_retries = retries;
        b
    }

    pub fn disable_jitter(mut self) -> RetryBehavior {
        self.jitter = false;
        self
    }

    pub fn max_backoff(mut self, max_backoff: Duration) -> RetryBehavior {
        self.max_backoff = max_backoff;
        self.wait_duration = min(self.wait_duration, max_backoff);
        self
    }

    pub fn initial_backoff(mut self, initial_backoff: Duration) -> RetryBehavior {
        self.wait_duration = min(initial_backoff, self.max_backoff);
        self
    }

    pub fn retry<F: Future>(self, future: F) -> Retry<F> {
        Retry {
            behavior: self,
            state: RetryState::Running(future),
        }
    }

    fn take(&mut self) -> Option<Duration> {
        if self.retry >= self.max_retries {
            return None;
        }

        self.retry += 1;
        let mut duration = self.wait_duration;
        self.wait_duration = min(self.wait_duration * 2, self.max_backoff);

        if self.jitter {
            duration = thread_rng().sample(Uniform::new(Duration::from_millis(0), duration));
        }

        Some(duration)
    }
}

impl Default for RetryBehavior {
    fn default() -> RetryBehavior {
        RetryBehavior {
            max_retries: 3usize,
            retry: 0,
            jitter: true,
            wait_duration: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }
}

enum RetryState<F: Future> {
    Running(F),
    Waiting(Option<F::Error>, Delay),
}

pub struct Retry<F: Future> {
    behavior: RetryBehavior,
    state: RetryState<F>,
}

pub enum RetryPoll<I, E> {
    FinalResult(I),
    FinalError(E),
    AsyncNotReady,
    Retry(E),
}

impl<F: Future> Retry<F> {
    pub fn set_future(&mut self, future: F) {
        self.state = RetryState::Running(future);
    }

    pub fn poll_next(&mut self) -> RetryPoll<F::Item, F::Error> {
        loop {
            let next_state = match self.state {
                RetryState::Running(ref mut f) => match f.poll() {
                    Ok(Async::Ready(v)) => return RetryPoll::FinalResult(v),
                    Ok(Async::NotReady) => return RetryPoll::AsyncNotReady,
                    Err(e) => match self.behavior.take() {
                        Some(delay) => {
                            RetryState::Waiting(Some(e), Delay::new(Instant::now() + delay))
                        }
                        None => return RetryPoll::FinalError(e),
                    },
                },
                RetryState::Waiting(ref mut retry_err, ref mut delay) => match delay.poll() {
                    Ok(Async::Ready(_)) | Err(_) => {
                        return RetryPoll::Retry(
                            retry_err.take().expect("Multiple poll_next calls on retry"),
                        );
                    }
                    Ok(Async::NotReady) => return RetryPoll::AsyncNotReady,
                },
            };
            self.state = next_state;
        }
    }
}
