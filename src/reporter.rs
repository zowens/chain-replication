use metrics::metrics::Metric;
use metrics::reporter::Reporter;
use std::time::Duration;
use std::thread;
use std::sync::mpsc;
use std::collections::HashMap;

enum ReporterMsg {
    AddMetric(String, Metric, Option<HashMap<String, String>>),
    RemoveMetric(String),
}

pub struct LogReporter {
    metrics: mpsc::Sender<Result<ReporterMsg, &'static str>>,
    join_handle: thread::JoinHandle<Result<(), String>>,
}

impl Reporter for LogReporter {
    fn get_unique_reporter_name(&self) -> &str {
        "log_reporter"
    }
    fn stop(self) -> Result<thread::JoinHandle<Result<(), String>>, String> {
        match self.metrics.send(Err("stop")) {
            Ok(_) => Ok(self.join_handle),
            Err(x) => Err(format!("Unable to stop reporter: {}", x)),
        }
    }
    fn addl<S: Into<String>>(&mut self,
                             name: S,
                             metric: Metric,
                             labels: Option<HashMap<String, String>>)
                             -> Result<(), String> {
        match self.metrics.send(Ok(ReporterMsg::AddMetric(name.into(), metric, labels))) {
            Ok(_) => Ok(()),
            Err(x) => Err(format!("Unable to send metric reporter{}", x)),
        }
    }
    fn remove<S: Into<String>>(&mut self, name: S) -> Result<(), String> {
        match self.metrics
            .send(Ok(ReporterMsg::RemoveMetric(name.into()))) {
            Ok(_) => Ok(()),
            Err(x) => Err(format!("Unable to remove metric reporter{}", x)),
        }
    }
}

impl LogReporter {
    pub fn new(delay_ms: u64) -> Self {
        let (tx, rx) = mpsc::channel();
        LogReporter {
            metrics: tx,
            join_handle: thread::spawn(move || {
                let mut metrics = HashMap::new();

                loop {

                    // try to get new metrics
                    loop {
                        match rx.try_recv() {
                            Ok(Ok(ReporterMsg::AddMetric(name, metric, labels))) => {
                                metrics.insert(name, (metric, labels));
                            }
                            Ok(Ok(ReporterMsg::RemoveMetric(name))) => {
                                metrics.remove(&name);
                            }
                            Ok(Err(e)) => {
                                info!("Stopping reporter because...: {}", e);
                                return Ok(());
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }

                    for (name, &(ref metric, ref labels)) in metrics.iter() {
                        match *metric {
                            Metric::Meter(ref x) => {
                                info!("{} ({:?}) {:?}", name, labels, x.snapshot());
                            }
                            Metric::Gauge(ref x) => {
                                info!("{} ({:?}) {:?}", name, labels, x.snapshot());
                            }
                            Metric::Counter(ref x) => {
                                info!("{} ({:?}) {:?}", name, labels, x.snapshot());
                            }
                            Metric::Histogram(ref x) => {
                                info!("{} ({:?}) {:?}", name, labels, x);
                            }
                        }
                    }

                    thread::sleep(Duration::from_millis(delay_ms));
                }
            }),
        }
    }
}
