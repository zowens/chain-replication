use hyper::header::ContentType;
use hyper::server::{Server, Request, Response};
use hyper::mime::Mime;

use config::MetricsConfig;
use prometheus::{self, Encoder, ProtobufEncoder};

pub fn spawn(cfg: MetricsConfig) {
    let encoder = ProtobufEncoder::new();
    Server::http(&cfg.server_addr)
        .unwrap()
        .handle(move |_: Request, mut res: Response| {
                    let metric_familys = prometheus::gather();
                    let mut buffer = vec![];
                    encoder.encode(&metric_familys, &mut buffer).unwrap();
                    res.headers_mut()
                        .set(ContentType(encoder.format_type().parse::<Mime>().unwrap()));
                    res.send(&buffer).unwrap();
                })
        .unwrap();
}
