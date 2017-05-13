use futures::future::{ok, FutureResult};
use hyper::{Get, StatusCode, Error};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Service, Request, Response};
use prometheus::{self, Encoder, ProtobufEncoder};
use config::MetricsConfig;
use bytes::Bytes;
use hyper::mime::Mime;

#[derive(Clone, Copy)]
struct MetricsService;

impl Service for MetricsService {
    type Request = Request;
    type Response = Response;
    type Error = Error;
    type Future = FutureResult<Response, Error>;

    fn call(&self, req: Request) -> Self::Future {
        ok(match (req.method(), req.path()) {
            (&Get, "/metrics") => {
                let encoder = ProtobufEncoder::new();
                let metric_familys = prometheus::gather();
                let mut buffer = vec![];
                encoder.encode(&metric_familys, &mut buffer).unwrap();

                let body: Bytes = buffer.into();
                Response::new()
                    .with_header(ContentLength(body.len() as u64))
                    .with_header(ContentType(encoder.format_type().parse::<Mime>().unwrap()))
                    .with_body(body)
            },
            _ => {
                Response::new()
                    .with_status(StatusCode::NotFound)
            }
        })
    }

}

pub fn spawn(cfg: MetricsConfig) {
    let server = Http::new().bind(&cfg.server_addr, || Ok(MetricsService)).unwrap();
    server.run().unwrap();
}
