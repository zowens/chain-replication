use futures::{Future, Stream};
use http::header;
use hyper::server::conn::Http;
use hyper::service::service_fn_ok;
use hyper::{Body, Method, Request, Response, StatusCode};
use prometheus::{self, Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio;
use tokio::net::TcpListener;

fn handle(req: Request<Body>) -> Response<Body> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let encoder = TextEncoder::new();
            let metric_familys = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_familys, &mut buffer).unwrap();

            let len = buffer.len().to_string().parse().unwrap();
            let mut res = Response::new(buffer.into());
            res.headers_mut().insert(header::CONTENT_LENGTH, len);
            res.headers_mut()
                .insert(header::CONTENT_TYPE, encoder.format_type().parse().unwrap());
            res
        }
        _ => {
            let mut res = Response::new(Body::empty());
            *res.status_mut() = StatusCode::NOT_FOUND;
            res
        }
    }
}

pub fn server(addr: &SocketAddr) -> impl Future<Item = (), Error = ()> {
    let listener = TcpListener::bind(addr).expect("unable to bind TCP listener for admin server");
    listener
        .incoming()
        .map_err(|e| error!("accept failed = {:?}", e))
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                warn!("Unable to set nodelay on socket: {}", e);
            }

            let http = Http::new();
            let handle_conn = http
                .serve_connection(sock, service_fn_ok(handle))
                .map_err(|e| error!("{}", e));
            tokio::spawn(handle_conn)
        })
}
