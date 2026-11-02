//! Runs a [`Tunnel`] against a fake edge: a local QUIC server that plays the
//! edge side of the protocol, one step at a time.

use std::{
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use quinn::{Connection, RecvStream, SendStream};
use rustls::RootCertStore;
use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    ConnectRequest, ConnectionStatus, EdgeSource, Error, Token, Tunnel, TunnelConfig,
    proto::fake_edge, stream::request_bytes, wire,
};

const DEADLINE: Duration = Duration::from_secs(10);
const PREAMBLE_LEN: usize = 8;
const LOCATION: &str = "mrs04";

/// Tags of the RPC `Message` union.
const CALL: u16 = 2;
const UNREGISTER_CONNECTION: u16 = 1;

/// Fails the test instead of hanging it.
async fn within<T>(what: &str, work: impl Future<Output = T>) -> T {
    match tokio::time::timeout(DEADLINE, work).await {
        Ok(done) => done,
        Err(_) => panic!("timed out: {what}"),
    }
}

/// A QUIC server with a self-signed certificate for the name the client dials.
struct FakeEdge {
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
    roots: RootCertStore,
}

impl FakeEdge {
    fn start() -> Self {
        let key = rcgen::KeyPair::generate().expect("key pair");
        let cert = rcgen::CertificateParams::new(vec!["quic.cftunnel.com".to_owned()])
            .expect("certificate params")
            .self_signed(&key)
            .expect("self-signed certificate");
        let der = cert.der().clone();

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3")
            .with_no_client_auth()
            .with_single_cert(
                vec![der.clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .expect("server certificate");
        tls.alpn_protocols = vec![b"argotunnel".to_vec()];

        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).expect("QUIC TLS");
        let config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        let endpoint = quinn::Endpoint::server(config, ([127, 0, 0, 1], 0).into()).expect("bind");

        let mut roots = RootCertStore::empty();
        roots.add(der).expect("trust the fake edge");

        Self {
            addr: endpoint.local_addr().expect("local address"),
            endpoint,
            roots,
        }
    }

    /// Waits for a client to dial and open its control stream.
    async fn accept(&self) -> EdgeConnection {
        let incoming = self.endpoint.accept().await.expect("incoming connection");
        let conn = incoming.await.expect("handshake");
        let (send, recv) = conn.accept_bi().await.expect("control stream");

        EdgeConnection { conn, send, recv }
    }

    fn config(&self) -> TunnelConfig {
        let mut config = TunnelConfig::new(Token::sample());
        config.connections = 1;
        config.edge = EdgeSource::Fixed(vec![self.addr]);
        config.roots = Some(self.roots.clone());

        config
    }
}

/// How the edge answers `registerConnection`.
enum Outcome {
    Registered,
    Refused(&'static str),
}

/// What the client sent in `registerConnection`.
struct Registration {
    conn_index: u8,
    client_id: Uuid,
}

/// The edge's end of one client connection.
struct EdgeConnection {
    conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl EdgeConnection {
    async fn read(&mut self) -> wire::Message {
        wire::read_message(&mut self.recv)
            .await
            .expect("RPC message")
    }

    /// Reads RPC messages until a call arrives. The client also sends `Finish`.
    async fn read_call(&mut self) -> wire::Message {
        loop {
            let message = self.read().await;

            if message.root().expect("root").u16(0) == CALL {
                return message;
            }
        }
    }

    async fn write(&mut self, message: Vec<u8>) {
        self.send.write_all(&message).await.expect("RPC write");
    }

    /// Serves the bootstrap exchange and `registerConnection`.
    async fn register(&mut self, outcome: Outcome) -> Registration {
        self.read().await;
        self.write(fake_edge::bootstrap_answer(0, 7)).await;

        let call = self.read_call().await;
        let body = call.root().unwrap().structure(0).unwrap().unwrap();
        let params = body
            .structure(1)
            .unwrap()
            .unwrap()
            .structure(0)
            .unwrap()
            .unwrap();
        let client = params
            .structure(2)
            .unwrap()
            .unwrap()
            .structure(0)
            .unwrap()
            .unwrap();
        let registration = Registration {
            conn_index: params.u8(0),
            client_id: Uuid::from_slice(client.data_bytes(0).unwrap()).unwrap(),
        };

        let answer = match outcome {
            Outcome::Registered => fake_edge::registered(1, LOCATION, Uuid::new_v4()),
            Outcome::Refused(cause) => fake_edge::refused(1, cause, false),
        };
        self.write(answer).await;

        registration
    }

    /// Serves `unregisterConnection`, then closes the connection as the edge does.
    async fn serve_unregister(&mut self) {
        let call = self.read_call().await;
        let body = call.root().unwrap().structure(0).unwrap().unwrap();
        assert_eq!(body.u16(4), UNREGISTER_CONNECTION, "method");

        self.write(fake_edge::empty_answer(2)).await;

        // Let the answer arrive before the close discards it.
        self.send.finish().unwrap();
        self.send.stopped().await.ok();
        self.conn.close(0u32.into(), b"unregistered");
    }

    /// Opens a request stream as the edge does, and reads the whole response.
    async fn request(&self, dest: &str, metadata: &[(&str, &str)]) -> Response {
        let (mut send, mut recv) = self.conn.open_bi().await.expect("request stream");
        send.write_all(&request_bytes(dest, metadata))
            .await
            .unwrap();
        send.finish().unwrap();

        let mut preamble = [0u8; PREAMBLE_LEN];
        recv.read_exact(&mut preamble)
            .await
            .expect("response preamble");

        let head = wire::read_message(&mut recv).await.expect("response head");
        let root = head.root().unwrap();
        let metadata = root
            .struct_list(1)
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry.text(0).unwrap().to_owned(),
                    entry.text(1).unwrap().to_owned(),
                )
            })
            .collect();

        Response {
            metadata,
            body: recv.read_to_end(1 << 20).await.expect("response body"),
        }
    }
}

struct Response {
    metadata: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn find(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Starts a tunnel and registers its connection on the edge.
async fn connected(edge: &FakeEdge) -> (Tunnel, EdgeConnection, Registration) {
    let (tunnel, (conn, registration)) = within("start", async {
        tokio::join!(Tunnel::start(edge.config()), async {
            let mut conn = edge.accept().await;
            let registration = conn.register(Outcome::Registered).await;

            (conn, registration)
        })
    })
    .await;

    (tunnel.expect("tunnel starts"), conn, registration)
}

/// Answers the next request of `tunnel` with a 200 and `body`.
async fn answer(tunnel: &Tunnel, body: &[u8]) -> ConnectRequest {
    let (request, _body, responder) = tunnel.accept().await.expect("request").split();
    let mut out = responder
        .respond_http(200, [("Content-Type", "text/plain")])
        .await
        .expect("response head");

    out.write_all(body).await.expect("response body");
    out.finish().unwrap();

    request
}

async fn wait_for_status(
    status: &mut watch::Receiver<Vec<ConnectionStatus>>,
    what: &str,
    expected: impl Fn(&ConnectionStatus) -> bool,
) {
    within(what, status.wait_for(|all| expected(&all[0])))
        .await
        .expect("status channel open");
}

#[tokio::test]
async fn serves_a_request_from_the_edge() {
    let edge = FakeEdge::start();
    let (tunnel, conn, registration) = connected(&edge).await;

    assert_eq!(registration.conn_index, 0);

    let (response, request) = within("request", async {
        tokio::join!(
            conn.request("https://h/hello?x=1", &[("HttpMethod", "GET")]),
            answer(&tunnel, b"hello"),
        )
    })
    .await;

    assert_eq!(request.dest, "https://h/hello?x=1");
    assert_eq!(request.http_method(), Some("GET"));
    assert_eq!(response.find("HttpStatus"), Some("200"));
    assert_eq!(response.body, b"hello");

    let current = tunnel.status().borrow()[0].clone();

    assert!(matches!(current, ConnectionStatus::Connected(d) if d.location == LOCATION));
}

#[tokio::test]
async fn reconnects_when_the_edge_drops_the_connection() {
    let edge = FakeEdge::start();
    let (tunnel, first, registration) = connected(&edge).await;
    let mut status = tunnel.status();

    // The edge closes the connection, as it does when it restarts.
    first.conn.close(0u32.into(), b"edge restart");
    wait_for_status(&mut status, "retrying", |s| {
        matches!(s, ConnectionStatus::Retrying { .. })
    })
    .await;

    // The client dials again and registers as the same client and index.
    let mut second = within("redial", edge.accept()).await;
    let again = within("register", second.register(Outcome::Registered)).await;

    assert_eq!(again.client_id, registration.client_id);
    assert_eq!(again.conn_index, registration.conn_index);

    wait_for_status(&mut status, "connected", |s| {
        matches!(s, ConnectionStatus::Connected(_))
    })
    .await;

    // The new connection carries requests.
    let (response, _) = within("request", async {
        tokio::join!(
            second.request("https://h/", &[("HttpMethod", "GET")]),
            answer(&tunnel, b"back"),
        )
    })
    .await;

    assert_eq!(response.body, b"back");
}

#[tokio::test]
async fn retries_when_the_edge_reports_a_duplicate_connection() {
    let edge = FakeEdge::start();

    let (tunnel, _conn) = within("start", async {
        tokio::join!(Tunnel::start(edge.config()), async {
            // The first attempt is refused, the second one is accepted.
            let mut refused = edge.accept().await;
            refused.register(Outcome::Refused("EDUPCONN")).await;

            let mut accepted = edge.accept().await;
            accepted.register(Outcome::Registered).await;

            accepted
        })
    })
    .await;

    assert!(tunnel.is_ok());
}

#[tokio::test]
async fn a_permanent_refusal_ends_start() {
    let edge = FakeEdge::start();

    let (result, _conn) = within("start", async {
        tokio::join!(Tunnel::start(edge.config()), async {
            let mut conn = edge.accept().await;
            conn.register(Outcome::Refused("bad token")).await;

            conn
        })
    })
    .await;

    let error = result.err().expect("start fails");

    assert!(matches!(
        error,
        Error::Refused { cause, should_retry: false, .. } if cause == "bad token"
    ));
}

#[tokio::test]
async fn shutdown_unregisters_and_stops() {
    let edge = FakeEdge::start();
    let (tunnel, mut conn, _) = connected(&edge).await;
    let status = tunnel.status();
    let started = Instant::now();

    within("shutdown", async {
        tokio::join!(
            tunnel.shutdown(Duration::from_secs(3)),
            conn.serve_unregister()
        )
    })
    .await;

    // The edge closed the connection, so the grace period did not run out.
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(status.borrow()[0], ConnectionStatus::Stopped));
}
