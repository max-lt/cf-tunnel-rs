use std::{net::SocketAddr, sync::Arc, time::Duration};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use quinn::{RecvStream, SendStream};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio::sync::oneshot;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

use crate::{
    Error, IncomingStream, Token,
    tunnelrpc_capnp::{connection_response, registration_server},
};

const SERVER_NAME: &str = "quic.cftunnel.com";
const ALPN: &[u8] = b"argotunnel";
const ROOTS: &[u8] = include_bytes!("../roots/cloudflare-origin-ca.crt");

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub token: Token,
    /// Edge address, usually port 7844.
    pub edge: SocketAddr,
    /// Index of this connection among the tunnel's connections.
    pub conn_index: u8,
    /// Reported to the edge as the client version.
    pub version: String,
}

impl ClientConfig {
    pub fn new(token: Token, edge: SocketAddr) -> Self {
        Self {
            token,
            edge,
            conn_index: 0,
            version: concat!("cf-tunnel-rs-", env!("CARGO_PKG_VERSION")).to_owned(),
        }
    }
}

/// Where the edge registered this connection.
#[derive(Debug, Clone)]
pub struct ConnectionDetails {
    pub uuid: Uuid,
    /// Colo code, for example `mrs04`.
    pub location: String,
    pub remotely_managed: bool,
}

/// One registered connection to the edge.
pub struct TunnelClient {
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    details: ConnectionDetails,
    shutdown: Option<oneshot::Sender<()>>,
    done: Option<oneshot::Receiver<()>>,
}

impl TunnelClient {
    /// Dials the edge and registers the connection.
    pub async fn connect(config: ClientConfig) -> Result<Self, Error> {
        let mut endpoint = quinn::Endpoint::client(match config.edge {
            SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
            SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
        })?;
        endpoint.set_default_client_config(quic_config()?);
        let conn = endpoint.connect(config.edge, SERVER_NAME)?.await?;
        let (send, recv) = conn.open_bi().await?;

        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        // capnp-rpc is not Send, so the control stream runs on its own thread.
        std::thread::Builder::new()
            .name("cf-tunnel-control".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build control runtime");
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(control(
                    send,
                    recv,
                    config,
                    ready_tx,
                    shutdown_rx,
                )));
                let _ = done_tx.send(());
            })?;

        let details = ready_rx
            .await
            .map_err(|_| Error::Protocol("control thread ended before registration".into()))??;
        Ok(Self {
            endpoint,
            conn,
            details,
            shutdown: Some(shutdown_tx),
            done: Some(done_rx),
        })
    }

    pub fn details(&self) -> &ConnectionDetails {
        &self.details
    }

    /// Waits for the next request stream from the edge.
    pub async fn accept(&self) -> Result<IncomingStream, Error> {
        loop {
            let (send, recv) = self.conn.accept_bi().await?;
            match IncomingStream::read(send, recv).await {
                Ok(Some(stream)) => return Ok(stream),
                Ok(None) => tracing::debug!("dropped edge RPC stream"),
                Err(e) => tracing::debug!("dropped malformed stream: {e}"),
            }
        }
    }

    /// Resolves when the connection ends.
    pub async fn closed(&self) -> Error {
        self.conn.closed().await.into()
    }

    /// Unregisters from the edge and closes the connection.
    pub async fn shutdown(mut self) {
        drop(self.shutdown.take());
        if let Some(done) = self.done.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), done).await;
        }
        self.conn.close(0u32.into(), b"");
        self.endpoint.wait_idle().await;
    }
}

impl Drop for TunnelClient {
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"");
    }
}

fn quic_config() -> Result<quinn::ClientConfig, Error> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ROOTS) {
        let cert = cert.map_err(|e| Error::Tls(e.to_string()))?;
        roots.add(cert).map_err(|e| Error::Tls(e.to_string()))?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| Error::Tls(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(1)));
    transport.max_idle_timeout(Some(
        Duration::from_secs(5)
            .try_into()
            .expect("5 s fits an idle timeout"),
    ));
    transport.initial_mtu(1232);
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| Error::Tls(e.to_string()))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));
    Ok(config)
}

/// Registers the connection, reports the result, and unregisters on shutdown.
async fn control(
    send: SendStream,
    recv: RecvStream,
    config: ClientConfig,
    ready: oneshot::Sender<Result<ConnectionDetails, Error>>,
    shutdown: oneshot::Receiver<()>,
) {
    let network = twoparty::VatNetwork::new(
        recv.compat(),
        send.compat_write(),
        Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let client: registration_server::Client = rpc.bootstrap(Side::Server);
    tokio::task::spawn_local(rpc);

    let _ = ready.send(register(&client, &config).await);
    let _ = shutdown.await;
    let _ = client.unregister_connection_request().send().promise.await;
}

async fn register(
    client: &registration_server::Client,
    config: &ClientConfig,
) -> Result<ConnectionDetails, Error> {
    let mut request = client.register_connection_request();
    {
        let mut params = request.get();
        let mut auth = params.reborrow().init_auth();
        auth.set_account_tag(config.token.account_tag.as_str());
        auth.set_tunnel_secret(&config.token.secret);
        params.set_tunnel_id(config.token.tunnel_id.as_bytes());
        params.set_conn_index(config.conn_index);
        let mut options = params.init_options();
        let mut info = options.reborrow().init_client();
        info.set_client_id(Uuid::new_v4().as_bytes());
        info.set_version(config.version.as_str());
        info.set_arch(std::env::consts::ARCH);
    }
    let reply = request.send().promise.await?;
    match reply.get()?.get_result()?.get_result().which()? {
        connection_response::result::Which::ConnectionDetails(details) => {
            let details = details?;
            Ok(ConnectionDetails {
                uuid: Uuid::from_slice(details.get_uuid()?)
                    .map_err(|e| Error::Protocol(e.to_string()))?,
                location: details.get_location_name()?.to_str()?.to_owned(),
                remotely_managed: details.get_tunnel_is_remotely_managed(),
            })
        }
        connection_response::result::Which::Error(error) => {
            let error = error?;
            Err(Error::Refused {
                cause: error.get_cause()?.to_str()?.to_owned(),
                retry_after: Duration::from_nanos(error.get_retry_after().max(0) as u64),
                should_retry: error.get_should_retry(),
            })
        }
    }
}
