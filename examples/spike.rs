//! Dial the edge, open the control stream, call registerConnection.
//! Usage: set -a; . ./.env; set +a; EDGE_ADDR=<ip>:7844 cargo run --example spike

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use base64::Engine;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use cf_tunnel_rs::tunnelrpc_capnp::{connection_response, registration_server};
use serde::Deserialize;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

const SERVER_NAME: &str = "quic.cftunnel.com";
const ALPN: &[u8] = b"argotunnel";

#[derive(Deserialize)]
struct Token {
    a: String,
    s: String,
    t: Uuid,
}

fn root_store() -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    let mut pem = &include_bytes!("../roots/cloudflare-origin-ca.crt")[..];
    for cert in rustls_pemfile_certs(&mut pem)? {
        roots.add(cert)?;
    }
    Ok(roots)
}

fn rustls_pemfile_certs(
    pem: &mut &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    CertificateDer::pem_slice_iter(pem)
        .map(|c| c.context("parse root CA"))
        .collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tokio::task::LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let raw = std::env::var("TUNNEL_TOKEN").context("TUNNEL_TOKEN is not set")?;
    let token: Token =
        serde_json::from_slice(&base64::engine::general_purpose::STANDARD.decode(raw.trim())?)?;
    let secret = base64::engine::general_purpose::STANDARD.decode(&token.s)?;
    let edge: SocketAddr = std::env::var("EDGE_ADDR")
        .context("EDGE_ADDR is not set")?
        .parse()?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(root_store()?)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(1)));
    transport.max_idle_timeout(Some(Duration::from_secs(5).try_into()?));
    transport.initial_mtu(1232);
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
    ));
    config.transport_config(Arc::new(transport));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(config);
    let conn = endpoint
        .connect(edge, SERVER_NAME)?
        .await
        .context("QUIC handshake")?;
    eprintln!("quic: connected to {edge}");

    let started = std::time::Instant::now();
    let watched = conn.clone();
    tokio::task::spawn_local(async move {
        let reason = watched.closed().await;
        eprintln!("quic closed after {:?}: {reason}", started.elapsed());
    });
    let (send, recv) = conn.open_bi().await?;
    let network = twoparty::VatNetwork::new(
        recv.compat(),
        send.compat_write(),
        Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let client: registration_server::Client = rpc.bootstrap(Side::Server);
    tokio::task::spawn_local(rpc);

    let mut request = client.register_connection_request();
    {
        let mut p = request.get();
        let mut auth = p.reborrow().init_auth();
        auth.set_account_tag(token.a.as_str());
        auth.set_tunnel_secret(&secret);
        p.set_tunnel_id(token.t.as_bytes());
        p.set_conn_index(0);
        let mut options = p.init_options();
        let mut info = options.reborrow().init_client();
        info.set_client_id(Uuid::new_v4().as_bytes());
        info.set_version("cf-tunnel-rs-spike");
        info.set_arch("arm64");
        options.set_replace_existing(false);
    }
    let reply = request.send().promise.await.context("registerConnection")?;
    match reply.get()?.get_result()?.get_result().which()? {
        connection_response::result::Which::ConnectionDetails(d) => {
            let d = d?;
            eprintln!(
                "registered: location={} remotely_managed={} uuid={}",
                d.get_location_name()?.to_str()?,
                d.get_tunnel_is_remotely_managed(),
                Uuid::from_slice(d.get_uuid()?)?
            );
        }
        connection_response::result::Which::Error(e) => {
            let e = e?;
            bail!(
                "edge refused: {} (retry={})",
                e.get_cause()?.to_str()?,
                e.get_should_retry()
            );
        }
    }

    let hold: u64 = std::env::var("HOLD_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    tokio::time::sleep(Duration::from_secs(hold)).await;
    eprintln!(
        "still open after {hold}s: {}",
        conn.close_reason().is_none()
    );
    match client.unregister_connection_request().send().promise.await {
        Ok(_) => eprintln!("unregistered"),
        Err(e) => eprintln!("unregister: {e}"),
    }
    conn.close(0u32.into(), b"");
    endpoint.wait_idle().await;
    Ok(())
}
