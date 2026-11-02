//! Serves an axum app through the tunnel.
//! Usage: set -a; . ./.env; set +a; cargo run --features http --example hello

use std::time::Duration;

use axum::{Router, routing::get};
use cf_tunnel_rs::{Tunnel, TunnelConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let token = std::env::var("TUNNEL_TOKEN")?.parse()?;
    let app = Router::new().route("/", get(|| async { "Hello from cf-tunnel-rs\n" }));

    let tunnel = Tunnel::start(TunnelConfig::new(token)).await?;

    // Serve until Ctrl-C, then unregister.
    tokio::select! {
        result = tunnel.serve(app) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }

    tunnel.shutdown(Duration::from_secs(3)).await;

    Ok(())
}
