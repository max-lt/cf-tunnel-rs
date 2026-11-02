//! A chat room over WebSocket. Every message goes to every browser.
//! Usage: set -a; . ./.env; set +a; cargo run --features http --example chat
//! Then open the tunnel hostname in two browser tabs.

use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
    routing::get,
};
use cf_tunnel_rs::{Tunnel, TunnelConfig};
use tokio::sync::broadcast::{self, error::RecvError};

const PAGE: &str = include_str!("chat.html");

type Room = broadcast::Sender<String>;

static GUESTS: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let token = std::env::var("TUNNEL_TOKEN")?.parse()?;
    let (room, _) = broadcast::channel(64);

    let app = Router::new()
        .route("/", get(|| async { Html(PAGE) }))
        .route("/ws", get(join))
        .with_state(room);

    let tunnel = Tunnel::start(TunnelConfig::new(token)).await?;
    tunnel.serve(app).await?;

    Ok(())
}

async fn join(upgrade: WebSocketUpgrade, State(room): State<Room>) -> impl IntoResponse {
    upgrade.on_upgrade(|socket| guest(socket, room))
}

/// Relays the room to one browser, and its messages to the room.
async fn guest(mut socket: WebSocket, room: Room) {
    let name = format!("guest {}", GUESTS.fetch_add(1, Ordering::Relaxed) + 1);

    // Subscribe first, so the guest sees its own announcement.
    let mut inbox = room.subscribe();
    let _ = room.send(format!("{name} joined"));

    loop {
        tokio::select! {
            // A message of the room goes to the browser.
            message = inbox.recv() => match message {
                Ok(message) => {
                    if socket.send(Message::text(message)).await.is_err() {
                        break;
                    }
                }

                // A slow browser misses some messages and keeps going.
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => break,
            },

            // A message of the browser goes to the room.
            message = socket.recv() => match message {
                Some(Ok(Message::Text(text))) if !text.trim().is_empty() => {
                    let _ = room.send(format!("{name}: {}", text.trim()));
                }

                // After a close frame, `recv` sends the reply and then returns `None`.
                Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }

    let _ = room.send(format!("{name} left"));
}
