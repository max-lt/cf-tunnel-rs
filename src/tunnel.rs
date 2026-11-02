use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    sync::{Mutex, mpsc, watch},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::edge::EdgeRegions;
use crate::{ClientConfig, ConnectionDetails, Error, IncomingStream, Token, TunnelClient};

const DEFAULT_CONNECTIONS: u8 = 4;
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// A connection that lived this long counts as healthy, so its backoff restarts.
const STABLE_AFTER: Duration = Duration::from_secs(30);
const STREAM_QUEUE: usize = 64;

/// Where the edge addresses come from.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EdgeSource {
    /// Resolve the edge through its SRV records.
    Discover,
    /// Use these addresses and skip discovery.
    Fixed(Vec<SocketAddr>),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TunnelConfig {
    pub token: Token,
    /// Number of connections to keep open. cloudflared uses 4.
    pub connections: u8,
    pub edge: EdgeSource,
    /// Identifies this process to the edge. All connections share it.
    pub client_id: Uuid,
    /// Reported to the edge as the client version.
    pub version: String,
}

impl TunnelConfig {
    pub fn new(token: Token) -> Self {
        // The per-connection defaults come from ClientConfig.
        let base = ClientConfig::new(token, SocketAddr::from(([0, 0, 0, 0], 0)));

        Self {
            token: base.token,
            connections: DEFAULT_CONNECTIONS,
            edge: EdgeSource::Discover,
            client_id: base.client_id,
            version: base.version,
        }
    }
}

/// State of one connection of a [`Tunnel`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConnectionStatus {
    Connecting,
    Connected(ConnectionDetails),
    /// The last attempt failed. The next one starts after `after`.
    Retrying {
        error: String,
        after: Duration,
    },
    /// The edge refused the connection for good, for example on a bad token.
    Failed(String),
    Stopped,
}

/// A set of connections to the edge. It reconnects each one with a backoff and
/// merges their request streams into one queue.
pub struct Tunnel {
    incoming: Mutex<mpsc::Receiver<IncomingStream>>,
    status: watch::Receiver<Vec<ConnectionStatus>>,
    stop: watch::Sender<Option<Duration>>,
    tasks: Vec<JoinHandle<()>>,
}

struct Shared {
    config: TunnelConfig,
    regions: EdgeRegions,
    status: watch::Sender<Vec<ConnectionStatus>>,
    streams: mpsc::Sender<IncomingStream>,
    first: mpsc::UnboundedSender<Result<(), Error>>,
    /// The edge address each connection uses, so two never share one.
    in_use: std::sync::Mutex<Vec<Option<SocketAddr>>>,
}

impl Shared {
    fn set(&self, index: usize, status: ConnectionStatus) {
        self.status.send_modify(|all| all[index] = status);
    }

    /// Takes an address no other connection uses, and reserves it.
    fn reserve_address(&self, index: usize, attempt: usize) -> Option<SocketAddr> {
        let mut in_use = self.in_use.lock().expect("in_use lock");
        let taken: Vec<SocketAddr> = in_use
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .filter_map(|(_, addr)| *addr)
            .collect();

        let edge = self.regions.pick(index, attempt, &taken);
        in_use[index] = edge;

        edge
    }

    fn client_config(&self, index: usize, edge: SocketAddr) -> ClientConfig {
        let mut config = ClientConfig::new(self.config.token.clone(), edge);
        config.conn_index = index as u8;
        config.client_id = self.config.client_id;
        config.version = self.config.version.clone();

        config
    }
}

impl Tunnel {
    /// Starts the connections and waits until one is registered. A transient
    /// failure keeps retrying in the background, so wrap this call in a
    /// timeout if the caller cannot wait. A permanent refusal, such as a bad
    /// token, returns its error.
    pub async fn start(config: TunnelConfig) -> Result<Self, Error> {
        let regions = match &config.edge {
            EdgeSource::Discover => EdgeRegions::discover().await?,
            EdgeSource::Fixed(addrs) => EdgeRegions::fixed(addrs.clone()),
        };

        if regions.is_empty() {
            return Err(Error::Dns("no edge address".into()));
        }

        let count = config.connections.max(1) as usize;
        let (streams, incoming) = mpsc::channel(STREAM_QUEUE);
        let (status_tx, status) = watch::channel(vec![ConnectionStatus::Connecting; count]);
        let (stop, stop_rx) = watch::channel(None);
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();

        let shared = Arc::new(Shared {
            config,
            regions,
            status: status_tx,
            streams,
            first: first_tx,
            in_use: std::sync::Mutex::new(vec![None; count]),
        });

        let tasks = (0..count)
            .map(|index| tokio::spawn(supervise(index, shared.clone(), stop_rx.clone())))
            .collect();

        let tunnel = Self {
            incoming: Mutex::new(incoming),
            status,
            stop,
            tasks,
        };

        // The first connection to register, or to fail for good, decides.
        drop(shared);
        match first_rx.recv().await {
            Some(Ok(())) => Ok(tunnel),
            Some(Err(e)) => Err(e),
            None => Err(Error::Stopped),
        }
    }

    /// Waits for the next request stream from any connection. Safe to cancel.
    /// Fails once every connection has stopped.
    pub async fn accept(&self) -> Result<IncomingStream, Error> {
        self.incoming
            .lock()
            .await
            .recv()
            .await
            .ok_or(Error::Stopped)
    }

    /// The state of each connection, in connection index order.
    pub fn status(&self) -> watch::Receiver<Vec<ConnectionStatus>> {
        self.status.clone()
    }

    /// Unregisters every connection, then waits up to `grace` for each edge to
    /// close it. Requests in flight keep running during the wait.
    pub async fn shutdown(mut self, grace: Duration) {
        self.stop.send_replace(Some(grace));

        for task in std::mem::take(&mut self.tasks) {
            let _ = task.await;
        }
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        // Without shutdown() the supervisors would reconnect forever.
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// How one attempt to keep a connection up ended.
enum Attempt {
    /// A stop request ended it.
    Stopped,
    /// It failed, and the next attempt can work. `stable` says the connection
    /// lived long enough to restart the backoff.
    Failed {
        error: Error,
        retry_after: Duration,
        stable: bool,
    },
    /// The edge will refuse again, so retrying would only add noise.
    Refused(Error),
}

/// Keeps connection `index` up until a stop is requested or the edge refuses it.
async fn supervise(index: usize, shared: Arc<Shared>, mut stop: watch::Receiver<Option<Duration>>) {
    let mut attempt = 0;

    while stop.borrow().is_none() {
        let (error, retry_after, stable) = match run_once(index, attempt, &shared, &mut stop).await
        {
            Attempt::Stopped => break,
            Attempt::Refused(error) => {
                shared.set(index, ConnectionStatus::Failed(error.to_string()));
                let _ = shared.first.send(Err(error));

                return;
            }
            Attempt::Failed {
                error,
                retry_after,
                stable,
            } => (error, retry_after, stable),
        };

        if stable {
            attempt = 0;
        }

        let after = jitter(backoff(attempt).max(retry_after));
        shared.set(
            index,
            ConnectionStatus::Retrying {
                error: error.to_string(),
                after,
            },
        );
        attempt += 1;

        // Wait out the backoff, unless a stop request arrives first.
        tokio::select! {
            _ = tokio::time::sleep(after) => {}
            _ = stop.changed() => break,
        }
    }

    shared.set(index, ConnectionStatus::Stopped);
}

/// Dials the edge once and serves the connection until it ends.
async fn run_once(
    index: usize,
    attempt: usize,
    shared: &Shared,
    stop: &mut watch::Receiver<Option<Duration>>,
) -> Attempt {
    let Some(edge) = shared.reserve_address(index, attempt) else {
        return Attempt::Refused(Error::Dns("no edge address".into()));
    };

    shared.set(index, ConnectionStatus::Connecting);

    // Dial, unless a stop request arrives first.
    let connected = tokio::select! {
        result = TunnelClient::connect(shared.client_config(index, edge)) => result,
        _ = stop.changed() => return Attempt::Stopped,
    };

    let client = match connected {
        Ok(client) => client,

        // Another connection holds this address. The next attempt picks a new one.
        Err(error) if error.is_duplicate() => {
            return Attempt::Failed {
                error,
                retry_after: Duration::ZERO,
                stable: false,
            };
        }

        Err(
            error @ Error::Refused {
                should_retry: false,
                ..
            },
        ) => return Attempt::Refused(error),

        Err(error) => {
            return Attempt::Failed {
                retry_after: error.retry_after(),
                error,
                stable: false,
            };
        }
    };

    shared.set(index, ConnectionStatus::Connected(client.details().clone()));
    let _ = shared.first.send(Ok(()));

    let started = Instant::now();

    match forward(client, &shared.streams, stop).await {
        None => Attempt::Stopped,
        Some(error) => Attempt::Failed {
            error,
            retry_after: Duration::ZERO,
            stable: started.elapsed() >= STABLE_AFTER,
        },
    }
}

/// Moves streams from one connection to the shared queue. Returns the error
/// that ended the connection, or `None` after a stop request.
async fn forward(
    client: TunnelClient,
    streams: &mpsc::Sender<IncomingStream>,
    stop: &mut watch::Receiver<Option<Duration>>,
) -> Option<Error> {
    loop {
        tokio::select! {
            accepted = client.accept() => match accepted {
                Ok(stream) => {
                    // The Tunnel was dropped, so nobody reads the queue.
                    if streams.send(stream).await.is_err() {
                        client.shutdown(Duration::ZERO).await;

                        return None;
                    }
                }
                Err(error) => return Some(error),
            },

            _ = stop.changed() => {
                let grace = stop.borrow().unwrap_or_default();
                client.shutdown(grace).await;

                return None;
            }
        }
    }
}

/// Delay before retry number `attempt`: 1 s, doubling up to 60 s.
fn backoff(attempt: usize) -> Duration {
    MIN_BACKOFF
        .saturating_mul(1 << attempt.min(6))
        .min(MAX_BACKOFF)
}

/// Adds up to 25 % so connections that fail together do not retry together.
fn jitter(delay: Duration) -> Duration {
    let fraction = (Uuid::new_v4().as_u128() % 250) as u32;

    delay + delay.mul_f64(f64::from(fraction) / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_then_caps() {
        assert_eq!(backoff(0), Duration::from_secs(1));
        assert_eq!(backoff(1), Duration::from_secs(2));
        assert_eq!(backoff(5), Duration::from_secs(32));
        assert_eq!(backoff(6), Duration::from_secs(60));
        assert_eq!(backoff(500), Duration::from_secs(60));
    }

    #[test]
    fn jitter_adds_at_most_a_quarter() {
        for _ in 0..100 {
            let delayed = jitter(Duration::from_secs(8));

            assert!(delayed >= Duration::from_secs(8));
            assert!(delayed < Duration::from_secs(10));
        }
    }
}
