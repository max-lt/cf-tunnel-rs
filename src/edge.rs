use std::net::{IpAddr, SocketAddr};

use hickory_resolver::{
    Resolver,
    config::{CLOUDFLARE, ResolverConfig},
    net::runtime::TokioRuntimeProvider,
    proto::rr::RData,
};

use crate::Error;

const SRV_NAME: &str = "_v2-origintunneld._tcp.argotunnel.com.";

/// Edge addresses, grouped by region. The edge publishes one SRV target per
/// region.
#[derive(Debug, Clone)]
pub struct EdgeRegions(Vec<Vec<SocketAddr>>);

impl EdgeRegions {
    /// Resolves the edge regions. Falls back to DNS-over-TLS when the system
    /// resolver cannot answer.
    pub async fn discover() -> Result<Self, Error> {
        let system = Resolver::builder_tokio()
            .and_then(|builder| builder.build())
            .map_err(|e| Error::Dns(e.to_string()))?;

        if let Ok(regions) = lookup(&system).await {
            return Ok(regions);
        }

        // Some networks drop SRV answers, so ask Cloudflare over TLS instead.
        let dot = Resolver::builder_with_config(
            ResolverConfig::tls(&CLOUDFLARE),
            TokioRuntimeProvider::default(),
        )
        .build()
        .map_err(|e| Error::Dns(e.to_string()))?;

        lookup(&dot).await
    }

    /// A single region made of the given addresses.
    pub fn fixed(addrs: Vec<SocketAddr>) -> Self {
        Self(vec![addrs])
    }

    /// Picks the address for connection `index` on its `attempt`-th try.
    /// Connections spread over the regions, and a retry moves to the next
    /// address of the same region. An address in `taken` is skipped, because
    /// the edge refuses two connections of one client on the same address.
    pub fn pick(&self, index: usize, attempt: usize, taken: &[SocketAddr]) -> Option<SocketAddr> {
        let region = self.0.get(index % self.0.len().max(1))?;

        if region.is_empty() {
            return None;
        }

        let start = index / self.0.len() + attempt;
        let candidates = (0..region.len()).map(|step| region[(start + step) % region.len()]);

        // With every address taken, reuse one rather than stay offline.
        candidates
            .clone()
            .find(|addr| !taken.contains(addr))
            .or(candidates.into_iter().next())
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(Vec::is_empty)
    }
}

async fn lookup(resolver: &hickory_resolver::TokioResolver) -> Result<EdgeRegions, Error> {
    let records = resolver
        .srv_lookup(SRV_NAME)
        .await
        .map_err(|e| Error::Dns(e.to_string()))?;

    let mut regions = Vec::new();

    // Resolve every SRV target to the addresses of one region.
    for record in records.answers() {
        let RData::SRV(srv) = &record.data else {
            continue;
        };

        let ips = resolver
            .lookup_ip(srv.target.to_string())
            .await
            .map_err(|e| Error::Dns(e.to_string()))?;

        let region: Vec<SocketAddr> = ips
            .iter()
            .filter(|ip: &IpAddr| ip.is_ipv4())
            .map(|ip| SocketAddr::new(ip, srv.port))
            .collect();

        if !region.is_empty() {
            regions.push(region);
        }
    }

    if regions.is_empty() {
        return Err(Error::Dns(format!("no edge address under {SRV_NAME}")));
    }

    Ok(EdgeRegions(regions))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::from(([198, 41, 192, last], 7844))
    }

    fn regions() -> EdgeRegions {
        EdgeRegions(vec![
            vec![addr(1), addr(2), addr(3)],
            vec![addr(11), addr(12)],
        ])
    }

    #[test]
    fn connections_alternate_between_regions() {
        let r = regions();

        assert_eq!(r.pick(0, 0, &[]), Some(addr(1)));
        assert_eq!(r.pick(1, 0, &[]), Some(addr(11)));
        assert_eq!(r.pick(2, 0, &[]), Some(addr(2)));
        assert_eq!(r.pick(3, 0, &[]), Some(addr(12)));
    }

    #[test]
    fn a_retry_moves_to_the_next_address_of_the_region() {
        let r = regions();

        assert_eq!(r.pick(0, 1, &[]), Some(addr(2)));
        assert_eq!(r.pick(0, 3, &[]), Some(addr(1)));
    }

    #[test]
    fn taken_addresses_are_skipped() {
        let r = regions();

        assert_eq!(r.pick(0, 0, &[addr(1)]), Some(addr(2)));
        assert_eq!(r.pick(0, 0, &[addr(1), addr(2)]), Some(addr(3)));

        // With every address taken, the first candidate is reused.
        assert_eq!(r.pick(0, 0, &[addr(1), addr(2), addr(3)]), Some(addr(1)));
    }

    #[test]
    fn empty_regions_give_no_address() {
        assert_eq!(EdgeRegions(vec![]).pick(0, 0, &[]), None);
        assert!(EdgeRegions::fixed(vec![]).is_empty());
    }
}
