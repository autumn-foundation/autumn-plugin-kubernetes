//! Shared test helpers.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use autumn_plugin_kubernetes::api::MemoryKubeApi;
use autumn_plugin_kubernetes::config::LeaderElectionConfig;
use autumn_plugin_kubernetes::{ElectorHandle, LeaderElector};

pub const NS: &str = "shop";
pub const LEASE: &str = "app-leader";

/// Leader config with the default timing: 15 s, 10 s, 2 s.
pub fn leader_config() -> LeaderElectionConfig {
    LeaderElectionConfig {
        enabled: true,
        lease_name: LEASE.to_owned(),
        ..LeaderElectionConfig::default()
    }
}

/// Starts an elector for `identity` on the shared fake.
pub fn elect(api: &MemoryKubeApi, identity: &str) -> ElectorHandle {
    LeaderElector::new(Arc::new(api.clone()), NS, identity, leader_config())
        .unwrap()
        .start()
}

/// Waits (virtual time) until `cond` is true. Panics after `limit`.
pub async fn wait_until(limit: Duration, mut cond: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < limit,
            "condition not met within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Sleeps in virtual time.
pub async fn advance(d: Duration) {
    tokio::time::sleep(d).await;
}
