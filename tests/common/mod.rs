//! Shared test helpers.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

pub mod contract;

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

/// Path of a built example program.
///
/// `AUTUMN_K8S_EXAMPLES_DIR` wins (CI sets it). Else `examples/<name>` next to
/// the test binary, which `cargo test` writes. `cargo llvm-cov --all-targets`
/// builds examples as test harnesses, so under it, run
/// `cargo build --examples` and set the variable to `target/debug/examples`.
pub fn example_bin(name: &str) -> std::path::PathBuf {
    let dir = std::env::var_os("AUTUMN_K8S_EXAMPLES_DIR").map_or_else(
        || {
            let exe = std::env::current_exe().unwrap();
            exe.parent().unwrap().parent().unwrap().join("examples")
        },
        std::path::PathBuf::from,
    );
    let path = dir.join(name);
    assert!(
        path.is_file(),
        "no example program {}: run `cargo build --examples` and set \
         AUTUMN_K8S_EXAMPLES_DIR=target/debug/examples",
        path.display()
    );
    path
}
