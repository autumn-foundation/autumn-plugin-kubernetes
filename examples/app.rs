//! Example app: pod info, a leader task, and a live ConfigMap.
//!
//! In a cluster, apply the output of `cargo run --example manifests`. On a
//! laptop with no cluster, the plugin runs detached. Then:
//! `cargo run --example app`
//!
//! `autumn.toml`:
//!
//! ```toml
//! [kubernetes.leader_election]
//! enabled = true
//! lease_name = "example-leader"
//!
//! [kubernetes.config_maps]
//! watch = ["example-flags"]
//! ```

use std::time::Duration;

use autumn_plugin_kubernetes::{ConfigMapStore, KubernetesPlugin, LeaderTask, Leadership, PodInfo};
use autumn_web::AppState;
use autumn_web::prelude::*;
use autumn_web::reexports::axum::extract::State;
use tokio_util::sync::CancellationToken;

/// Shows who serves the request and who leads.
#[get("/whoami")]
async fn whoami(State(state): State<AppState>) -> String {
    let pod = PodInfo::from_state(&state).unwrap_or_default();
    let leader = Leadership::from_state(&state).map_or_else(
        || "leader election off".to_owned(),
        |l| format!("leading: {}, holder: {:?}", l.is_leader(), l.holder()),
    );
    format!(
        "pod: {:?}, namespace: {:?}, {leader}\n",
        pod.name, pod.namespace
    )
}

/// Reads a live flag from the `example-flags` ConfigMap.
#[get("/flags/beta")]
async fn beta(State(state): State<AppState>) -> String {
    let on = ConfigMapStore::from_state(&state)
        .and_then(|s| s.json::<bool>("example-flags", "beta").ok().flatten())
        .unwrap_or(false);
    format!("beta: {on}\n")
}

/// Runs on one replica only. Stops when leadership ends.
async fn report(_state: AppState, cancel: CancellationToken) {
    loop {
        tracing::info!("leader task: this replica does the singleton work");
        tokio::select! {
            () = cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_secs(10)) => {}
        }
    }
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(routes![whoami, beta])
        .plugin(KubernetesPlugin::new().leader_task(LeaderTask::new("report", report)))
        .run()
        .await;
}
