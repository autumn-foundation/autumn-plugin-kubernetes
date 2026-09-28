//! Health indicator `kubernetes` for `/actuator/health`.
//!
//! Status: `Up` when the API server answers `GET /version` in 1.5 s, or when
//! the plugin runs detached (no cluster). `Down` when the check fails.
//! `Unknown` before startup. Details have the mode, namespace, leader state,
//! and ConfigMap sync state. They have no URLs, tokens, or error text: only
//! a short error class.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator, HealthStatus, IndicatorGroup};
use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::api::KubeApi;
use crate::configmap::ConfigMapStore;
use crate::leader::Leadership;
use crate::metrics::KubernetesMetrics;

/// Time limit for the API check. It is under the 2 s indicator timeout.
pub const CHECK_TIMEOUT: Duration = Duration::from_millis(1_500);

/// What the indicator checks. Set once at startup.
pub(crate) struct HealthTarget {
    /// `None`: detached or disabled. No API check.
    pub api: Option<Arc<dyn KubeApi>>,
    /// `in_cluster`, `kubeconfig`, `custom`, `detached`, or `disabled`.
    pub mode: &'static str,
    pub namespace: Option<String>,
    pub leadership: Option<Leadership>,
    pub config_maps: Option<ConfigMapStore>,
}

/// The `kubernetes` health indicator.
pub struct KubernetesHealth {
    target: OnceLock<HealthTarget>,
    readiness: bool,
    metrics: Arc<KubernetesMetrics>,
}

impl std::fmt::Debug for KubernetesHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubernetesHealth")
            .field("started", &self.target.get().is_some())
            .field("readiness", &self.readiness)
            .finish_non_exhaustive()
    }
}

impl KubernetesHealth {
    pub(crate) const fn new(readiness: bool, metrics: Arc<KubernetesMetrics>) -> Self {
        Self {
            target: OnceLock::new(),
            readiness,
            metrics,
        }
    }

    /// Sets the target once. Later calls do nothing.
    pub(crate) fn set(&self, target: HealthTarget) {
        let _ = self.target.set(target);
    }

    async fn run_check(&self) -> HealthCheckOutput {
        let Some(target) = self.target.get() else {
            return HealthCheckOutput {
                status: HealthStatus::Unknown,
                details: HashMap::from([("reason".to_owned(), json!("not started"))]),
            };
        };
        let mut details: HashMap<String, Value> = HashMap::new();
        details.insert("mode".to_owned(), json!(target.mode));
        if let Some(ns) = &target.namespace {
            details.insert("namespace".to_owned(), json!(ns));
        }
        if let Some(lead) = &target.leadership {
            details.insert(
                "leader".to_owned(),
                json!({
                    "lease": lead.lease_name(),
                    "leading": lead.is_leader(),
                    "holder": lead.holder(),
                }),
            );
        }
        if let Some(store) = &target.config_maps {
            let maps: serde_json::Map<String, Value> = store
                .names()
                .into_iter()
                .map(|n| {
                    let state = if !store.is_synced(&n) {
                        "waiting"
                    } else if store.get(&n).is_some() {
                        "present"
                    } else {
                        "missing"
                    };
                    (n, json!(state))
                })
                .collect();
            details.insert("config_maps".to_owned(), Value::Object(maps));
        }
        let Some(api) = &target.api else {
            return HealthCheckOutput {
                status: HealthStatus::Up,
                details,
            };
        };
        let status = match tokio::time::timeout(CHECK_TIMEOUT, api.server_version()).await {
            Ok(Ok(version)) => {
                details.insert("version".to_owned(), json!(version));
                HealthStatus::Up
            }
            Ok(Err(e)) => {
                details.insert("error".to_owned(), json!(e.class()));
                HealthStatus::Down
            }
            Err(_) => {
                details.insert("error".to_owned(), json!("timeout"));
                HealthStatus::Down
            }
        };
        self.metrics.set_api_up(status == HealthStatus::Up);
        HealthCheckOutput { status, details }
    }
}

impl HealthIndicator for KubernetesHealth {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        Box::pin(self.run_check())
    }

    fn group(&self) -> IndicatorGroup {
        if self.readiness {
            IndicatorGroup::Readiness
        } else {
            IndicatorGroup::HealthOnly
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::MemoryKubeApi;

    fn health(readiness: bool) -> (KubernetesHealth, Arc<KubernetesMetrics>) {
        let m = Arc::new(KubernetesMetrics::new());
        (KubernetesHealth::new(readiness, Arc::clone(&m)), m)
    }

    fn target(api: Option<Arc<dyn KubeApi>>, mode: &'static str) -> HealthTarget {
        HealthTarget {
            api,
            mode,
            namespace: Some("shop".into()),
            leadership: None,
            config_maps: None,
        }
    }

    #[tokio::test]
    async fn unknown_before_start() {
        let (h, _) = health(false);
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Unknown);
        assert_eq!(out.details["reason"], json!("not started"));
        assert!(format!("{h:?}").contains("started: false"));
    }

    #[tokio::test]
    async fn detached_is_up() {
        let (h, m) = health(false);
        h.set(target(None, "detached"));
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Up);
        assert_eq!(out.details["mode"], json!("detached"));
        assert_eq!(m.snapshot().api_up, -1, "no check, no gauge");
    }

    #[tokio::test]
    async fn api_up_and_down() {
        let api = MemoryKubeApi::new();
        let (h, m) = health(false);
        h.set(target(Some(Arc::new(api.clone())), "in_cluster"));
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Up);
        assert_eq!(out.details["mode"], json!("in_cluster"));
        assert_eq!(out.details["namespace"], json!("shop"));
        assert_eq!(out.details["version"], json!("v1.34.0-memory"));
        assert_eq!(m.snapshot().api_up, 1);
        api.set_down(true);
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Down);
        assert_eq!(out.details["error"], json!("api"));
        let text = serde_json::to_string(&out.details).unwrap();
        assert!(!text.contains("refused"), "no raw error text: {text}");
        assert_eq!(m.snapshot().api_up, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_api_is_down() {
        let api = MemoryKubeApi::new();
        api.set_latency(Duration::from_secs(5));
        let (h, _) = health(false);
        h.set(target(Some(Arc::new(api)), "in_cluster"));
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Down);
        assert_eq!(out.details["error"], json!("timeout"));
    }

    #[tokio::test(start_paused = true)]
    async fn shows_leader_and_config_maps() {
        let api = MemoryKubeApi::new();
        let elector = crate::leader::LeaderElector::new(
            Arc::new(api.clone()),
            "shop",
            "me",
            crate::config::LeaderElectionConfig {
                enabled: true,
                lease_name: "l".into(),
                ..crate::config::LeaderElectionConfig::default()
            },
        )
        .unwrap()
        .start();
        assert!(elector.leadership().wait_until_leader().await);
        let store = ConfigMapStore::new(["flags"]);
        let (h, _) = health(false);
        h.set(HealthTarget {
            leadership: Some(elector.leadership()),
            config_maps: Some(store),
            ..target(Some(Arc::new(api)), "custom")
        });
        let out = h.check().await;
        assert_eq!(out.details["leader"]["lease"], json!("l"));
        assert_eq!(out.details["leader"]["leading"], json!(true));
        assert_eq!(out.details["leader"]["holder"], json!("me"));
        assert_eq!(out.details["config_maps"]["flags"], json!("waiting"));
        elector.stop().await;
    }

    #[test]
    fn group_follows_readiness_flag() {
        assert!(matches!(
            health(false).0.group(),
            IndicatorGroup::HealthOnly
        ));
        assert!(matches!(health(true).0.group(), IndicatorGroup::Readiness));
    }
}
