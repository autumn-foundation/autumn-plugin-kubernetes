//! Health indicator `kubernetes` for `/actuator/health`.
//!
//! Status: `Up` when the API server answers `GET /version` in 1.5 s, or when
//! the plugin runs detached (no cluster). `Down` when the check fails.
//! `Unknown` before startup. Details have the mode, namespace, leader state,
//! and ConfigMap sync state. They have no URLs, tokens, versions, pod names,
//! or error text: only a short error class.
//!
//! The indicator keeps each result for `UP_TTL` or `DOWN_TTL`. Thus health
//! requests do not send many calls to the API server. The runtime also
//! refreshes the result in the background. Thus the `kubernetes_api_up`
//! metric stays current when there are no health requests.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::time::Instant;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator, HealthStatus, IndicatorGroup};
use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::api::KubeApi;
use crate::configmap::ConfigMapStore;
use crate::leader::Leadership;
use crate::metrics::KubernetesMetrics;

/// Time limit for the API check. It is under the 2 s indicator timeout.
pub const CHECK_TIMEOUT: Duration = Duration::from_millis(1_500);
/// How long an `Up` result is reused. Health requests do not reach the API
/// server more often than this.
pub const UP_TTL: Duration = Duration::from_secs(5);
/// How long a failed result is reused. Short, so recovery shows fast.
pub const DOWN_TTL: Duration = Duration::from_secs(1);

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
    /// Last API check: when, and the error class if it failed.
    cache: tokio::sync::Mutex<Option<(Instant, Result<(), &'static str>)>>,
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
            cache: tokio::sync::Mutex::const_new(None),
        }
    }

    /// Checks the API server when the stored result is old. The runtime
    /// calls it in the background. A fresh result is not checked again, so a
    /// request waits for at most one probe.
    pub(crate) async fn refresh(&self) {
        let _ = self.api_status().await;
    }

    /// One `GET /version`. Sets the metric.
    async fn probe(&self) -> Result<(), &'static str> {
        let Some(api) = self.target.get().and_then(|t| t.api.as_ref()) else {
            return Ok(());
        };
        let result = match tokio::time::timeout(CHECK_TIMEOUT, api.server_version()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.class()),
            Err(_) => Err("timeout"),
        };
        self.metrics.set_api_up(result.is_ok());
        result
    }

    /// The cached result, or a new check when the cached one is old. One
    /// check at a time.
    async fn api_status(&self) -> Result<(), &'static str> {
        let mut cache = self.cache.lock().await;
        if let Some((at, result)) = *cache {
            let ttl = if result.is_ok() { UP_TTL } else { DOWN_TTL };
            if at.elapsed() < ttl {
                return result;
            }
        }
        let result = self.probe().await;
        *cache = Some((Instant::now(), result));
        result
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
        if target.api.is_none() {
            return HealthCheckOutput {
                status: HealthStatus::Up,
                details,
            };
        }
        let status = match self.api_status().await {
            Ok(()) => HealthStatus::Up,
            Err(class) => {
                details.insert("error".to_owned(), json!(class));
                HealthStatus::Down
            }
        };
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

    #[tokio::test(start_paused = true)]
    async fn api_up_and_down() {
        let api = MemoryKubeApi::new();
        let (h, m) = health(false);
        h.set(target(Some(Arc::new(api.clone())), "in_cluster"));
        let out = h.check().await;
        assert_eq!(out.status, HealthStatus::Up);
        assert_eq!(out.details["mode"], json!("in_cluster"));
        assert_eq!(out.details["namespace"], json!("shop"));
        assert!(
            !out.details.contains_key("version"),
            "no server version in output"
        );
        assert_eq!(m.snapshot().api_up, 1);
        api.set_down(true);
        tokio::time::sleep(UP_TTL).await;
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
        assert!(
            out.details["leader"].get("holder").is_none(),
            "no pod names in output"
        );
        assert_eq!(out.details["config_maps"]["flags"], json!("waiting"));
        elector.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn results_are_cached_so_requests_do_not_hit_the_api() {
        let api = MemoryKubeApi::new();
        let (h, _) = health(false);
        h.set(target(Some(Arc::new(api.clone())), "in_cluster"));
        assert_eq!(h.check().await.status, HealthStatus::Up);
        api.set_down(true);
        assert_eq!(
            h.check().await.status,
            HealthStatus::Up,
            "cached for UP_TTL"
        );
        tokio::time::sleep(UP_TTL).await;
        assert_eq!(h.check().await.status, HealthStatus::Down);
        api.set_down(false);
        assert_eq!(
            h.check().await.status,
            HealthStatus::Down,
            "cached for DOWN_TTL"
        );
        tokio::time::sleep(DOWN_TTL).await;
        assert_eq!(
            h.check().await.status,
            HealthStatus::Up,
            "failures are cached briefly"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_updates_the_metric_without_requests() {
        let api = MemoryKubeApi::new();
        let (h, m) = health(false);
        h.set(target(Some(Arc::new(api.clone())), "in_cluster"));
        h.refresh().await;
        assert_eq!(m.snapshot().api_up, 1);
        api.set_down(true);
        h.refresh().await;
        assert_eq!(m.snapshot().api_up, 1, "fresh result: no new probe");
        tokio::time::sleep(UP_TTL).await;
        h.refresh().await;
        assert_eq!(m.snapshot().api_up, 0);
    }

    /// Review round 2: a request must not wait for two probes in a row.
    #[tokio::test(start_paused = true)]
    async fn a_request_waits_for_at_most_one_probe() {
        let api = MemoryKubeApi::new();
        api.set_latency(Duration::from_millis(1_400));
        let (h, _) = health(false);
        let h = Arc::new(h);
        h.set(target(Some(Arc::new(api)), "in_cluster"));
        let first = tokio::spawn({
            let h = Arc::clone(&h);
            async move { h.check().await }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let refresh = tokio::spawn({
            let h = Arc::clone(&h);
            async move { h.refresh().await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let start = Instant::now();
        let second = h.check().await;
        assert!(
            start.elapsed() < Duration::from_millis(2_000),
            "{:?}",
            start.elapsed()
        );
        assert_eq!(second.status, HealthStatus::Up);
        first.await.unwrap();
        refresh.await.unwrap();
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
