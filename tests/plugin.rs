//! Plugin wiring: install, modes, roles, leader tasks, ConfigMaps, events,
//! health, and metrics (AC1, AC2, AC3, AC6, AC7, AC8, AC9, AC10, AC13).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::similar_names
)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_plugin_kubernetes::api::{KubeApi, MemoryKubeApi};
use autumn_plugin_kubernetes::{
    ConfigMapStore, KubeError, KubernetesConfig, KubernetesPlugin, LeaderTask, Leadership, PodInfo,
};
use autumn_web::actuator::{HealthIndicator, HealthStatus, IndicatorGroup, MetricsSource};
use autumn_web::{AppState, ProcessRole};
use common::{LEASE, NS, wait_until};

fn pod() -> PodInfo {
    PodInfo {
        name: Some("web-1".into()),
        namespace: Some(NS.into()),
        uid: Some("uid-1".into()),
        in_cluster: true,
        ..PodInfo::default()
    }
}

fn config() -> KubernetesConfig {
    let mut c = KubernetesConfig::default();
    c.leader_election.enabled = true;
    c.leader_election.lease_name = LEASE.into();
    c.config_maps.watch = vec!["flags".into()];
    c
}

fn plugin(api: &MemoryKubeApi, cfg: KubernetesConfig) -> KubernetesPlugin {
    KubernetesPlugin::with_config(cfg)
        .with_api(api.clone())
        .with_pod_info(pod())
}

async fn no_cluster(_pod: Option<String>) -> Result<kube::Client, KubeError> {
    Err(KubeError::NoCluster("test".into()))
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_installs_with_one_call() {
    use autumn_web::test::TestApp;
    let api = MemoryKubeApi::new();
    let client = TestApp::new().plugin(plugin(&api, config())).build();
    let state = client.state();
    let pod = PodInfo::from_state(state).unwrap();
    assert_eq!(pod.name.as_deref(), Some("web-1"));
    assert!(ConfigMapStore::from_state(state).is_some());
    let lead = Leadership::from_state(state).unwrap();
    assert_eq!(lead.lease_name(), LEASE);
    // TestApp runs startup hooks on a runtime that ends after startup. The
    // elector task ends with it, and a stopped elector never reads as leader.
    assert!(!lead.is_leader());
    let body = client.get("/actuator/health").send().await.text();
    assert!(body.contains("kubernetes"), "{body}");
    assert!(body.contains("UP"), "{body}");
}

#[test]
fn plugin_name_and_config_section() {
    use autumn_web::plugin::Plugin;
    let p = KubernetesPlugin::new();
    assert_eq!(p.name(), "autumn-plugin-kubernetes");
    assert!(format!("{p:?}").contains("KubernetesPlugin"));
    let app = autumn_web::app().plugin(KubernetesPlugin::default());
    assert!(app.has_plugin("autumn-plugin-kubernetes"));
    assert!(app.has_config_section("kubernetes"));
}

#[tokio::test(start_paused = true)]
async fn leader_runtime_end_to_end() {
    let api = MemoryKubeApi::new();
    let runs = Arc::new(AtomicU32::new(0));
    let r2 = Arc::clone(&runs);
    let task = LeaderTask::new("sweeper", move |_state: AppState, cancel| {
        let r2 = Arc::clone(&r2);
        async move {
            r2.fetch_add(1, Ordering::SeqCst);
            cancel.cancelled().await;
        }
    });
    let state = AppState::for_test();
    let rt = plugin(&api, config())
        .leader_task(task)
        .start_with_role(&state, ProcessRole::Worker)
        .await
        .unwrap();
    assert_eq!(rt.mode(), "custom");
    assert_eq!(rt.namespace(), Some(NS));
    let lead = rt.leadership().unwrap();
    assert!(lead.wait_until_leader().await);
    assert!(lead.identity().starts_with("web-1-"), "{}", lead.identity());
    wait_until(Duration::from_secs(5), || runs.load(Ordering::SeqCst) == 1).await;
    wait_until(Duration::from_secs(5), || api.events().len() >= 2).await;
    rt.shutdown().await;
    let reasons: Vec<String> = api.events().iter().map(|(_, e)| e.reason.clone()).collect();
    assert_eq!(
        reasons,
        vec!["Started", "LeaderElected", "Stopping"],
        "{reasons:?}"
    );
    let (pod_ref, _) = &api.events()[0];
    assert_eq!(pod_ref.name, "web-1");
    assert_eq!(pod_ref.namespace, NS);
    assert_eq!(pod_ref.uid.as_deref(), Some("uid-1"));
    assert!(
        api.lease(NS, LEASE).unwrap().is_free(),
        "released on shutdown"
    );
    assert!(!lead.is_leader());
}

#[tokio::test(start_paused = true)]
async fn role_not_listed_does_not_campaign() {
    let api = MemoryKubeApi::new();
    let mut cfg = config();
    cfg.leader_election.roles = vec!["worker".into()];
    let runs = Arc::new(AtomicU32::new(0));
    let r2 = Arc::clone(&runs);
    let state = AppState::for_test();
    let rt = plugin(&api, cfg)
        .leader_task(LeaderTask::new("t", move |_s: AppState, _c| {
            let r2 = Arc::clone(&r2);
            async move {
                r2.fetch_add(1, Ordering::SeqCst);
            }
        }))
        .start_with_role(&state, ProcessRole::Web)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(rt.leadership().is_none());
    assert!(Leadership::from_state(&state).is_none());
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(api.lease(NS, LEASE).is_none(), "no lease write");
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn config_maps_are_live_in_state() {
    let api = MemoryKubeApi::new().with_namespace("other");
    api.put_config_map(
        NS,
        "flags",
        BTreeMap::from([("beta".into(), "true".into())]),
    );
    let state = AppState::for_test();
    let rt = plugin(&api, config()).start(&state).await.unwrap();
    let store = ConfigMapStore::from_state(&state).unwrap();
    wait_until(Duration::from_secs(5), || store.is_synced("flags")).await;
    assert_eq!(store.json::<bool>("flags", "beta").unwrap(), Some(true));
    api.put_config_map(
        NS,
        "flags",
        BTreeMap::from([("beta".into(), "false".into())]),
    );
    wait_until(Duration::from_secs(5), || {
        store.value("flags", "beta").as_deref() == Some("false")
    })
    .await;
    assert!(rt.config_maps().is_some());
    assert_eq!(rt.metrics().snapshot().config_maps["flags"].updates, 2);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn namespace_order_is_config_then_pod_then_client() {
    let api = MemoryKubeApi::new().with_namespace("client-ns");
    let mut cfg = config();
    cfg.namespace = "cfg-ns".into();
    let rt = plugin(&api, cfg)
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert_eq!(rt.namespace(), Some("cfg-ns"));
    rt.shutdown().await;

    let rt = plugin(&api, config())
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert_eq!(rt.namespace(), Some(NS), "pod namespace");
    rt.shutdown().await;

    let rt = KubernetesPlugin::with_config(config())
        .with_api(api.clone())
        .with_pod_info(PodInfo::default())
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert_eq!(rt.namespace(), Some("client-ns"));
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn configured_identity_is_used_verbatim() {
    let api = MemoryKubeApi::new();
    let mut cfg = config();
    cfg.leader_election.identity = "fixed-id".into();
    let rt = plugin(&api, cfg)
        .start(&AppState::for_test())
        .await
        .unwrap();
    let lead = rt.leadership().unwrap();
    assert_eq!(lead.identity(), "fixed-id");
    assert!(lead.wait_until_leader().await);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn no_pod_name_writes_no_events() {
    let api = MemoryKubeApi::new();
    let rt = KubernetesPlugin::with_config(config())
        .with_api(api.clone())
        .with_pod_info(PodInfo::default())
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert!(rt.leadership().unwrap().wait_until_leader().await);
    assert!(rt.leadership().unwrap().identity().starts_with("autumn-"));
    rt.shutdown().await;
    assert!(api.events().is_empty());
}

#[tokio::test(start_paused = true)]
async fn events_off_writes_no_events() {
    let api = MemoryKubeApi::new();
    let mut cfg = config();
    cfg.events = false;
    let rt = plugin(&api, cfg)
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert!(rt.leadership().unwrap().wait_until_leader().await);
    rt.shutdown().await;
    assert!(api.events().is_empty());
}

#[tokio::test(start_paused = true)]
async fn event_failures_do_not_stop_the_app() {
    let api = MemoryKubeApi::new();
    api.set_events_failing(true);
    let rt = plugin(&api, config())
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert!(rt.leadership().unwrap().wait_until_leader().await);
    let m = rt.metrics();
    wait_until(Duration::from_secs(5), || m.snapshot().events_failed >= 2).await;
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn disabled_gives_only_pod_info() {
    let api = MemoryKubeApi::new();
    let mut cfg = config();
    cfg.enabled = false;
    let state = AppState::for_test();
    let rt = plugin(&api, cfg).start(&state).await.unwrap();
    assert_eq!(rt.mode(), "disabled");
    assert_eq!(rt.namespace(), None);
    assert!(PodInfo::from_state(&state).is_some());
    assert!(Leadership::from_state(&state).is_none());
    assert!(ConfigMapStore::from_state(&state).is_none());
    let out = rt.health().check().await;
    assert_eq!(out.status, HealthStatus::Up);
    assert_eq!(out.details["mode"], "disabled");
    rt.shutdown().await;
    assert!(api.lease(NS, LEASE).is_none());
}

#[tokio::test(start_paused = true)]
async fn no_cluster_runs_detached() {
    let state = AppState::for_test();
    let rt = KubernetesPlugin::with_config(config())
        .with_connector(no_cluster)
        .with_pod_info(pod())
        .start(&state)
        .await
        .unwrap();
    assert_eq!(rt.mode(), "detached");
    assert!(rt.leadership().is_none());
    assert!(state.extension::<kube::Client>().is_none());
    assert!(PodInfo::from_state(&state).is_some());
    let out = rt.health().check().await;
    assert_eq!(out.status, HealthStatus::Up);
    assert_eq!(out.details["mode"], "detached");
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn required_without_cluster_fails() {
    let mut cfg = config();
    cfg.required = true;
    let err = KubernetesPlugin::with_config(cfg)
        .with_connector(no_cluster)
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(matches!(err, KubeError::NoCluster(_)), "{err}");
}

#[tokio::test(start_paused = true)]
async fn required_with_api_down_fails() {
    let api = MemoryKubeApi::new();
    api.set_down(true);
    let mut cfg = config();
    cfg.required = true;
    let err = plugin(&api, cfg)
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(matches!(err, KubeError::Api(_)), "{err}");
}

#[tokio::test(start_paused = true)]
async fn connector_error_other_than_no_cluster_fails() {
    let err = KubernetesPlugin::with_config(config())
        .with_connector(|_| async { Err(KubeError::Api("bad kubeconfig".into())) })
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(matches!(err, KubeError::Api(_)), "{err}");
}

#[tokio::test(start_paused = true)]
async fn leader_tasks_need_leader_election() {
    let mut cfg = config();
    cfg.leader_election.enabled = false;
    let err = plugin(&MemoryKubeApi::new(), cfg)
        .leader_task(LeaderTask::new("t", |_s: AppState, _c| async {}))
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("leader_election.enabled"), "{err}");
}

#[tokio::test(start_paused = true)]
async fn bad_config_fails() {
    let mut cfg = config();
    cfg.leader_election.lease_name = String::new();
    let err = plugin(&MemoryKubeApi::new(), cfg)
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(matches!(err, KubeError::Config(_)), "{err}");
}

#[tokio::test(start_paused = true)]
async fn health_and_metrics_report_state() {
    let api = MemoryKubeApi::new();
    let rt = plugin(&api, config())
        .start(&AppState::for_test())
        .await
        .unwrap();
    assert!(rt.leadership().unwrap().wait_until_leader().await);
    let h = rt.health();
    assert!(matches!(h.group(), IndicatorGroup::HealthOnly));
    let out = h.check().await;
    assert_eq!(out.status, HealthStatus::Up);
    assert_eq!(out.details["leader"]["leading"], true);
    api.set_down(true);
    // The result is reused for a short time. Then the check sees the outage.
    tokio::time::sleep(autumn_plugin_kubernetes::health::UP_TTL).await;
    assert_eq!(h.check().await.status, HealthStatus::Down);
    assert_eq!(rt.metrics().snapshot().api_up, 0);
    api.set_down(false);
    let names: Vec<String> = rt.metrics().collect().into_iter().map(|f| f.name).collect();
    assert!(names.contains(&"kubernetes_leader".to_owned()), "{names:?}");
    assert_eq!(rt.metrics().snapshot().leading, 1);
    assert!(format!("{rt:?}").contains("custom"));
    rt.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_opt_in_gates_ready() {
    use autumn_web::test::TestApp;
    let api = MemoryKubeApi::new();
    api.set_down(true);
    let plain = TestApp::new()
        .plugin(plugin(&api, KubernetesConfig::default()))
        .build();
    plain.get("/ready").send().await.assert_status(200);
    let gated = TestApp::new()
        .plugin(plugin(&api, KubernetesConfig::default()).readiness(true))
        .build();
    gated.get("/ready").send().await.assert_status(503);
    // Positive control: the same gate passes when the API answers (after
    // the short reuse time of a failed result).
    api.set_down(false);
    tokio::time::sleep(autumn_plugin_kubernetes::health::DOWN_TTL).await;
    gated.get("/ready").send().await.assert_status(200);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_hook_releases_lease() {
    // TestApp does not run shutdown hooks. Build the real app, then run the
    // hooks through the runtime handle instead.
    let api = MemoryKubeApi::new();
    let state = AppState::for_test();
    let rt = plugin(&api, config()).start(&state).await.unwrap();
    assert!(rt.leadership().unwrap().wait_until_leader().await);
    rt.shutdown().await;
    assert!(api.lease(NS, LEASE).unwrap().is_free());
    let _: Arc<dyn KubeApi> = Arc::new(api);
}

#[tokio::test(start_paused = true)]
async fn detached_runs_no_leader_tasks_by_default() {
    let runs = Arc::new(AtomicU32::new(0));
    let r2 = Arc::clone(&runs);
    let rt = KubernetesPlugin::with_config(config())
        .with_connector(no_cluster)
        .leader_task(LeaderTask::new("t", move |_s: AppState, _c| {
            let r2 = Arc::clone(&r2);
            async move {
                r2.fetch_add(1, Ordering::SeqCst);
            }
        }))
        .start(&AppState::for_test())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(rt.leadership().is_none());
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lead_when_detached_runs_leader_tasks_locally() {
    let mut cfg = config();
    cfg.leader_election.lead_when_detached = true;
    let running = Arc::new(AtomicU32::new(0));
    let r2 = Arc::clone(&running);
    let state = AppState::for_test();
    let rt = KubernetesPlugin::with_config(cfg)
        .with_connector(no_cluster)
        .with_pod_info(pod())
        .leader_task(LeaderTask::new("t", move |_s: AppState, cancel| {
            let r2 = Arc::clone(&r2);
            async move {
                r2.fetch_add(1, Ordering::SeqCst);
                cancel.cancelled().await;
                r2.fetch_sub(1, Ordering::SeqCst);
            }
        }))
        .start(&state)
        .await
        .unwrap();
    assert_eq!(rt.mode(), "detached");
    let lead = Leadership::from_state(&state).unwrap();
    assert!(lead.is_leader());
    assert_eq!(lead.holder().as_deref(), Some(lead.identity()));
    wait_until(Duration::from_secs(5), || {
        running.load(Ordering::SeqCst) == 1
    })
    .await;
    let out = rt.health().check().await;
    assert_eq!(out.details["leader"]["leading"], true);
    rt.shutdown().await;
    assert!(!lead.is_leader());
    assert_eq!(running.load(Ordering::SeqCst), 0, "task stopped");
}

#[tokio::test(start_paused = true)]
async fn losing_the_lease_writes_leader_lost() {
    let api = MemoryKubeApi::new();
    let rt = plugin(&api, config())
        .start(&AppState::for_test())
        .await
        .unwrap();
    let lead = rt.leadership().unwrap();
    assert!(lead.wait_until_leader().await);
    let mut intruder = api.lease(NS, LEASE).unwrap();
    intruder.holder = Some("intruder".into());
    api.put_lease(NS, LEASE, intruder);
    lead.wait_until_follower().await;
    wait_until(Duration::from_secs(5), || {
        api.events().iter().any(|(_, e)| e.reason == "LeaderLost")
    })
    .await;
    let lost = api
        .events()
        .into_iter()
        .find(|(_, e)| e.reason == "LeaderLost")
        .unwrap()
        .1;
    assert_eq!(lost.kind, autumn_plugin_kubernetes::api::EventKind::Warning);
    rt.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn required_with_a_silent_api_times_out() {
    let api = MemoryKubeApi::new();
    api.set_latency(Duration::from_secs(60));
    let mut cfg = config();
    cfg.required = true;
    let err = plugin(&api, cfg)
        .start(&AppState::for_test())
        .await
        .unwrap_err();
    assert!(
        matches!(err, KubeError::Api(ref m) if m.contains("in time")),
        "{err}"
    );
}
