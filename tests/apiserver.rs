//! Tests against a real `kube-apiserver` (AC3, AC4, AC5, AC7, AC8, AC11,
//! AC14).
//!
//! Run `scripts/envtest.sh start` first and export `KUBE_IT_DIR`. With no
//! `KUBE_IT_DIR` the tests skip, unless `KUBE_IT_REQUIRED=1` (CI).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use autumn_plugin_kubernetes::api::{KubeApi, KubeClientApi, LeaseRecord};
use autumn_plugin_kubernetes::config::LeaderElectionConfig;
use autumn_plugin_kubernetes::manifest::ManifestSpec;
use autumn_plugin_kubernetes::{
    ConfigMapStore, KubeError, KubernetesConfig, KubernetesPlugin, LeaderElector, PodInfo,
};
use autumn_web::AppState;
use autumn_web::actuator::{HealthIndicator, HealthStatus};
use common::contract;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
use k8s_openapi::api::events::v1::Event;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams};
use kube::core::GroupVersionKind;

fn it_dir() -> Option<PathBuf> {
    let dir = std::env::var_os("KUBE_IT_DIR").map(PathBuf::from);
    if dir.is_none() {
        assert!(
            std::env::var_os("KUBE_IT_REQUIRED").is_none(),
            "KUBE_IT_REQUIRED is set but KUBE_IT_DIR is not"
        );
        eprintln!("skip: KUBE_IT_DIR is not set (run scripts/envtest.sh start)");
    }
    dir
}

async fn client(dir: &std::path::Path, file: &str) -> kube::Client {
    let kc = kube::config::Kubeconfig::read_from(dir.join(file)).unwrap();
    let mut cfg =
        kube::Config::from_custom_kubeconfig(kc, &kube::config::KubeConfigOptions::default())
            .await
            .unwrap();
    // The test server is on 127.0.0.1. kube takes HTTPS_PROXY from the env
    // and has no NO_PROXY support, so clear it for this loopback client.
    cfg.proxy_url = None;
    kube::Client::try_from(cfg).unwrap()
}

/// A name that is new for each run on the same server.
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            & 0xff_ffff_ffff
    )
}

async fn ensure_ns(admin: &kube::Client, ns: &str) {
    let api: Api<Namespace> = Api::all(admin.clone());
    let obj: Namespace = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}
    }))
    .unwrap();
    match api.create(&PostParams::default(), &obj).await {
        Ok(_) => {}
        Err(kube::Error::Api(s)) if s.is_already_exists() => {}
        Err(e) => panic!("{e}"),
    }
}

async fn put_cm(admin: &kube::Client, ns: &str, name: &str, data: BTreeMap<String, String>) {
    let api: Api<ConfigMap> = Api::namespaced(admin.clone(), ns);
    let cm = ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    };
    api.patch(name, &PatchParams::apply("it").force(), &Patch::Apply(&cm))
        .await
        .unwrap();
}

async fn delete_cm(admin: &kube::Client, ns: &str, name: &str) {
    let api: Api<ConfigMap> = Api::namespaced(admin.clone(), ns);
    api.delete(name, &DeleteParams::default()).await.unwrap();
}

/// Applies manifest objects with server-side apply.
async fn apply(admin: &kube::Client, objs: &[serde_json::Value], dry_run: bool) {
    for obj in objs {
        let api_version = obj["apiVersion"].as_str().unwrap();
        let (group, version) = api_version.split_once('/').unwrap_or(("", api_version));
        let gvk = GroupVersionKind::gvk(group, version, obj["kind"].as_str().unwrap());
        let (ar, _caps) = kube::discovery::pinned_kind(admin, &gvk).await.unwrap();
        let ns = obj["metadata"]["namespace"].as_str().unwrap();
        let name = obj["metadata"]["name"].as_str().unwrap();
        let api: Api<DynamicObject> = Api::namespaced_with(admin.clone(), ns, &ar);
        let mut pp = PatchParams::apply("autumn-it").force();
        if dry_run {
            pp = pp.dry_run();
        }
        api.patch(name, &pp, &Patch::Apply(obj))
            .await
            .unwrap_or_else(|e| panic!("{} {name}: {e}", obj["kind"]));
    }
}

async fn wait_for(limit: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < limit, "condition not met in {limit:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn fast_leader(lease: &str) -> LeaderElectionConfig {
    LeaderElectionConfig {
        enabled: true,
        lease_name: lease.to_owned(),
        lease_duration_secs: 3,
        renew_deadline_secs: 2,
        retry_period_secs: 1,
        ..LeaderElectionConfig::default()
    }
}

#[tokio::test]
async fn real_client_passes_lease_contract() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = unique("it-lease");
    ensure_ns(&admin, &ns).await;
    let api = KubeClientApi::new(admin, None);
    contract::lease_contract(&api, &ns, "l").await;
}

#[tokio::test]
async fn real_client_passes_config_map_contract() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = unique("it-cm");
    ensure_ns(&admin, &ns).await;
    let api = KubeClientApi::new(admin.clone(), None);
    let (a, b, n1, n2) = (admin.clone(), admin.clone(), ns.clone(), ns.clone());
    contract::config_map_contract(
        &api,
        &ns,
        "flags",
        async move |name, data| put_cm(&a, &n1, name, data).await,
        async move |name| delete_cm(&b, &n2, name).await,
    )
    .await;
}

#[tokio::test]
async fn real_client_passes_event_contract() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = unique("it-ev");
    ensure_ns(&admin, &ns).await;
    let api = KubeClientApi::new(admin.clone(), Some("pod-1".into()));
    contract::event_contract(&api, &ns, "pod-1").await;
    let events: Api<Event> = Api::namespaced(admin, &ns);
    let list = events.list(&ListParams::default()).await.unwrap();
    let ev = list
        .items
        .iter()
        .find(|e| e.reason.as_deref() == Some("ContractTest"))
        .unwrap();
    assert_eq!(
        ev.reporting_controller.as_deref(),
        Some("autumn-plugin-kubernetes")
    );
    assert_eq!(
        ev.regarding.as_ref().unwrap().name.as_deref(),
        Some("pod-1")
    );
    assert_eq!(ev.regarding.as_ref().unwrap().kind.as_deref(), Some("Pod"));
}

#[tokio::test]
async fn three_electors_on_a_real_lease() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = unique("it-elect");
    ensure_ns(&admin, &ns).await;
    let api: Arc<dyn KubeApi> = Arc::new(KubeClientApi::new(admin, None));
    let mut handles: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|id| {
            Some(
                LeaderElector::new(Arc::clone(&api), ns.clone(), *id, fast_leader("l"))
                    .unwrap()
                    .start(),
            )
        })
        .collect();
    let views: Vec<_> = handles
        .iter()
        .map(|h| h.as_ref().unwrap().leadership())
        .collect();
    let start = std::time::Instant::now();
    let mut crashed = 0;
    let mut seen = std::collections::BTreeSet::new();
    while start.elapsed() < Duration::from_secs(25) {
        let leading: Vec<_> = views.iter().filter(|v| v.is_leader()).collect();
        assert!(leading.len() <= 1, "two leaders on a real API server");
        if let Some(l) = leading.first() {
            seen.insert(l.identity().to_owned());
            if crashed < 2 && start.elapsed() > Duration::from_secs(5 + 8 * crashed) {
                let idx = views
                    .iter()
                    .position(|v| v.identity() == l.identity())
                    .unwrap();
                handles[idx].take().unwrap().abort();
                crashed += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(crashed, 2);
    assert_eq!(seen.len(), 3, "{seen:?}");
    let rec: LeaseRecord = api.get_lease(&ns, "l").await.unwrap().unwrap();
    // Two crashes give two takeovers. A slow CI disk can add one more.
    assert!(rec.transitions >= 2, "{}", rec.transitions);
    for h in handles.into_iter().flatten() {
        h.stop().await;
    }
    assert!(
        api.get_lease(&ns, "l").await.unwrap().unwrap().is_free(),
        "released"
    );
}

#[tokio::test]
async fn manifest_passes_server_side_dry_run() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = unique("it-dry");
    ensure_ns(&admin, &ns).await;
    let mut spec = ManifestSpec::new("shop", "ghcr.io/acme/shop:1.0.0");
    spec.namespace = ns;
    spec.lease_name = Some("shop-leader".into());
    spec.config_maps = vec!["flags".into()];
    spec.env.insert("RUST_LOG".into(), "info".into());
    apply(&admin, &spec.objects().unwrap(), true).await;
    // The YAML text parses and applies the same way.
    let yaml = spec.render_yaml().unwrap();
    let docs: Vec<serde_json::Value> = yaml
        .split("\n---\n")
        .map(|d| d.trim_start_matches("---\n").trim())
        .filter(|d| !d.is_empty())
        .map(|d| serde_saphyr::from_str(d).unwrap())
        .collect();
    assert_eq!(docs.len(), 6);
    apply(&admin, &docs, true).await;
}

/// The generated Role is enough for the plugin, and no more.
#[tokio::test]
#[allow(clippy::too_many_lines)] // One scenario, start to end.
async fn generated_rbac_is_enough_and_minimal() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = "it-app";
    ensure_ns(&admin, ns).await;
    // Clean state from an earlier run on this server.
    let leases: Api<k8s_openapi::api::coordination::v1::Lease> = Api::namespaced(admin.clone(), ns);
    let _ = leases.delete("shop-leader", &DeleteParams::default()).await;

    let mut spec = ManifestSpec::new("shop", "ghcr.io/acme/shop:1.0.0");
    spec.namespace = ns.into();
    spec.lease_name = Some("shop-leader".into());
    spec.config_maps = vec!["flags".into()];
    apply(&admin, &spec.objects().unwrap(), false).await;
    put_cm(
        &admin,
        ns,
        "flags",
        BTreeMap::from([("beta".into(), "true".into())]),
    )
    .await;

    // Two replicas that authenticate as the app ServiceAccount.
    let mut cfg = KubernetesConfig::default();
    cfg.leader_election = fast_leader("shop-leader");
    cfg.config_maps.watch = vec!["flags".into()];
    cfg.required = true;
    let start = |pod: &str| {
        let dir = dir.clone();
        let pod_info = PodInfo {
            name: Some(pod.to_owned()),
            namespace: Some(ns.to_owned()),
            ..PodInfo::default()
        };
        KubernetesPlugin::with_config(cfg.clone())
            .with_pod_info(pod_info)
            .with_connector(move |_| {
                let dir = dir.clone();
                async move { Ok(client(&dir, "app.kubeconfig").await) }
            })
    };
    let s1 = AppState::for_test();
    let s2 = AppState::for_test();
    let r1 = start("shop-1").start(&s1).await.unwrap();
    let r2 = start("shop-2").start(&s2).await.unwrap();
    assert_eq!(r1.mode(), "custom");
    let (l1, l2) = (r1.leadership().unwrap(), r2.leadership().unwrap());
    wait_for(Duration::from_secs(10), async || {
        l1.is_leader() || l2.is_leader()
    })
    .await;
    for _ in 0..30 {
        assert!(!(l1.is_leader() && l2.is_leader()), "two leaders");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let store = ConfigMapStore::from_state(&s1).unwrap();
    wait_for(Duration::from_secs(10), async || store.is_synced("flags")).await;
    assert_eq!(store.json::<bool>("flags", "beta").unwrap(), Some(true));
    put_cm(
        &admin,
        ns,
        "flags",
        BTreeMap::from([("beta".into(), "false".into())]),
    )
    .await;
    wait_for(Duration::from_secs(10), async || {
        store.value("flags", "beta").as_deref() == Some("false")
    })
    .await;

    let out = r1.health().check().await;
    assert_eq!(out.status, HealthStatus::Up, "{:?}", out.details);
    assert_eq!(r1.metrics().snapshot().lease_errors, 0, "no RBAC errors");

    // Events from the plugin, written with the app identity.
    let events: Api<Event> = Api::namespaced(admin.clone(), ns);
    wait_for(Duration::from_secs(10), async || {
        let list = events.list(&ListParams::default()).await.unwrap();
        list.items
            .iter()
            .any(|e| e.reason.as_deref() == Some("LeaderElected"))
            && list
                .items
                .iter()
                .filter(|e| e.reason.as_deref() == Some("Started"))
                .count()
                >= 2
    })
    .await;

    // Hand over: stop the leader. The other takes over fast (release).
    let (leader_rt, other, other_rt) = if l1.is_leader() {
        (r1, l2, r2)
    } else {
        (r2, l1, r1)
    };
    let t0 = std::time::Instant::now();
    // Release comes before the Stopping event, so the handover does not
    // wait for the event write. Expect release plus one retry (1.2 s).
    let stop = tokio::spawn(leader_rt.shutdown());
    wait_for(Duration::from_secs(5), async || other.is_leader()).await;
    assert!(t0.elapsed() < Duration::from_secs(3), "{:?}", t0.elapsed());
    stop.await.unwrap();

    // Least privilege: another lease name is denied.
    let app = KubeClientApi::new(client(&dir, "app.kubeconfig").await, None);
    let err = app.get_lease(ns, "other-lease").await.unwrap_err();
    assert!(matches!(err, KubeError::Forbidden { .. }), "{err}");
    let err = app.get_lease("default", "shop-leader").await.unwrap_err();
    assert!(matches!(err, KubeError::Forbidden { .. }), "{err}");
    contract::denied_watch_contract(&app, ns, "secret-settings").await;
    // `create` is limited to the lease name too: no squatting on others.
    let err = app
        .create_lease(ns, "another-app-leader", &LeaseRecord::default())
        .await
        .unwrap_err();
    assert!(matches!(err, KubeError::Forbidden { .. }), "{err}");

    other_rt.shutdown().await;
}

#[tokio::test]
async fn plugin_health_on_real_server() {
    let Some(dir) = it_dir() else { return };
    let admin_dir = dir.clone();
    let state = AppState::for_test();
    let rt = KubernetesPlugin::with_config(KubernetesConfig {
        required: true,
        ..KubernetesConfig::default()
    })
    .with_pod_info(PodInfo::default())
    .with_connector(move |_| {
        let dir = admin_dir.clone();
        async move { Ok(client(&dir, "admin.kubeconfig").await) }
    })
    .start(&state)
    .await
    .unwrap();
    assert!(
        state.extension::<kube::Client>().is_some(),
        "the app gets the kube::Client"
    );
    let out = rt.health().check().await;
    assert_eq!(out.status, HealthStatus::Up);
    assert_eq!(out.details["mode"], "custom");
    assert!(!out.details.contains_key("version"));
    assert_eq!(rt.metrics().snapshot().api_up, 1, "background refresh");
    assert_eq!(rt.namespace(), Some("default"), "kubeconfig namespace");
    rt.shutdown().await;
}

/// A renew must keep fields that other writers set (`GitOps` labels, owner
/// references, coordinated leader election fields).
#[tokio::test]
async fn replace_keeps_metadata_and_other_spec_fields() {
    let Some(dir) = it_dir() else { return };
    let admin = client(&dir, "admin.kubeconfig").await;
    let ns = unique("it-keep");
    ensure_ns(&admin, &ns).await;
    let leases: Api<k8s_openapi::api::coordination::v1::Lease> =
        Api::namespaced(admin.clone(), &ns);
    let lease: k8s_openapi::api::coordination::v1::Lease =
        serde_json::from_value(serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "l",
                "labels": {"app.kubernetes.io/part-of": "shop"},
                "annotations": {"argocd.argoproj.io/tracking-id": "x"}
            },
            "spec": {"holderIdentity": "a", "leaseDurationSeconds": 15}
        }))
        .unwrap();
    leases.create(&PostParams::default(), &lease).await.unwrap();
    let api = KubeClientApi::new(admin, None);
    let mut rec = api.get_lease(&ns, "l").await.unwrap().unwrap();
    rec.holder = Some("b".into());
    rec.transitions = 1;
    api.replace_lease(&ns, "l", &rec).await.unwrap();
    let after = leases.get("l").await.unwrap();
    let meta = after.metadata;
    assert_eq!(
        meta.labels.unwrap()["app.kubernetes.io/part-of"],
        "shop",
        "labels kept"
    );
    assert_eq!(
        meta.annotations.unwrap()["argocd.argoproj.io/tracking-id"],
        "x"
    );
    assert_eq!(after.spec.unwrap().holder_identity.as_deref(), Some("b"));
}

/// The default connect path (kubeconfig) end to end: run `examples/app` with
/// `KUBECONFIG` and read its health (AC3).
#[tokio::test]
async fn example_app_connects_with_kubeconfig() {
    use std::io::{Read, Write};
    let Some(dir) = it_dir() else { return };
    let app = common::example_bin("app");
    // A killed run on this server leaves the lease held. Start clean.
    let admin = client(&dir, "admin.kubeconfig").await;
    let leases: Api<k8s_openapi::api::coordination::v1::Lease> = Api::namespaced(admin, "default");
    let _ = leases
        .delete("example-leader", &DeleteParams::default())
        .await;
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let work = std::env::temp_dir().join(format!("akp-app-{}", std::process::id()));
    std::fs::create_dir_all(&work).unwrap();
    // No HTTPS_PROXY in the child env: the test server is on 127.0.0.1.
    let mut child = std::process::Command::new(app)
        .current_dir(&work)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", &work)
        .env("KUBECONFIG", dir.join("admin.kubeconfig"))
        .env("AUTUMN_MANIFEST_DIR", &work)
        .env("AUTUMN_SERVER__PORT", port.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let get = |path: &str| -> Option<String> {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
        write!(
            s,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        .ok()?;
        let mut body = String::new();
        s.read_to_string(&mut body).ok()?;
        Some(body)
    };
    let mut health = String::new();
    for _ in 0..300 {
        if let Some(b) = get("/actuator/health")
            && b.contains("\"leading\":true")
        {
            health = b;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&work);
    assert!(health.contains("\"mode\":\"kubeconfig\""), "{health}");
    assert!(health.contains("\"lease\":\"example-leader\""), "{health}");
}
