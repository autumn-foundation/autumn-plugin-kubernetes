//! One contract for every `KubeApi`. The fake and the real client must both
//! pass it. This keeps `MemoryKubeApi` true to the API server.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use autumn_plugin_kubernetes::KubeError;
use autumn_plugin_kubernetes::api::{
    ConfigMapEvent, EventKind, KubeApi, LeaseRecord, PodEvent, PodRef,
};
use futures::StreamExt;
use k8s_openapi::jiff::Timestamp;

fn record(holder: &str) -> LeaseRecord {
    LeaseRecord {
        holder: Some(holder.to_owned()),
        lease_duration_secs: 15,
        acquire_time: Some(Timestamp::now()),
        renew_time: Some(Timestamp::now()),
        transitions: 0,
        resource_version: None,
    }
}

/// Lease rules: get missing, create, create twice, replace, stale replace,
/// replace with no version, replace missing, round trip of all fields.
pub async fn lease_contract(api: &dyn KubeApi, ns: &str, name: &str) {
    assert_eq!(api.get_lease(ns, name).await.unwrap(), None);
    let created = api.create_lease(ns, name, &record("a")).await.unwrap();
    assert!(created.held_by("a"));
    assert!(created.resource_version.is_some());
    assert_eq!(created.lease_duration_secs, 15);
    let read = api.get_lease(ns, name).await.unwrap().unwrap();
    assert_eq!(read, created, "read returns what create stored");

    let err = api.create_lease(ns, name, &record("b")).await.unwrap_err();
    assert!(matches!(err, KubeError::Conflict(_)), "create twice: {err}");

    let mut next = read.clone();
    next.holder = Some("b".into());
    next.transitions = 1;
    let replaced = api.replace_lease(ns, name, &next).await.unwrap();
    assert!(replaced.held_by("b"));
    assert_eq!(replaced.transitions, 1);
    assert_ne!(replaced.resource_version, read.resource_version);

    let err = api.replace_lease(ns, name, &read).await.unwrap_err();
    assert!(
        matches!(err, KubeError::Conflict(_)),
        "stale replace: {err}"
    );

    let mut none = replaced.clone();
    none.resource_version = None;
    let err = api.replace_lease(ns, name, &none).await.unwrap_err();
    assert!(matches!(err, KubeError::Conflict(_)), "no version: {err}");

    let mut free = replaced.clone();
    free.holder = None;
    let freed = api.replace_lease(ns, name, &free).await.unwrap();
    assert!(freed.is_free());

    // A replace is a patch with a version check. It does not create.
    let missing = format!("{name}-missing");
    let mut ghost = record("a");
    ghost.resource_version = Some("1".into());
    let err = api.replace_lease(ns, &missing, &ghost).await.unwrap_err();
    assert!(matches!(err, KubeError::NotFound(_)), "missing: {err}");
    assert_eq!(api.get_lease(ns, &missing).await.unwrap(), None);
}

/// ConfigMap watch rules: first item is the state, then each change. A
/// change to another ConfigMap is not seen.
pub async fn config_map_contract(
    api: &dyn KubeApi,
    ns: &str,
    name: &str,
    put: impl AsyncFn(&str, BTreeMap<String, String>),
    delete: impl AsyncFn(&str),
) {
    let limit = Duration::from_secs(20);
    let mut w = api.watch_config_map(ns, name);
    let next =
        async |w: &mut futures::stream::BoxStream<'static, Result<ConfigMapEvent, KubeError>>| {
            tokio::time::timeout(limit, w.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        };
    assert_eq!(
        next(&mut w).await,
        ConfigMapEvent::Deleted,
        "missing at start"
    );
    put(name, BTreeMap::from([("k".into(), "v1".into())])).await;
    assert_eq!(
        next(&mut w).await,
        ConfigMapEvent::Applied(BTreeMap::from([("k".into(), "v1".into())]))
    );
    let other = format!("{name}-other");
    put(&other, BTreeMap::from([("x".into(), "y".into())])).await;
    put(name, BTreeMap::from([("k".into(), "v2".into())])).await;
    assert_eq!(
        next(&mut w).await,
        ConfigMapEvent::Applied(BTreeMap::from([("k".into(), "v2".into())])),
        "the other ConfigMap is not seen"
    );
    delete(name).await;
    assert_eq!(next(&mut w).await, ConfigMapEvent::Deleted);
}

/// Event write rule.
pub async fn event_contract(api: &dyn KubeApi, ns: &str, pod: &str) {
    let pod = PodRef {
        namespace: ns.to_owned(),
        name: pod.to_owned(),
        uid: None,
    };
    let ev = PodEvent {
        kind: EventKind::Normal,
        reason: "ContractTest".into(),
        action: "Test".into(),
        note: Some("contract".into()),
    };
    api.publish_event(&pod, &ev).await.unwrap();
    assert!(api.server_version().await.unwrap().starts_with('v'));
}

/// A watch that RBAC denies yields `Forbidden` and stays open: it retries.
pub async fn denied_watch_contract(api: &dyn KubeApi, ns: &str, name: &str) {
    let mut w = api.watch_config_map(ns, name);
    for n in 0..2 {
        let item = tokio::time::timeout(Duration::from_secs(20), w.next())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("stream ended at item {n}"));
        let err = item.unwrap_err();
        assert!(
            matches!(err, KubeError::Forbidden { .. }),
            "item {n}: {err}"
        );
    }
}
