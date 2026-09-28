//! In-memory fake of the Kubernetes API.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use futures::stream::BoxStream;
use tokio::sync::watch;

use super::{ApiFuture, ConfigMapEvent, KubeApi, LeaseRecord, PodEvent, PodRef};
use crate::error::KubeError;

type Key = (String, String);

#[derive(Default)]
struct State {
    leases: HashMap<Key, LeaseRecord>,
    next_version: u64,
    down: bool,
    failing_lease_writes: u32,
    forbidden_leases: bool,
    events_failing: bool,
    config_maps: HashMap<Key, watch::Sender<Option<BTreeMap<String, String>>>>,
    events: Vec<(PodRef, PodEvent)>,
    lease_writes: u64,
    latency: std::time::Duration,
}

/// In-memory fake of the Kubernetes API. Clones share state.
///
/// It follows the API server rules: create fails when the Lease exists, and
/// replace needs the current `resourceVersion`.
#[derive(Clone, Default)]
pub struct MemoryKubeApi {
    state: Arc<Mutex<State>>,
    namespace: Arc<str>,
}

impl std::fmt::Debug for MemoryKubeApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryKubeApi")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

fn key(namespace: &str, name: &str) -> Key {
    (namespace.to_owned(), name.to_owned())
}

fn down_error() -> KubeError {
    KubeError::Api("connection refused (MemoryKubeApi is down)".to_owned())
}

impl MemoryKubeApi {
    /// Makes an empty fake. The default namespace is `default`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::default(),
            namespace: Arc::from("default"),
        }
    }

    /// Sets the default namespace.
    #[must_use]
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespace = Arc::from(namespace);
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns the stored Lease.
    #[must_use]
    pub fn lease(&self, namespace: &str, name: &str) -> Option<LeaseRecord> {
        self.lock().leases.get(&key(namespace, name)).cloned()
    }

    /// Writes a Lease as another writer would. It gets a new version.
    pub fn put_lease(&self, namespace: &str, name: &str, mut record: LeaseRecord) {
        let mut s = self.lock();
        s.next_version += 1;
        record.resource_version = Some(s.next_version.to_string());
        s.leases.insert(key(namespace, name), record);
    }

    /// Number of successful Lease creates and replaces.
    #[must_use]
    pub fn lease_writes(&self) -> u64 {
        self.lock().lease_writes
    }

    /// When `true`, every call fails like a network outage.
    pub fn set_down(&self, down: bool) {
        self.lock().down = down;
    }

    /// The next `n` Lease writes fail with an API error.
    pub fn fail_lease_writes(&self, n: u32) {
        self.lock().failing_lease_writes = n;
    }

    /// When `true`, Lease calls fail with HTTP 403.
    pub fn set_leases_forbidden(&self, forbidden: bool) {
        self.lock().forbidden_leases = forbidden;
    }

    /// Every Lease call and version check waits this long first.
    pub fn set_latency(&self, latency: std::time::Duration) {
        self.lock().latency = latency;
    }

    fn latency(&self) -> std::time::Duration {
        self.lock().latency
    }

    /// When `true`, event writes fail.
    pub fn set_events_failing(&self, failing: bool) {
        self.lock().events_failing = failing;
    }

    fn config_map_sender(
        &self,
        namespace: &str,
        name: &str,
    ) -> watch::Sender<Option<BTreeMap<String, String>>> {
        self.lock()
            .config_maps
            .entry(key(namespace, name))
            .or_insert_with(|| watch::channel(None).0)
            .clone()
    }

    /// Creates or updates a ConfigMap.
    pub fn put_config_map(&self, namespace: &str, name: &str, data: BTreeMap<String, String>) {
        self.config_map_sender(namespace, name)
            .send_replace(Some(data));
    }

    /// Deletes a ConfigMap.
    pub fn delete_config_map(&self, namespace: &str, name: &str) {
        self.config_map_sender(namespace, name).send_replace(None);
    }

    /// Returns the published events.
    #[must_use]
    pub fn events(&self) -> Vec<(PodRef, PodEvent)> {
        self.lock().events.clone()
    }

    /// Checks outage and RBAC faults for a Lease call.
    fn lease_guard(s: &State, verb: &str) -> Result<(), KubeError> {
        if s.down {
            return Err(down_error());
        }
        if s.forbidden_leases {
            return Err(KubeError::Forbidden {
                verb: verb.to_owned(),
                resource: "leases.coordination.k8s.io".to_owned(),
            });
        }
        Ok(())
    }

    /// Checks faults for a Lease write. Uses up one injected failure.
    fn write_guard(s: &mut State, verb: &str) -> Result<(), KubeError> {
        Self::lease_guard(s, verb)?;
        if s.failing_lease_writes > 0 {
            s.failing_lease_writes -= 1;
            return Err(KubeError::Api("injected lease write failure".to_owned()));
        }
        Ok(())
    }

    fn record_event(&self, pod: &PodRef, event: &PodEvent) -> Result<(), KubeError> {
        let mut s = self.lock();
        if s.down {
            return Err(down_error());
        }
        if s.events_failing {
            return Err(KubeError::Api("injected event failure".to_owned()));
        }
        s.events.push((pod.clone(), event.clone()));
        drop(s);
        Ok(())
    }

    fn store(s: &mut State, k: Key, record: &LeaseRecord) -> LeaseRecord {
        s.next_version += 1;
        s.lease_writes += 1;
        let mut stored = record.clone();
        stored.resource_version = Some(s.next_version.to_string());
        s.leases.insert(k, stored.clone());
        stored
    }
}

impl MemoryKubeApi {
    fn do_get(&self, namespace: &str, name: &str) -> Result<Option<LeaseRecord>, KubeError> {
        let s = self.lock();
        Self::lease_guard(&s, "get")?;
        Ok(s.leases.get(&key(namespace, name)).cloned())
    }

    fn do_create(
        &self,
        namespace: &str,
        name: &str,
        record: &LeaseRecord,
    ) -> Result<LeaseRecord, KubeError> {
        let mut s = self.lock();
        Self::write_guard(&mut s, "create")?;
        let k = key(namespace, name);
        let result = if s.leases.contains_key(&k) {
            Err(KubeError::Conflict(format!(
                "leases.coordination.k8s.io \"{name}\" already exists"
            )))
        } else {
            Ok(Self::store(&mut s, k, record))
        };
        drop(s);
        result
    }

    fn do_replace(
        &self,
        namespace: &str,
        name: &str,
        record: &LeaseRecord,
    ) -> Result<LeaseRecord, KubeError> {
        let mut s = self.lock();
        Self::write_guard(&mut s, "update")?;
        let k = key(namespace, name);
        let result = match s.leases.get(&k) {
            // Like the API server: Leases allow create on update.
            None if record.resource_version.is_some() => Ok(Self::store(&mut s, k, record)),
            None => Err(KubeError::Conflict(format!(
                "leases.coordination.k8s.io \"{name}\": no resourceVersion"
            ))),
            Some(current)
                if record.resource_version.is_none()
                    || record.resource_version != current.resource_version =>
            {
                Err(KubeError::Conflict(format!(
                    "Operation cannot be fulfilled on leases.coordination.k8s.io \"{name}\": \
                     the object has been modified"
                )))
            }
            Some(_) => Ok(Self::store(&mut s, k, record)),
        };
        drop(s);
        result
    }
}

async fn wait(latency: std::time::Duration) {
    if !latency.is_zero() {
        tokio::time::sleep(latency).await;
    }
}

impl KubeApi for MemoryKubeApi {
    fn server_version(&self) -> ApiFuture<'_, String> {
        let latency = self.latency();
        Box::pin(async move {
            wait(latency).await;
            if self.lock().down {
                Err(down_error())
            } else {
                Ok("v1.34.0-memory".to_owned())
            }
        })
    }

    fn get_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> ApiFuture<'a, Option<LeaseRecord>> {
        let latency = self.latency();
        Box::pin(async move {
            wait(latency).await;
            self.do_get(namespace, name)
        })
    }

    fn create_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        record: &'a LeaseRecord,
    ) -> ApiFuture<'a, LeaseRecord> {
        let latency = self.latency();
        Box::pin(async move {
            wait(latency).await;
            self.do_create(namespace, name, record)
        })
    }

    fn replace_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        record: &'a LeaseRecord,
    ) -> ApiFuture<'a, LeaseRecord> {
        let latency = self.latency();
        Box::pin(async move {
            wait(latency).await;
            self.do_replace(namespace, name, record)
        })
    }

    fn watch_config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> BoxStream<'static, Result<ConfigMapEvent, KubeError>> {
        if self.lock().down {
            return Box::pin(futures::stream::once(async { Err(down_error()) }));
        }
        let rx = self.config_map_sender(namespace, name).subscribe();
        // First item: the current state. Then one item per change.
        Box::pin(futures::stream::unfold(
            (rx, true),
            |(mut rx, first)| async move {
                if !first && rx.changed().await.is_err() {
                    return None;
                }
                let event = rx
                    .borrow_and_update()
                    .clone()
                    .map_or(ConfigMapEvent::Deleted, ConfigMapEvent::Applied);
                Some((Ok(event), (rx, false)))
            },
        ))
    }

    fn publish_event<'a>(&'a self, pod: &'a PodRef, event: &'a PodEvent) -> ApiFuture<'a, ()> {
        let result = self.record_event(pod, event);
        Box::pin(async move { result })
    }

    fn default_namespace(&self) -> String {
        self.namespace.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::EventKind;
    use futures::StreamExt;

    fn rec(holder: &str) -> LeaseRecord {
        LeaseRecord {
            holder: Some(holder.to_owned()),
            lease_duration_secs: 15,
            ..LeaseRecord::default()
        }
    }

    #[tokio::test]
    async fn create_then_get_sets_a_version() {
        let api = MemoryKubeApi::new();
        assert_eq!(api.get_lease("ns", "l").await.unwrap(), None);
        let created = api.create_lease("ns", "l", &rec("a")).await.unwrap();
        assert!(created.resource_version.is_some());
        assert_eq!(api.get_lease("ns", "l").await.unwrap(), Some(created));
        assert_eq!(api.lease_writes(), 1);
    }

    #[tokio::test]
    async fn create_twice_is_a_conflict() {
        let api = MemoryKubeApi::new();
        api.create_lease("ns", "l", &rec("a")).await.unwrap();
        let err = api.create_lease("ns", "l", &rec("b")).await.unwrap_err();
        assert!(matches!(err, KubeError::Conflict(_)), "{err}");
        assert!(api.lease("ns", "l").unwrap().held_by("a"));
    }

    #[tokio::test]
    async fn replace_needs_current_version() {
        let api = MemoryKubeApi::new();
        let v1 = api.create_lease("ns", "l", &rec("a")).await.unwrap();
        let mut next = v1.clone();
        next.holder = Some("b".into());
        let v2 = api.replace_lease("ns", "l", &next).await.unwrap();
        assert_ne!(v1.resource_version, v2.resource_version);
        // Old version loses.
        let err = api.replace_lease("ns", "l", &v1).await.unwrap_err();
        assert!(matches!(err, KubeError::Conflict(_)), "{err}");
        // No version loses.
        let mut none = v2.clone();
        none.resource_version = None;
        assert!(matches!(
            api.replace_lease("ns", "l", &none).await,
            Err(KubeError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn replace_missing_creates_like_the_api_server() {
        let api = MemoryKubeApi::new();
        let err = api.replace_lease("ns", "l", &rec("a")).await.unwrap_err();
        assert!(matches!(err, KubeError::Conflict(_)), "no version: {err}");
        let mut with_version = rec("a");
        with_version.resource_version = Some("9".into());
        let made = api.replace_lease("ns", "l", &with_version).await.unwrap();
        assert!(made.held_by("a"));
    }

    #[tokio::test]
    async fn put_lease_bumps_version() {
        let api = MemoryKubeApi::new();
        let v1 = api.create_lease("ns", "l", &rec("a")).await.unwrap();
        api.put_lease("ns", "l", rec("other"));
        let now = api.lease("ns", "l").unwrap();
        assert!(now.held_by("other"));
        assert_ne!(now.resource_version, v1.resource_version);
        assert!(matches!(
            api.replace_lease("ns", "l", &v1).await,
            Err(KubeError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn outage_and_faults() {
        let api = MemoryKubeApi::new();
        api.set_down(true);
        assert!(matches!(api.server_version().await, Err(KubeError::Api(_))));
        assert!(api.get_lease("ns", "l").await.is_err());
        api.set_down(false);
        assert!(api.server_version().await.unwrap().starts_with('v'));

        api.fail_lease_writes(1);
        assert!(matches!(
            api.create_lease("ns", "l", &rec("a")).await,
            Err(KubeError::Api(_))
        ));
        api.create_lease("ns", "l", &rec("a")).await.unwrap();

        api.set_leases_forbidden(true);
        let err = api.get_lease("ns", "l").await.unwrap_err();
        assert!(matches!(err, KubeError::Forbidden { .. }), "{err}");
    }

    #[tokio::test]
    async fn config_map_watch_sees_state_then_changes() {
        let api = MemoryKubeApi::new();
        let data = BTreeMap::from([("k".to_owned(), "v1".to_owned())]);
        api.put_config_map("ns", "flags", data.clone());
        let mut w = api.watch_config_map("ns", "flags");
        assert_eq!(
            w.next().await.unwrap().unwrap(),
            ConfigMapEvent::Applied(data)
        );
        let data2 = BTreeMap::from([("k".to_owned(), "v2".to_owned())]);
        api.put_config_map("ns", "flags", data2.clone());
        assert_eq!(
            w.next().await.unwrap().unwrap(),
            ConfigMapEvent::Applied(data2)
        );
        api.delete_config_map("ns", "flags");
        assert_eq!(w.next().await.unwrap().unwrap(), ConfigMapEvent::Deleted);
    }

    #[tokio::test]
    async fn watch_of_missing_config_map_starts_deleted() {
        let api = MemoryKubeApi::new();
        let mut w = api.watch_config_map("ns", "none");
        assert_eq!(w.next().await.unwrap().unwrap(), ConfigMapEvent::Deleted);
    }

    #[tokio::test]
    async fn events_are_recorded_or_fail() {
        let api = MemoryKubeApi::new().with_namespace("shop");
        assert_eq!(api.default_namespace(), "shop");
        let pod = PodRef {
            namespace: "shop".into(),
            name: "p".into(),
            uid: None,
        };
        let ev = PodEvent {
            kind: EventKind::Normal,
            reason: "Started".into(),
            action: "Start".into(),
            note: None,
        };
        api.publish_event(&pod, &ev).await.unwrap();
        assert_eq!(api.events(), vec![(pod.clone(), ev.clone())]);
        api.set_events_failing(true);
        assert!(api.publish_event(&pod, &ev).await.is_err());
        assert_eq!(api.events().len(), 1);
    }

    #[test]
    fn default_namespace_is_default() {
        assert_eq!(MemoryKubeApi::new().default_namespace(), "default");
        assert!(format!("{:?}", MemoryKubeApi::new()).contains("default"));
    }
}
