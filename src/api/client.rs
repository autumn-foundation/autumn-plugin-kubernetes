//! [`KubeApi`] on a real `kube::Client`.

use futures::StreamExt;
use futures::stream::BoxStream;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{ConfigMap, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, PostParams};
use kube::runtime::WatchStreamExt;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::runtime::watcher;

use super::{ApiFuture, ConfigMapEvent, EventKind, KubeApi, LeaseRecord, PodEvent, PodRef};
use crate::error::KubeError;

/// Controller name on published events.
pub const REPORTER: &str = "autumn-plugin-kubernetes";

/// [`KubeApi`] on a real `kube::Client`.
#[derive(Clone)]
pub struct KubeClientApi {
    client: kube::Client,
    recorder: Recorder,
}

impl std::fmt::Debug for KubeClientApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeClientApi")
            .field("namespace", &self.client.default_namespace())
            .finish_non_exhaustive()
    }
}

impl KubeClientApi {
    /// Wraps a client. `instance` names this replica on events (the pod name).
    #[must_use]
    pub fn new(client: kube::Client, instance: Option<String>) -> Self {
        let recorder = Recorder::new(
            client.clone(),
            Reporter {
                controller: REPORTER.to_owned(),
                instance,
            },
        );
        Self { client, recorder }
    }

    /// Connects in cluster, or with the kubeconfig (`KUBECONFIG` or
    /// `~/.kube/config`).
    ///
    /// # Errors
    /// Returns [`KubeError::NoCluster`] when neither is found, and
    /// [`KubeError::Api`] when the config is bad.
    pub async fn connect(instance: Option<String>) -> Result<Self, KubeError> {
        let config = kube::Config::infer()
            .await
            .map_err(|e| KubeError::NoCluster(e.to_string()))?;
        let client = kube::Client::try_from(config).map_err(|e| map_error(&e, "", ""))?;
        Ok(Self::new(client, instance))
    }

    fn leases(&self, namespace: &str) -> Api<Lease> {
        Api::namespaced(self.client.clone(), namespace)
    }
}

/// Maps a `kube` error to a [`KubeError`]. `verb` and `resource` name the
/// call for a 403.
pub(crate) fn map_error(e: &kube::Error, verb: &str, resource: &str) -> KubeError {
    match e {
        kube::Error::Api(status) if status.is_conflict() || status.is_already_exists() => {
            KubeError::Conflict(status.message.clone())
        }
        kube::Error::Api(status) if status.is_not_found() => {
            KubeError::NotFound(status.message.clone())
        }
        kube::Error::Api(status) if status.is_forbidden() => KubeError::Forbidden {
            verb: verb.to_owned(),
            resource: resource.to_owned(),
        },
        other => KubeError::Api(other.to_string()),
    }
}

const LEASES: &str = "leases.coordination.k8s.io";

pub(crate) fn to_lease(name: &str, record: &LeaseRecord) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            resource_version: record.resource_version.clone(),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: record.holder.clone().filter(|h| !h.is_empty()),
            lease_duration_seconds: Some(record.lease_duration_secs),
            acquire_time: record.acquire_time.map(MicroTime),
            renew_time: record.renew_time.map(MicroTime),
            lease_transitions: Some(i32::try_from(record.transitions).unwrap_or(i32::MAX)),
            ..LeaseSpec::default()
        }),
    }
}

pub(crate) fn from_lease(lease: Lease) -> LeaseRecord {
    let spec = lease.spec.unwrap_or_default();
    LeaseRecord {
        holder: spec.holder_identity.filter(|h| !h.is_empty()),
        lease_duration_secs: spec.lease_duration_seconds.unwrap_or(0),
        acquire_time: spec.acquire_time.map(|t| t.0),
        renew_time: spec.renew_time.map(|t| t.0),
        transitions: spec
            .lease_transitions
            .and_then(|t| u32::try_from(t).ok())
            .unwrap_or(0),
        resource_version: lease.metadata.resource_version,
    }
}

impl KubeApi for KubeClientApi {
    fn server_version(&self) -> ApiFuture<'_, String> {
        Box::pin(async move {
            self.client
                .apiserver_version()
                .await
                .map(|v| v.git_version)
                .map_err(|e| map_error(&e, "get", "/version"))
        })
    }

    fn get_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> ApiFuture<'a, Option<LeaseRecord>> {
        Box::pin(async move {
            self.leases(namespace)
                .get_opt(name)
                .await
                .map(|l| l.map(from_lease))
                .map_err(|e| map_error(&e, "get", LEASES))
        })
    }

    fn create_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        record: &'a LeaseRecord,
    ) -> ApiFuture<'a, LeaseRecord> {
        Box::pin(async move {
            let mut lease = to_lease(name, record);
            lease.metadata.resource_version = None;
            self.leases(namespace)
                .create(&PostParams::default(), &lease)
                .await
                .map(from_lease)
                .map_err(|e| map_error(&e, "create", LEASES))
        })
    }

    fn replace_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        record: &'a LeaseRecord,
    ) -> ApiFuture<'a, LeaseRecord> {
        Box::pin(async move {
            if record.resource_version.is_none() {
                return Err(KubeError::Conflict(
                    "replace needs a resourceVersion".to_owned(),
                ));
            }
            self.leases(namespace)
                .replace(name, &PostParams::default(), &to_lease(name, record))
                .await
                .map(from_lease)
                .map_err(|e| map_error(&e, "update", LEASES))
        })
    }

    fn watch_config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> BoxStream<'static, Result<ConfigMapEvent, KubeError>> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        let config = watcher::Config::default().fields(&format!("metadata.name={name}"));
        // A relist starts with `Init`. `InitDone` with no object means the
        // ConfigMap is gone.
        let mut seen = false;
        watcher(api, config)
            .default_backoff()
            .filter_map(move |item| {
                let out = match item {
                    Ok(watcher::Event::Init) => {
                        seen = false;
                        None
                    }
                    Ok(watcher::Event::InitApply(cm) | watcher::Event::Apply(cm)) => {
                        seen = true;
                        Some(Ok(ConfigMapEvent::Applied(cm.data.unwrap_or_default())))
                    }
                    Ok(watcher::Event::InitDone) => (!seen).then_some(Ok(ConfigMapEvent::Deleted)),
                    Ok(watcher::Event::Delete(_)) => {
                        seen = false;
                        Some(Ok(ConfigMapEvent::Deleted))
                    }
                    Err(e) => Some(Err(KubeError::Api(e.to_string()))),
                };
                std::future::ready(out)
            })
            .boxed()
    }

    fn publish_event<'a>(&'a self, pod: &'a PodRef, event: &'a PodEvent) -> ApiFuture<'a, ()> {
        Box::pin(async move {
            let reference = ObjectReference {
                api_version: Some("v1".to_owned()),
                kind: Some("Pod".to_owned()),
                name: Some(pod.name.clone()),
                namespace: Some(pod.namespace.clone()),
                uid: pod.uid.clone(),
                ..ObjectReference::default()
            };
            let ev = Event {
                type_: match event.kind {
                    EventKind::Normal => EventType::Normal,
                    EventKind::Warning => EventType::Warning,
                },
                reason: event.reason.clone(),
                note: event.note.clone(),
                action: event.action.clone(),
                secondary: None,
            };
            self.recorder
                .publish(&ev, &reference)
                .await
                .map_err(|e| map_error(&e, "create", "events.events.k8s.io"))
        })
    }

    fn default_namespace(&self) -> String {
        self.client.default_namespace().to_owned()
    }

    fn client(&self) -> Option<kube::Client> {
        Some(self.client.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::jiff::Timestamp;

    #[test]
    fn lease_round_trip() {
        let rec = LeaseRecord {
            holder: Some("a".into()),
            lease_duration_secs: 15,
            acquire_time: Some(Timestamp::UNIX_EPOCH),
            renew_time: Some(Timestamp::UNIX_EPOCH),
            transitions: 3,
            resource_version: Some("7".into()),
        };
        let lease = to_lease("l", &rec);
        assert_eq!(lease.metadata.name.as_deref(), Some("l"));
        assert_eq!(from_lease(lease), rec);
    }

    #[test]
    fn empty_holder_is_none_both_ways() {
        let rec = LeaseRecord {
            holder: Some(String::new()),
            ..LeaseRecord::default()
        };
        let lease = to_lease("l", &rec);
        assert_eq!(lease.spec.as_ref().unwrap().holder_identity, None);
        assert_eq!(from_lease(lease).holder, None);
    }

    #[test]
    fn bad_transitions_clamp() {
        let rec = LeaseRecord {
            transitions: u32::MAX,
            ..LeaseRecord::default()
        };
        let mut lease = to_lease("l", &rec);
        assert_eq!(
            lease.spec.as_ref().unwrap().lease_transitions,
            Some(i32::MAX)
        );
        lease.spec.as_mut().unwrap().lease_transitions = Some(-1);
        assert_eq!(from_lease(lease).transitions, 0);
        assert_eq!(from_lease(Lease::default()), LeaseRecord::default());
    }

    #[test]
    fn errors_map_by_status() {
        let status = |code: u16, reason: &str| {
            kube::Error::Api(
                kube::core::Status {
                    code,
                    reason: reason.to_owned(),
                    message: "m".to_owned(),
                    ..kube::core::Status::failure("m", reason)
                }
                .boxed(),
            )
        };
        assert!(matches!(
            map_error(&status(409, "Conflict"), "update", LEASES),
            KubeError::Conflict(_)
        ));
        assert!(matches!(
            map_error(&status(409, "AlreadyExists"), "create", LEASES),
            KubeError::Conflict(_)
        ));
        assert!(matches!(
            map_error(&status(404, "NotFound"), "get", LEASES),
            KubeError::NotFound(_)
        ));
        let forbidden = map_error(&status(403, "Forbidden"), "update", LEASES);
        assert_eq!(
            forbidden,
            KubeError::Forbidden {
                verb: "update".into(),
                resource: LEASES.into()
            }
        );
        assert!(matches!(
            map_error(&status(500, "InternalError"), "get", LEASES),
            KubeError::Api(_)
        ));
    }
}
