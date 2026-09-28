//! [`KubeApi`] on a real `kube::Client`.

use futures::StreamExt;
use futures::stream::BoxStream;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{ConfigMap, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, Patch, PatchParams, PostParams};
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
    /// Returns [`KubeError::NoCluster`] when there is no kubeconfig and no
    /// pod. Returns [`KubeError::Api`] when a config is present but bad.
    pub async fn connect(instance: Option<String>) -> Result<Self, KubeError> {
        let config = kube::Config::infer()
            .await
            .map_err(|e| classify_infer_error(cluster_config_present(), &e.to_string()))?;
        let client = kube::Client::try_from(config).map_err(|e| map_error(&e, "", ""))?;
        Ok(Self::new(client, instance))
    }

    fn leases(&self, namespace: &str) -> Api<Lease> {
        Api::namespaced(self.client.clone(), namespace)
    }
}

/// `true` when a kubeconfig file exists or this process runs in a pod. Then
/// a config error is a real error, not "no cluster".
fn cluster_config_present() -> bool {
    kubeconfig_present() || std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
}

/// Config inference failed. With no kubeconfig and no pod, there is no
/// cluster. With one of them, the config is bad: an error, not detached mode.
pub(crate) fn classify_infer_error(config_present: bool, message: &str) -> KubeError {
    let message = scrub(message);
    if config_present {
        KubeError::Api(format!(
            "kubernetes config is present but not usable: {message}"
        ))
    } else {
        KubeError::NoCluster(message)
    }
}

/// The mode name. kube reads a kubeconfig first, then the in-cluster config.
pub(crate) const fn mode_for(kubeconfig_present: bool) -> &'static str {
    if kubeconfig_present {
        "kubeconfig"
    } else {
        "in_cluster"
    }
}

/// `true` when a kubeconfig file exists: a path in `KUBECONFIG` (a list), or
/// `~/.kube/config` when `KUBECONFIG` is not set. kube reads it first.
pub(crate) fn kubeconfig_present() -> bool {
    kubeconfig_file_exists(
        std::env::var_os("KUBECONFIG").as_deref(),
        std::env::var_os("HOME").as_deref(),
    )
}

pub(crate) fn kubeconfig_file_exists(
    kubeconfig: Option<&std::ffi::OsStr>,
    home: Option<&std::ffi::OsStr>,
) -> bool {
    kubeconfig.filter(|v| !v.is_empty()).map_or_else(
        || home.is_some_and(|h| std::path::Path::new(h).join(".kube/config").is_file()),
        |list| std::env::split_paths(list).any(|p| p.is_file()),
    )
}

/// Query keys whose values `scrub` hides.
const SECRET_KEYS: [&str; 5] = [
    "token=",
    "access_token=",
    "password=",
    "secret=",
    "api_key=",
];

/// Removes credentials from error text: URL user info
/// (`scheme://user:pass@host` to `scheme://***@host`, also with no scheme),
/// secret query values, and bearer tokens. It works on words (split at
/// white space). It can hide more than necessary, never less.
pub(crate) fn scrub(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut after_bearer = false;
    for piece in text.split_inclusive(char::is_whitespace) {
        let word = piece.trim_end_matches(char::is_whitespace);
        let space = &piece[word.len()..];
        let clean = if after_bearer && !word.is_empty() {
            "***".to_owned()
        } else {
            scrub_word(word)
        };
        after_bearer = word.eq_ignore_ascii_case("bearer");
        out.push_str(&clean);
        out.push_str(space);
    }
    out
}

/// Hides URL user info in one word.
fn hide_user_info(word: &str) -> String {
    if let Some(i) = word.find("://") {
        let (head, rest) = word.split_at(i + 3);
        return rest
            .rfind('@')
            .map_or_else(|| word.to_owned(), |at| format!("{head}***{}", &rest[at..]));
    }
    // `user:pass@host` with no scheme. A plain e-mail has no colon.
    match word.rfind('@') {
        Some(at) if word[..at].contains(':') => format!("***{}", &word[at..]),
        _ => word.to_owned(),
    }
}

fn scrub_word(word: &str) -> String {
    let mut w = hide_user_info(word);
    for key in SECRET_KEYS {
        let mut from = 0;
        while let Some(i) = w[from..].find(key) {
            let start = from + i + key.len();
            let end = w[start..]
                .find(['&', '"', '\''])
                .map_or(w.len(), |e| start + e);
            w.replace_range(start..end, "***");
            from = start + 3;
        }
    }
    w
}

/// Text for a `kube` error that is safe to log. An auth error can contain
/// exec plugin output. A proxy error can contain proxy credentials.
pub(crate) fn describe(e: &kube::Error) -> String {
    match e {
        kube::Error::Auth(_) => "authentication failed".to_owned(),
        kube::Error::ProxyProtocolDisabled { .. }
        | kube::Error::ProxyProtocolUnsupported { .. } => {
            "the proxy protocol is not supported (proxy URL hidden)".to_owned()
        }
        other => scrub(&other.to_string()),
    }
}

/// Maps a `kube` error to a [`KubeError`]. `verb` and `resource` name the
/// call for a 403.
pub(crate) fn map_error(e: &kube::Error, verb: &str, resource: &str) -> KubeError {
    match e {
        kube::Error::Api(status) => map_status(status, verb, resource),
        other => KubeError::Api(describe(other)),
    }
}

fn map_status(status: &kube::core::Status, verb: &str, resource: &str) -> KubeError {
    if status.is_conflict() || status.is_already_exists() {
        KubeError::Conflict(scrub(&status.message))
    } else if status.is_not_found() {
        KubeError::NotFound(scrub(&status.message))
    } else if status.is_forbidden() {
        KubeError::Forbidden {
            verb: verb.to_owned(),
            resource: resource.to_owned(),
        }
    } else {
        KubeError::Api(scrub(&status.message))
    }
}

/// Maps a watch error. A 403 on the list or the watch is `Forbidden`.
pub(crate) fn map_watch_error(e: &watcher::Error, resource: &str) -> KubeError {
    match e {
        watcher::Error::InitialListFailed(k) => map_error(k, "list", resource),
        watcher::Error::WatchStartFailed(k) | watcher::Error::WatchFailed(k) => {
            map_error(k, "watch", resource)
        }
        watcher::Error::WatchError(status) => map_status(status, "watch", resource),
        watcher::Error::NoResourceVersion => KubeError::Api(e.to_string()),
    }
}

const LEASES: &str = "leases.coordination.k8s.io";
const CONFIG_MAPS: &str = "configmaps";

/// The spec fields that the elector owns, as a JSON merge patch with a
/// `resourceVersion` check. Other fields and all metadata stay as they are.
pub(crate) fn lease_patch(record: &LeaseRecord) -> serde_json::Value {
    let time = |t: Option<k8s_openapi::jiff::Timestamp>| {
        t.map_or(serde_json::Value::Null, |t| {
            serde_json::to_value(MicroTime(t)).unwrap_or(serde_json::Value::Null)
        })
    };
    serde_json::json!({
        "metadata": { "resourceVersion": record.resource_version },
        "spec": {
            "holderIdentity": record.holder.clone().filter(|h| !h.is_empty()),
            "leaseDurationSeconds": record.lease_duration_secs,
            "acquireTime": time(record.acquire_time),
            "renewTime": time(record.renew_time),
            "leaseTransitions": i32::try_from(record.transitions).unwrap_or(i32::MAX),
        }
    })
}

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
            // PUT with no version creates the Lease (create on update). RBAC
            // can then limit `create` by name. If the Lease exists, the
            // server says 422 "resourceVersion must be specified".
            self.leases(namespace)
                .replace(name, &PostParams::default(), &lease)
                .await
                .map(from_lease)
                .map_err(|e| match &e {
                    kube::Error::Api(s)
                        if s.is_invalid() && s.message.contains("resourceVersion") =>
                    {
                        KubeError::Conflict(format!("lease {name} already exists"))
                    }
                    _ => map_error(&e, "create", LEASES),
                })
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
                .patch(
                    name,
                    &PatchParams::default(),
                    &Patch::Merge(lease_patch(record)),
                )
                .await
                .map(from_lease)
                .map_err(|e| map_error(&e, "patch", LEASES))
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
                    Err(e) => Some(Err(map_watch_error(&e, CONFIG_MAPS))),
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
    fn scrub_hides_user_info() {
        assert_eq!(
            scrub("proxy socks5://user:pa55@proxy:1080/x failed"),
            "proxy socks5://***@proxy:1080/x failed"
        );
        assert_eq!(
            scrub("https://10.0.0.1:443/api"),
            "https://10.0.0.1:443/api"
        );
        assert_eq!(scrub("a://b@c d://e"), "a://***@c d://e");
        assert_eq!(scrub("no url"), "no url");
        assert_eq!(scrub("end://u@h"), "end://***@h");
    }

    #[test]
    fn scrub_hides_more_credential_forms() {
        // Review round 2: forms that passed through before.
        assert_eq!(
            scrub("proxy http://u:pa/ss@proxy:3128 failed"),
            "proxy http://***@proxy:3128 failed"
        );
        assert_eq!(scrub("dial u:pw@proxy:3128"), "dial ***@proxy:3128");
        assert_eq!(
            scrub("GET https://h/api?token=abc123&x=1 failed"),
            "GET https://h/api?token=***&x=1 failed"
        );
        assert_eq!(
            scrub("header Bearer abc.def rejected"),
            "header Bearer *** rejected"
        );
        assert_eq!(scrub("\"https://u:p@h\""), "\"https://***@h\"");
        // Not credentials: stay as they are.
        assert_eq!(scrub("mail me@example.com"), "mail me@example.com");
        assert_eq!(scrub("https://[::1]:6443/x"), "https://[::1]:6443/x");
    }

    #[test]
    fn infer_errors_split_no_cluster_from_bad_config() {
        assert!(matches!(
            classify_infer_error(false, "no config"),
            KubeError::NoCluster(_)
        ));
        let bad = classify_infer_error(true, "bad yaml at https://u:p@h");
        assert!(
            matches!(bad, KubeError::Api(ref m) if m.contains("***@h") && !m.contains("u:p")),
            "{bad}"
        );
    }

    #[test]
    fn kubeconfig_presence_needs_a_file() {
        use std::ffi::OsStr;
        let dir = std::env::temp_dir().join(format!("akp-kc-{}", std::process::id()));
        std::fs::create_dir_all(dir.join(".kube")).unwrap();
        let file = dir.join("kc");
        std::fs::write(&file, "").unwrap();
        let list = std::env::join_paths([dir.join("missing"), file]).unwrap();
        assert!(
            kubeconfig_file_exists(Some(&list), None),
            "any path in the list"
        );
        assert!(!kubeconfig_file_exists(
            Some(OsStr::new("/no/such")),
            Some(dir.as_os_str())
        ));
        assert!(
            !kubeconfig_file_exists(None, Some(dir.as_os_str())),
            "no ~/.kube/config"
        );
        std::fs::write(dir.join(".kube/config"), "").unwrap();
        assert!(kubeconfig_file_exists(None, Some(dir.as_os_str())));
        assert!(
            kubeconfig_file_exists(Some(OsStr::new("")), Some(dir.as_os_str())),
            "empty: home"
        );
        assert!(!kubeconfig_file_exists(None, None));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn mode_follows_kube_order() {
        assert_eq!(mode_for(true), "kubeconfig");
        assert_eq!(mode_for(false), "in_cluster");
    }

    #[test]
    fn patch_has_only_elector_fields_and_the_version() {
        let rec = LeaseRecord {
            holder: None,
            lease_duration_secs: 1,
            acquire_time: Some(Timestamp::UNIX_EPOCH),
            renew_time: None,
            transitions: 2,
            resource_version: Some("9".into()),
        };
        let p = lease_patch(&rec);
        assert_eq!(p["metadata"], serde_json::json!({"resourceVersion": "9"}));
        assert!(
            p["spec"]["holderIdentity"].is_null(),
            "null clears the holder"
        );
        assert!(p["spec"]["renewTime"].is_null());
        assert_eq!(p["spec"]["acquireTime"], "1970-01-01T00:00:00.000000Z");
        assert_eq!(p["spec"]["leaseTransitions"], 2);
        assert_eq!(p["spec"].as_object().unwrap().len(), 5);
    }

    #[test]
    fn watch_errors_map_forbidden() {
        let status = kube::core::Status::failure("denied", "Forbidden").with_code(403);
        let e = watcher::Error::InitialListFailed(kube::Error::Api(status.clone().boxed()));
        assert!(
            matches!(map_watch_error(&e, CONFIG_MAPS), KubeError::Forbidden { ref verb, .. } if verb == "list")
        );
        let e = watcher::Error::WatchError(status.boxed());
        assert!(
            matches!(map_watch_error(&e, CONFIG_MAPS), KubeError::Forbidden { ref verb, .. } if verb == "watch")
        );
        assert!(matches!(
            map_watch_error(&watcher::Error::NoResourceVersion, CONFIG_MAPS),
            KubeError::Api(_)
        ));
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
