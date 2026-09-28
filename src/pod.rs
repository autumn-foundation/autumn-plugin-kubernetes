//! Pod identity from the Downward API.
//!
//! Env vars: `POD_NAME`, `POD_NAMESPACE`, `POD_UID`, `NODE_NAME`, `POD_IP`,
//! `POD_SERVICE_ACCOUNT`. Files: the service account namespace file and the
//! Downward API `labels` file. The manifest generator sets all of these.

use std::collections::BTreeMap;
use std::path::Path;

/// Service account directory in a pod.
pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
/// Downward API volume directory that the manifest generator mounts.
pub const PODINFO_DIR: &str = "/etc/podinfo";

/// Facts about the pod that runs this process. All fields are optional.
/// Outside a cluster most are `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodInfo {
    /// Pod name. Falls back to `HOSTNAME` (the pod name in a pod).
    pub name: Option<String>,
    /// Pod namespace. Falls back to the service account file.
    pub namespace: Option<String>,
    /// Pod UID.
    pub uid: Option<String>,
    /// Node name.
    pub node_name: Option<String>,
    /// Pod IP.
    pub pod_ip: Option<String>,
    /// Service account name.
    pub service_account: Option<String>,
    /// Pod labels from the Downward API volume.
    pub labels: BTreeMap<String, String>,
    /// `true` when `KUBERNETES_SERVICE_HOST` is set.
    pub in_cluster: bool,
}

impl PodInfo {
    /// Reads the process env and the default file paths.
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_sources(
            |k| std::env::var(k).ok(),
            Path::new(SERVICE_ACCOUNT_DIR),
            Path::new(PODINFO_DIR),
        )
    }

    /// Reads from `env` and the two directories. Use it in tests.
    #[must_use]
    pub fn from_sources(
        env: impl Fn(&str) -> Option<String>,
        service_account_dir: &Path,
        podinfo_dir: &Path,
    ) -> Self {
        let get = |k: &str| {
            env(k)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        let file = |path: &Path| {
            std::fs::read_to_string(path)
                .ok()
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        Self {
            name: get("POD_NAME").or_else(|| get("HOSTNAME")),
            namespace: get("POD_NAMESPACE")
                .or_else(|| file(&service_account_dir.join("namespace"))),
            uid: get("POD_UID"),
            node_name: get("NODE_NAME"),
            pod_ip: get("POD_IP"),
            service_account: get("POD_SERVICE_ACCOUNT"),
            labels: std::fs::read_to_string(podinfo_dir.join("labels"))
                .map(|t| parse_downward_file(&t))
                .unwrap_or_default(),
            in_cluster: get("KUBERNETES_SERVICE_HOST").is_some(),
        }
    }

    /// Reads the pod info from the app state.
    #[must_use]
    pub fn from_state(state: &autumn_web::AppState) -> Option<std::sync::Arc<Self>> {
        state.extension::<Self>()
    }
}

/// Parses a Downward API `labels` or `annotations` file: `key="value"` lines.
#[must_use]
pub fn parse_downward_file(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| {
            let (key, raw) = line.split_once('=')?;
            let key = key.trim();
            if key.is_empty() {
                return None;
            }
            // Values are Go-quoted strings. JSON reads the common cases.
            let value = serde_json::from_str::<String>(raw.trim())
                .unwrap_or_else(|_| raw.trim().to_owned());
            Some((key.to_owned(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("akp-pod-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn reads_downward_env() {
        let env = |k: &str| {
            Some(
                match k {
                    "POD_NAME" => "web-7f9c-abcde",
                    "POD_NAMESPACE" => "shop",
                    "POD_UID" => "uid-1",
                    "NODE_NAME" => "node-a",
                    "POD_IP" => "10.1.2.3",
                    "POD_SERVICE_ACCOUNT" => "web",
                    "KUBERNETES_SERVICE_HOST" => "10.0.0.1",
                    _ => return None,
                }
                .to_owned(),
            )
        };
        let p = PodInfo::from_sources(env, Path::new("/no/sa"), Path::new("/no/podinfo"));
        assert_eq!(p.name.as_deref(), Some("web-7f9c-abcde"));
        assert_eq!(p.namespace.as_deref(), Some("shop"));
        assert_eq!(p.uid.as_deref(), Some("uid-1"));
        assert_eq!(p.node_name.as_deref(), Some("node-a"));
        assert_eq!(p.pod_ip.as_deref(), Some("10.1.2.3"));
        assert_eq!(p.service_account.as_deref(), Some("web"));
        assert!(p.in_cluster);
        assert!(p.labels.is_empty());
    }

    #[test]
    fn falls_back_to_hostname_and_service_account_file() {
        let sa = tmp("sa");
        std::fs::write(sa.join("namespace"), "billing\n").unwrap();
        let env = |k: &str| (k == "HOSTNAME").then(|| "pod-x".to_owned());
        let p = PodInfo::from_sources(env, &sa, Path::new("/no/podinfo"));
        assert_eq!(p.name.as_deref(), Some("pod-x"));
        assert_eq!(p.namespace.as_deref(), Some("billing"));
        assert!(!p.in_cluster);
        std::fs::remove_dir_all(sa).unwrap();
    }

    #[test]
    fn empty_values_are_none() {
        let env = |k: &str| (k == "POD_NAME" || k == "HOSTNAME").then(|| "  ".to_owned());
        let p = PodInfo::from_sources(env, Path::new("/no"), Path::new("/no"));
        assert_eq!(p, PodInfo::default());
    }

    #[test]
    fn reads_labels_file() {
        let d = tmp("labels");
        std::fs::write(
            d.join("labels"),
            "app=\"web\"\npod-template-hash=\"7f9c\"\nnote=\"a \\\"quoted\\\" = sign\"\n\nbad-line\n",
        )
        .unwrap();
        let p = PodInfo::from_sources(|_| None, Path::new("/no"), &d);
        assert_eq!(p.labels["app"], "web");
        assert_eq!(p.labels["pod-template-hash"], "7f9c");
        assert_eq!(p.labels["note"], "a \"quoted\" = sign");
        assert_eq!(p.labels.len(), 3);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn unquoted_value_is_kept() {
        let m = parse_downward_file("k=plain\n");
        assert_eq!(m["k"], "plain");
    }

    #[test]
    fn from_env_does_not_panic() {
        let _ = PodInfo::from_env();
    }
}
