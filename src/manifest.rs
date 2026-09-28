//! Kubernetes manifests for an Autumn app.
//!
//! [`ManifestSpec::render_yaml`] makes a ServiceAccount, a Role and a
//! RoleBinding (only the verbs the config needs), a Deployment, a Service,
//! and a PodDisruptionBudget (2 or more replicas).
//!
//! The Deployment uses the Autumn probe paths, the Downward API env that
//! [`crate::PodInfo`] reads, a `preStop` sleep, and the Autumn grace formula:
//! `preStop + prestop_grace + shutdown_timeout + buffer`.

use std::collections::BTreeMap;

use autumn_web::ProcessRole;
use autumn_web::config::AutumnConfig;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, DownwardAPIVolumeFile, DownwardAPIVolumeSource, EnvVar,
    EnvVarSource, HTTPGetAction, Lifecycle, LifecycleHandler, ObjectFieldSelector, PodSpec,
    PodTemplateSpec, Probe, SeccompProfile, SecurityContext, Service, ServiceAccount, ServicePort,
    ServiceSpec, SleepAction, Volume, VolumeMount,
};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::api::rbac::v1::{PolicyRule, Role};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::config::KubernetesConfig;
use crate::error::KubeError;

/// Probe paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbePaths {
    /// Liveness path (`health.live_path`).
    pub live: String,
    /// Readiness path (`health.ready_path`).
    pub ready: String,
    /// Startup path (`health.startup_path`).
    pub startup: String,
}

impl Default for ProbePaths {
    fn default() -> Self {
        Self {
            live: "/live".to_owned(),
            ready: "/ready".to_owned(),
            startup: "/startup".to_owned(),
        }
    }
}

/// Input for the manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSpec {
    /// App name. A DNS-1035 label. Names all objects.
    pub name: String,
    /// Namespace.
    pub namespace: String,
    /// Container image.
    pub image: String,
    /// Container port (`server.port`).
    pub port: u16,
    /// Replicas.
    pub replicas: i32,
    /// Process role. `None`: `combined` (no `AUTUMN_ROLE` env).
    pub role: Option<ProcessRole>,
    /// Probe paths.
    pub probes: ProbePaths,
    /// `preStop` sleep in seconds. 0: no hook.
    pub prestop_hook_secs: u64,
    /// `server.prestop_grace_secs`.
    pub prestop_grace_secs: u64,
    /// `server.shutdown_timeout_secs`.
    pub shutdown_timeout_secs: u64,
    /// Extra grace in seconds.
    pub buffer_secs: u64,
    /// Lease name when leader election is on.
    pub lease_name: Option<String>,
    /// ConfigMaps that the app watches.
    pub config_maps: Vec<String>,
    /// The app writes Kubernetes Events.
    pub events: bool,
    /// The plugin talks to the API. Mounts the service account token.
    pub api_access: bool,
    /// Extra env vars.
    pub env: BTreeMap<String, String>,
}

/// Label key for the app name.
pub const NAME_LABEL: &str = "app.kubernetes.io/name";
/// Label key for the tool.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
/// Downward API env vars that the manifest sets. Users cannot override them.
pub const DOWNWARD_ENV: [(&str, &str); 6] = [
    ("POD_NAME", "metadata.name"),
    ("POD_NAMESPACE", "metadata.namespace"),
    ("POD_UID", "metadata.uid"),
    ("NODE_NAME", "spec.nodeName"),
    ("POD_IP", "status.podIP"),
    ("POD_SERVICE_ACCOUNT", "spec.serviceAccountName"),
];

impl ManifestSpec {
    /// Makes a spec with the Autumn defaults: port 3000, 2 replicas, 5 s
    /// `preStop`, 5 s prestop grace, 30 s shutdown, 10 s buffer.
    #[must_use]
    pub fn new(name: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: "default".to_owned(),
            image: image.into(),
            port: 3000,
            replicas: 2,
            role: None,
            probes: ProbePaths::default(),
            prestop_hook_secs: 5,
            prestop_grace_secs: 5,
            shutdown_timeout_secs: 30,
            buffer_secs: 10,
            lease_name: None,
            config_maps: Vec::new(),
            events: true,
            api_access: true,
            env: BTreeMap::new(),
        }
    }

    /// Makes a spec from the Autumn config and the `[kubernetes]` config.
    #[must_use]
    pub fn from_config(
        name: impl Into<String>,
        image: impl Into<String>,
        autumn: &AutumnConfig,
        kube: &KubernetesConfig,
    ) -> Self {
        let mut s = Self::new(name, image);
        s.port = autumn.server.port;
        s.shutdown_timeout_secs = autumn.server.shutdown_timeout_secs;
        s.prestop_grace_secs = autumn.server.prestop_grace_secs;
        s.probes = ProbePaths {
            live: autumn.health.live_path.clone(),
            ready: autumn.health.ready_path.clone(),
            startup: autumn.health.startup_path.clone(),
        };
        s.role = (autumn.role != ProcessRole::Combined).then_some(autumn.role);
        if !kube.namespace.is_empty() {
            s.namespace.clone_from(&kube.namespace);
        }
        s.events = kube.enabled && kube.events;
        s.api_access = kube.enabled;
        if kube.enabled {
            if kube.leader_election.enabled {
                s.lease_name = Some(kube.leader_election.lease_name.clone());
            }
            s.config_maps.clone_from(&kube.config_maps.watch);
        }
        s
    }

    /// `terminationGracePeriodSeconds`.
    #[must_use]
    pub const fn grace_period_secs(&self) -> u64 {
        crate::policy::grace_period_secs(
            self.prestop_hook_secs,
            self.prestop_grace_secs,
            self.shutdown_timeout_secs,
            self.buffer_secs,
        )
    }

    /// Checks the spec.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] with the first problem.
    pub fn validate(&self) -> Result<(), KubeError> {
        let bad = |m: String| Err(KubeError::Config(format!("manifest: {m}")));
        let dns1035 = self
            .name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase)
            && crate::config::is_dns_label(&self.name);
        if !dns1035 {
            return bad(format!(
                "name {:?} is not a DNS-1035 label (a-z, 0-9, '-', starts with a letter)",
                self.name
            ));
        }
        if !crate::config::is_dns_label(&self.namespace) {
            return bad(format!(
                "namespace {:?} is not a DNS-1123 label",
                self.namespace
            ));
        }
        if self.image.trim().is_empty() {
            return bad("image is empty".to_owned());
        }
        if self.port == 0 {
            return bad("port is 0".to_owned());
        }
        if self.replicas < 0 {
            return bad(format!("replicas {} is below 0", self.replicas));
        }
        for (what, path) in [
            ("live", &self.probes.live),
            ("ready", &self.probes.ready),
            ("startup", &self.probes.startup),
        ] {
            if !path.starts_with('/') {
                return bad(format!(
                    "{what} probe path {path:?} does not start with '/'"
                ));
            }
        }
        if let Some(lease) = &self.lease_name
            && !crate::config::is_dns_subdomain(lease)
        {
            return bad(format!("lease name {lease:?} is not a DNS-1123 subdomain"));
        }
        if let Some(cm) = self
            .config_maps
            .iter()
            .find(|n| !crate::config::is_dns_subdomain(n))
        {
            return bad(format!(
                "config map name {cm:?} is not a DNS-1123 subdomain"
            ));
        }
        for key in self.env.keys() {
            let valid = key.as_bytes().first().is_some_and(|b| !b.is_ascii_digit())
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.');
            if !valid {
                return bad(format!("env name {key:?} is not valid"));
            }
            if DOWNWARD_ENV.iter().any(|(n, _)| n == key) {
                return bad(format!("env {key} is set by the manifest"));
            }
        }
        if i64::try_from(self.grace_period_secs()).is_err()
            || i64::try_from(self.prestop_hook_secs).is_err()
        {
            return bad("grace period is too long".to_owned());
        }
        Ok(())
    }

    /// The objects, as JSON values, in apply order.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] when the spec is not valid.
    pub fn objects(&self) -> Result<Vec<serde_json::Value>, KubeError> {
        self.validate()?;
        let to_value = |v: serde_json::Result<serde_json::Value>| {
            v.map_err(|e| KubeError::Config(format!("manifest: {e}")))
        };
        let mut out = vec![to_value(serde_json::to_value(self.service_account()))?];
        if let Some(role) = self.role_object() {
            out.push(to_value(serde_json::to_value(role))?);
            out.push(self.role_binding());
        }
        out.push(to_value(serde_json::to_value(self.deployment()))?);
        out.push(to_value(serde_json::to_value(self.service()))?);
        if self.replicas >= 2 {
            out.push(to_value(serde_json::to_value(self.pdb()))?);
        }
        Ok(out)
    }

    /// The objects as one multi-document YAML text.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] when the spec is not valid.
    pub fn render_yaml(&self) -> Result<String, KubeError> {
        let mut out = String::new();
        for obj in self.objects()? {
            let doc = serde_saphyr::to_string(&obj)
                .map_err(|e| KubeError::Config(format!("manifest: {e}")))?;
            out.push_str("---\n");
            out.push_str(&doc);
            if !doc.ends_with('\n') {
                out.push('\n');
            }
        }
        Ok(out)
    }

    fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (NAME_LABEL.to_owned(), self.name.clone()),
            (MANAGED_BY_LABEL.to_owned(), crate::api::REPORTER.to_owned()),
        ])
    }

    fn selector(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(NAME_LABEL.to_owned(), self.name.clone())])
    }

    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            name: Some(self.name.clone()),
            namespace: Some(self.namespace.clone()),
            labels: Some(self.labels()),
            ..ObjectMeta::default()
        }
    }

    fn rules(&self) -> Vec<PolicyRule> {
        let rule =
            |group: &str, resource: &str, verbs: &[&str], names: Option<Vec<String>>| PolicyRule {
                api_groups: Some(vec![group.to_owned()]),
                resources: Some(vec![resource.to_owned()]),
                verbs: verbs.iter().map(|v| (*v).to_owned()).collect(),
                resource_names: names,
                ..PolicyRule::default()
            };
        let mut rules = Vec::new();
        if let Some(lease) = &self.lease_name {
            // The elector creates with PUT (create on update), so `create`
            // is limited by name too. Renew and release use a merge patch.
            rules.push(rule(
                "coordination.k8s.io",
                "leases",
                &["create", "get", "patch", "update"],
                Some(vec![lease.clone()]),
            ));
        }
        if !self.config_maps.is_empty() {
            // The watch uses a `metadata.name` field selector, so names apply
            // to list and watch. It makes no get call.
            rules.push(rule(
                "",
                "configmaps",
                &["list", "watch"],
                Some(self.config_maps.clone()),
            ));
        }
        if self.events {
            rules.push(rule("events.k8s.io", "events", &["create", "patch"], None));
        }
        rules
    }

    fn service_account(&self) -> ServiceAccount {
        ServiceAccount {
            metadata: self.meta(),
            ..ServiceAccount::default()
        }
    }

    fn role_object(&self) -> Option<Role> {
        let rules = self.rules();
        (!rules.is_empty()).then(|| Role {
            metadata: self.meta(),
            rules: Some(rules),
        })
    }

    /// JSON, not `RoleBinding`: `roleRef.apiGroup` changes type across
    /// k8s-openapi versions.
    fn role_binding(&self) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": self.meta(),
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": self.name,
            },
            "subjects": [{
                "kind": "ServiceAccount",
                "name": self.name,
                "namespace": self.namespace,
            }],
        })
    }

    fn env_vars(&self) -> Vec<EnvVar> {
        let mut env: Vec<EnvVar> = DOWNWARD_ENV
            .iter()
            .map(|(name, field)| EnvVar {
                name: (*name).to_owned(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: (*field).to_owned(),
                        ..ObjectFieldSelector::default()
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            })
            .collect();
        let mut plain = BTreeMap::from([
            // Autumn binds 127.0.0.1 by default. Probes need all interfaces.
            ("AUTUMN_SERVER__HOST".to_owned(), "0.0.0.0".to_owned()),
            ("AUTUMN_SERVER__PORT".to_owned(), self.port.to_string()),
        ]);
        if let Some(role) = self.role {
            plain.insert("AUTUMN_ROLE".to_owned(), role.as_str().to_owned());
        }
        plain.extend(self.env.clone());
        env.extend(plain.into_iter().map(|(name, value)| EnvVar {
            name,
            value: Some(value),
            ..EnvVar::default()
        }));
        env
    }

    fn probe(path: &str, period: i32, failures: i32) -> Probe {
        Probe {
            http_get: Some(HTTPGetAction {
                path: Some(path.to_owned()),
                port: IntOrString::String("http".to_owned()),
                ..HTTPGetAction::default()
            }),
            period_seconds: Some(period),
            failure_threshold: Some(failures),
            ..Probe::default()
        }
    }

    fn deployment(&self) -> Deployment {
        // The plugin needs the token to find the cluster, also with no rules.
        let needs_token = self.api_access || !self.rules().is_empty();
        let lifecycle = (self.prestop_hook_secs > 0).then(|| Lifecycle {
            pre_stop: Some(LifecycleHandler {
                // `sleep` needs no shell in the image.
                sleep: Some(SleepAction {
                    seconds: i64::try_from(self.prestop_hook_secs).unwrap_or(i64::MAX),
                }),
                ..LifecycleHandler::default()
            }),
            ..Lifecycle::default()
        });
        let container = Container {
            name: "app".to_owned(),
            image: Some(self.image.clone()),
            ports: Some(vec![ContainerPort {
                name: Some("http".to_owned()),
                container_port: i32::from(self.port),
                ..ContainerPort::default()
            }]),
            env: Some(self.env_vars()),
            liveness_probe: Some(Self::probe(&self.probes.live, 10, 3)),
            // Autumn guide: leave the Service on the first 503.
            readiness_probe: Some(Self::probe(&self.probes.ready, 5, 1)),
            startup_probe: Some(Self::probe(&self.probes.startup, 2, 30)),
            lifecycle,
            volume_mounts: Some(vec![VolumeMount {
                name: "podinfo".to_owned(),
                mount_path: crate::pod::PODINFO_DIR.to_owned(),
                read_only: Some(true),
                ..VolumeMount::default()
            }]),
            security_context: Some(SecurityContext {
                allow_privilege_escalation: Some(false),
                run_as_non_root: Some(true),
                capabilities: Some(Capabilities {
                    drop: Some(vec!["ALL".to_owned()]),
                    ..Capabilities::default()
                }),
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".to_owned(),
                    ..SeccompProfile::default()
                }),
                ..SecurityContext::default()
            }),
            ..Container::default()
        };
        Deployment {
            metadata: self.meta(),
            spec: Some(DeploymentSpec {
                replicas: Some(self.replicas),
                selector: LabelSelector {
                    match_labels: Some(self.selector()),
                    ..LabelSelector::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(self.labels()),
                        ..ObjectMeta::default()
                    }),
                    spec: Some(PodSpec {
                        service_account_name: Some(self.name.clone()),
                        automount_service_account_token: Some(needs_token),
                        termination_grace_period_seconds: Some(
                            i64::try_from(self.grace_period_secs()).unwrap_or(i64::MAX),
                        ),
                        containers: vec![container],
                        volumes: Some(vec![Volume {
                            name: "podinfo".to_owned(),
                            downward_api: Some(DownwardAPIVolumeSource {
                                items: Some(vec![DownwardAPIVolumeFile {
                                    path: "labels".to_owned(),
                                    field_ref: Some(ObjectFieldSelector {
                                        field_path: "metadata.labels".to_owned(),
                                        ..ObjectFieldSelector::default()
                                    }),
                                    ..DownwardAPIVolumeFile::default()
                                }]),
                                ..DownwardAPIVolumeSource::default()
                            }),
                            ..Volume::default()
                        }]),
                        ..PodSpec::default()
                    }),
                },
                ..DeploymentSpec::default()
            }),
            ..Deployment::default()
        }
    }

    fn service(&self) -> Service {
        Service {
            metadata: self.meta(),
            spec: Some(ServiceSpec {
                selector: Some(self.selector()),
                ports: Some(vec![ServicePort {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: Some(IntOrString::String("http".to_owned())),
                    ..ServicePort::default()
                }]),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        }
    }

    fn pdb(&self) -> PodDisruptionBudget {
        PodDisruptionBudget {
            metadata: self.meta(),
            spec: Some(PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(1)),
                selector: Some(LabelSelector {
                    match_labels: Some(self.selector()),
                    ..LabelSelector::default()
                }),
                ..PodDisruptionBudgetSpec::default()
            }),
            ..PodDisruptionBudget::default()
        }
    }
}
