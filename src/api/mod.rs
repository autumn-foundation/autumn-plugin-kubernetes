//! The Kubernetes API calls that the plugin makes.
//!
//! Only this module calls Kubernetes. Other code uses [`KubeApi`].
//! [`KubeClientApi`] is the real client. [`MemoryKubeApi`] is a fake for
//! tests. It follows the same rules as the API server.

mod client;
pub use client::REPORTER;
mod memory;

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use k8s_openapi::jiff::Timestamp;

pub use client::KubeClientApi;
pub use memory::MemoryKubeApi;

use crate::error::KubeError;

/// A future from a [`KubeApi`] call.
pub type ApiFuture<'a, T> = BoxFuture<'a, Result<T, KubeError>>;

/// The Lease fields that leader election uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseRecord {
    /// Holder identity. `None` or empty: nobody holds the lease.
    pub holder: Option<String>,
    /// `leaseDurationSeconds`.
    pub lease_duration_secs: i32,
    /// `acquireTime`.
    pub acquire_time: Option<Timestamp>,
    /// `renewTime`.
    pub renew_time: Option<Timestamp>,
    /// `leaseTransitions`.
    pub transitions: u32,
    /// Set by the API server. A replace needs the value from the last read.
    pub resource_version: Option<String>,
}

impl LeaseRecord {
    /// Returns `true` when `identity` holds the lease.
    #[must_use]
    pub fn held_by(&self, identity: &str) -> bool {
        self.holder.as_deref() == Some(identity)
    }

    /// Returns `true` when nobody holds the lease.
    #[must_use]
    pub fn is_free(&self) -> bool {
        self.holder.as_deref().is_none_or(str::is_empty)
    }
}

/// A change to a watched ConfigMap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigMapEvent {
    /// The ConfigMap exists with this `data`.
    Applied(BTreeMap<String, String>),
    /// The ConfigMap does not exist.
    Deleted,
}

/// Event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// Normal.
    Normal,
    /// Warning.
    Warning,
}

/// A Kubernetes Event about the pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodEvent {
    /// Normal or warning.
    pub kind: EventKind,
    /// Short `UpperCamelCase` reason, for example `LeaderElected`.
    pub reason: String,
    /// Short `UpperCamelCase` action, for example `Elect`.
    pub action: String,
    /// Text for people. It has no secrets.
    pub note: Option<String>,
}

/// The pod that an event is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRef {
    /// Namespace.
    pub namespace: String,
    /// Pod name.
    pub name: String,
    /// Pod UID, if known.
    pub uid: Option<String>,
}

/// The Kubernetes API calls that the plugin makes.
pub trait KubeApi: Send + Sync + 'static {
    /// Returns the API server version (`GET /version`).
    fn server_version(&self) -> ApiFuture<'_, String>;

    /// Reads a Lease. `Ok(None)` when it does not exist.
    fn get_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> ApiFuture<'a, Option<LeaseRecord>>;

    /// Creates a Lease. [`KubeError::Conflict`] when it exists.
    fn create_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        record: &'a LeaseRecord,
    ) -> ApiFuture<'a, LeaseRecord>;

    /// Replaces a Lease. [`KubeError::Conflict`] when
    /// `record.resource_version` is old or missing. Like the API server, a
    /// replace of a Lease that does not exist creates it.
    fn replace_lease<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        record: &'a LeaseRecord,
    ) -> ApiFuture<'a, LeaseRecord>;

    /// Watches one ConfigMap. The first item is the current state. The stream
    /// retries on errors and yields them.
    fn watch_config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> BoxStream<'static, Result<ConfigMapEvent, KubeError>>;

    /// Writes an Event about the pod.
    fn publish_event<'a>(&'a self, pod: &'a PodRef, event: &'a PodEvent) -> ApiFuture<'a, ()>;

    /// The default namespace of the client.
    fn default_namespace(&self) -> String;

    /// The `kube::Client`, when this is a real API.
    fn client(&self) -> Option<kube::Client> {
        None
    }
}
