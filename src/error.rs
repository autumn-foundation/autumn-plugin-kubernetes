//! Errors.

/// A plugin error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum KubeError {
    /// The config is not valid.
    #[error("kubernetes config: {0}")]
    Config(String),
    /// No cluster config was found (not in a pod, no kubeconfig).
    #[error("no kubernetes cluster found: {0}")]
    NoCluster(String),
    /// The object does not exist.
    #[error("kubernetes object not found: {0}")]
    NotFound(String),
    /// Another writer changed the object first (HTTP 409).
    #[error("kubernetes write conflict: {0}")]
    Conflict(String),
    /// RBAC denied the call (HTTP 403).
    #[error("kubernetes denied {verb} on {resource}; add it to the Role")]
    Forbidden {
        /// The verb, for example `update`.
        verb: String,
        /// The resource, for example `leases.coordination.k8s.io`.
        resource: String,
    },
    /// Another API or transport error.
    #[error("kubernetes API error: {0}")]
    Api(String),
    /// A ConfigMap value does not decode.
    #[error("ConfigMap {name} key {key} does not decode: {message}")]
    Decode {
        /// ConfigMap name.
        name: String,
        /// Data key.
        key: String,
        /// Parser message.
        message: String,
    },
}

impl KubeError {
    /// A short class for health output and metrics. It has no URL or name.
    #[must_use]
    pub const fn class(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::NoCluster(_) => "no_cluster",
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Forbidden { .. } => "forbidden",
            Self::Api(_) => "api",
            Self::Decode { .. } => "decode",
        }
    }
}
