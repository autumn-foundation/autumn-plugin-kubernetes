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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_are_short_and_have_no_detail() {
        let cases = [
            (KubeError::Config("secret-ish".into()), "config"),
            (KubeError::NoCluster("x".into()), "no_cluster"),
            (KubeError::NotFound("x".into()), "not_found"),
            (KubeError::Conflict("x".into()), "conflict"),
            (
                KubeError::Forbidden {
                    verb: "get".into(),
                    resource: "leases".into(),
                },
                "forbidden",
            ),
            (KubeError::Api("https://10.0.0.1".into()), "api"),
            (
                KubeError::Decode {
                    name: "n".into(),
                    key: "k".into(),
                    message: "m".into(),
                },
                "decode",
            ),
        ];
        for (err, class) in cases {
            assert_eq!(err.class(), class);
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn forbidden_names_the_fix() {
        let e = KubeError::Forbidden {
            verb: "update".into(),
            resource: "leases.coordination.k8s.io".into(),
        };
        assert_eq!(
            e.to_string(),
            "kubernetes denied update on leases.coordination.k8s.io; add it to the Role"
        );
    }
}
