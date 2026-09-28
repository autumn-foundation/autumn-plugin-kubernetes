//! Kubernetes plugin for autumn-web.

pub mod api;
pub mod config;
pub mod configmap;
pub mod error;
mod events;
pub mod health;
pub mod leader;
pub mod manifest;
pub mod metrics;
pub mod plugin;
pub mod pod;
pub mod policy;

pub use config::KubernetesConfig;
pub use configmap::ConfigMapStore;
pub use error::KubeError;
pub use leader::{ElectorHandle, LeaderElector, LeaderState, LeaderTask, LeaderTasks, Leadership};
pub use plugin::{KubernetesPlugin, KubernetesRuntime};
pub use pod::PodInfo;
