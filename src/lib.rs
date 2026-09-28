//! Kubernetes plugin for autumn-web.

pub mod api;
pub mod config;
pub mod error;
mod events;
pub mod leader;
pub mod metrics;
pub mod pod;
pub mod policy;

pub use config::KubernetesConfig;
pub use error::KubeError;
pub use leader::{ElectorHandle, LeaderElector, LeaderState, LeaderTask, LeaderTasks, Leadership};
pub use pod::PodInfo;
