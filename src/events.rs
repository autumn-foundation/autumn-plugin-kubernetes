//! Kubernetes Events about the pod.
//!
//! A failed write is counted and logged at debug level. It never stops the
//! app.

use std::sync::Arc;

use crate::api::{EventKind, KubeApi, PodEvent, PodRef};
use crate::metrics::KubernetesMetrics;

/// Writes events about this pod. It does nothing with no pod reference.
#[derive(Clone)]
pub(crate) struct EventSink {
    api: Arc<dyn KubeApi>,
    pod: Option<PodRef>,
    metrics: Arc<KubernetesMetrics>,
}

impl EventSink {
    pub(crate) fn new(
        api: Arc<dyn KubeApi>,
        pod: Option<PodRef>,
        metrics: Arc<KubernetesMetrics>,
    ) -> Self {
        Self { api, pod, metrics }
    }

    /// Writes the event now.
    pub(crate) async fn emit(&self, event: PodEvent) {
        let Some(pod) = &self.pod else {
            return;
        };
        match self.api.publish_event(pod, &event).await {
            Ok(()) => self.metrics.event(true),
            Err(e) => {
                self.metrics.event(false);
                tracing::debug!(reason = %event.reason, error = %e, "kubernetes event not written");
            }
        }
    }

    /// Writes the event in the background.
    pub(crate) fn spawn(&self, event: PodEvent) {
        let sink = self.clone();
        tokio::spawn(async move { sink.emit(event).await });
    }
}

fn event(kind: EventKind, reason: &str, action: &str, note: String) -> PodEvent {
    PodEvent {
        kind,
        reason: reason.to_owned(),
        action: action.to_owned(),
        note: Some(note),
    }
}

/// The app started.
pub(crate) fn started(mode: &str) -> PodEvent {
    event(
        EventKind::Normal,
        "Started",
        "Start",
        format!("autumn app started ({mode})"),
    )
}

/// This replica became leader.
pub(crate) fn leader_elected(lease: &str, identity: &str) -> PodEvent {
    event(
        EventKind::Normal,
        "LeaderElected",
        "Elect",
        format!("{identity} leads lease {lease}"),
    )
}

/// This replica stopped leading.
pub(crate) fn leader_lost(lease: &str, identity: &str) -> PodEvent {
    event(
        EventKind::Warning,
        "LeaderLost",
        "Elect",
        format!("{identity} stopped leading lease {lease}"),
    )
}

/// The app is stopping.
pub(crate) fn stopping() -> PodEvent {
    event(
        EventKind::Normal,
        "Stopping",
        "Stop",
        "autumn app is stopping".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::MemoryKubeApi;

    fn pod() -> PodRef {
        PodRef {
            namespace: "shop".into(),
            name: "web-1".into(),
            uid: Some("u".into()),
        }
    }

    #[tokio::test]
    async fn emits_and_counts() {
        let api = MemoryKubeApi::new();
        let m = Arc::new(KubernetesMetrics::new());
        let sink = EventSink::new(Arc::new(api.clone()), Some(pod()), Arc::clone(&m));
        sink.emit(started("in_cluster")).await;
        let events = api.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, pod());
        assert_eq!(events[0].1.reason, "Started");
        assert_eq!(m.snapshot().events_published, 1);
    }

    #[tokio::test]
    async fn failure_is_counted_not_raised() {
        let api = MemoryKubeApi::new();
        api.set_events_failing(true);
        let m = Arc::new(KubernetesMetrics::new());
        let sink = EventSink::new(Arc::new(api.clone()), Some(pod()), Arc::clone(&m));
        sink.emit(stopping()).await;
        assert_eq!(m.snapshot().events_failed, 1);
    }

    #[tokio::test]
    async fn no_pod_does_nothing() {
        let api = MemoryKubeApi::new();
        let m = Arc::new(KubernetesMetrics::new());
        let sink = EventSink::new(Arc::new(api.clone()), None, Arc::clone(&m));
        sink.emit(stopping()).await;
        assert!(api.events().is_empty());
        assert_eq!(
            m.snapshot().events_published + m.snapshot().events_failed,
            0
        );
    }

    #[tokio::test]
    async fn spawn_writes_in_background() {
        let api = MemoryKubeApi::new();
        let sink = EventSink::new(
            Arc::new(api.clone()),
            Some(pod()),
            Arc::new(KubernetesMetrics::new()),
        );
        sink.spawn(stopping());
        for _ in 0..100 {
            if !api.events().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(api.events().len(), 1);
    }

    #[test]
    fn event_shapes() {
        let e = leader_elected("l", "me");
        assert_eq!(
            (e.kind, e.reason.as_str(), e.action.as_str()),
            (EventKind::Normal, "LeaderElected", "Elect")
        );
        assert!(e.note.unwrap().contains("me"));
        let e = leader_lost("l", "me");
        assert_eq!(
            (e.kind, e.reason.as_str()),
            (EventKind::Warning, "LeaderLost")
        );
        assert_eq!(stopping().reason, "Stopping");
        assert!(started("detached").note.unwrap().contains("detached"));
    }
}
