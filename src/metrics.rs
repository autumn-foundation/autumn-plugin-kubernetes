//! Metrics for `/actuator/prometheus`.
//!
//! Labels are the lease name and ConfigMap names from the config. Nothing
//! else, so the label set stays small.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample, MetricsSource};

/// Counters for one watched ConfigMap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConfigMapCounts {
    /// Applied or deleted events.
    pub updates: u64,
    /// Watch errors.
    pub errors: u64,
}

/// A copy of the counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Lease name, when leader election runs.
    pub lease: Option<String>,
    /// `1` while this replica leads.
    pub leading: u64,
    /// Times this replica became leader.
    pub leader_acquired: u64,
    /// Times this replica stopped leading.
    pub leader_lost: u64,
    /// Failed Lease calls.
    pub lease_errors: u64,
    /// Published events.
    pub events_published: u64,
    /// Failed event writes.
    pub events_failed: u64,
    /// `1` up, `0` down, `-1` not checked yet.
    pub api_up: i64,
    /// Counters per ConfigMap.
    pub config_maps: BTreeMap<String, ConfigMapCounts>,
}

/// Plugin metrics. A [`MetricsSource`].
#[derive(Debug)]
pub struct KubernetesMetrics {
    lease: OnceLock<String>,
    leading: AtomicU64,
    leader_acquired: AtomicU64,
    leader_lost: AtomicU64,
    lease_errors: AtomicU64,
    events_published: AtomicU64,
    events_failed: AtomicU64,
    api_up: AtomicI64,
    config_maps: Mutex<BTreeMap<String, ConfigMapCounts>>,
}

impl Default for KubernetesMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl KubernetesMetrics {
    /// Makes zeroed metrics.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            lease: OnceLock::new(),
            leading: AtomicU64::new(0),
            leader_acquired: AtomicU64::new(0),
            leader_lost: AtomicU64::new(0),
            lease_errors: AtomicU64::new(0),
            events_published: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            api_up: AtomicI64::new(-1),
            config_maps: Mutex::new(BTreeMap::new()),
        }
    }

    /// Sets the lease label once.
    pub fn set_lease(&self, name: &str) {
        let _ = self.lease.set(name.to_owned());
    }

    /// Records a change of leadership. Only edges count.
    pub fn set_leading(&self, leading: bool) {
        let prev = self.leading.swap(u64::from(leading), Ordering::SeqCst);
        match (prev == 1, leading) {
            (false, true) => {
                self.leader_acquired.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.leader_lost.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Counts a failed Lease call.
    pub fn lease_error(&self) {
        self.lease_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an event write.
    pub fn event(&self, ok: bool) {
        let counter = if ok {
            &self.events_published
        } else {
            &self.events_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the last API check.
    pub fn set_api_up(&self, up: bool) {
        self.api_up.store(i64::from(up), Ordering::Relaxed);
    }

    fn maps(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, ConfigMapCounts>> {
        self.config_maps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a ConfigMap label with zero counts.
    pub fn watch_config_map(&self, name: &str) {
        self.maps().entry(name.to_owned()).or_default();
    }

    /// Counts a ConfigMap update or watch error.
    pub fn config_map(&self, name: &str, ok: bool) {
        let mut maps = self.maps();
        let counts = maps.entry(name.to_owned()).or_default();
        if ok {
            counts.updates += 1;
        } else {
            counts.errors += 1;
        }
        drop(maps);
    }

    /// Returns a copy of the counters.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            lease: self.lease.get().cloned(),
            leading: self.leading.load(Ordering::SeqCst),
            leader_acquired: self.leader_acquired.load(Ordering::Relaxed),
            leader_lost: self.leader_lost.load(Ordering::Relaxed),
            lease_errors: self.lease_errors.load(Ordering::Relaxed),
            events_published: self.events_published.load(Ordering::Relaxed),
            events_failed: self.events_failed.load(Ordering::Relaxed),
            api_up: self.api_up.load(Ordering::Relaxed),
            config_maps: self.maps().clone(),
        }
    }
}

#[allow(clippy::cast_precision_loss)] // Counters stay far below 2^53.
fn sample(labels: &[(&str, &str)], value: u64) -> MetricSample {
    MetricSample {
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        value: value as f64,
    }
}

fn family(name: &str, help: &str, kind: MetricKind, samples: Vec<MetricSample>) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        kind,
        samples,
    }
}

impl MetricsSource for KubernetesMetrics {
    fn collect(&self) -> Vec<MetricFamily> {
        let s = self.snapshot();
        let mut out = Vec::new();
        if let Some(lease) = s.lease.as_deref() {
            let l = [("lease", lease)];
            out.push(family(
                "kubernetes_leader",
                "1 while this replica holds the lease",
                MetricKind::Gauge,
                vec![sample(&l, s.leading)],
            ));
            out.push(family(
                "kubernetes_leader_acquired_total",
                "Times this replica became leader",
                MetricKind::Counter,
                vec![sample(&l, s.leader_acquired)],
            ));
            out.push(family(
                "kubernetes_leader_lost_total",
                "Times this replica stopped leading",
                MetricKind::Counter,
                vec![sample(&l, s.leader_lost)],
            ));
            out.push(family(
                "kubernetes_lease_errors_total",
                "Failed Lease API calls",
                MetricKind::Counter,
                vec![sample(&l, s.lease_errors)],
            ));
        }
        out.push(family(
            "kubernetes_events_published_total",
            "Kubernetes Events written",
            MetricKind::Counter,
            vec![sample(&[], s.events_published)],
        ));
        out.push(family(
            "kubernetes_events_failed_total",
            "Kubernetes Event writes that failed",
            MetricKind::Counter,
            vec![sample(&[], s.events_failed)],
        ));
        out.push(family(
            "kubernetes_api_up",
            "1 when the last API server check passed, 0 when it failed",
            MetricKind::Gauge,
            u64::try_from(s.api_up)
                .ok()
                .map(|v| sample(&[], v))
                .into_iter()
                .collect(),
        ));
        let per_map = |pick: fn(&ConfigMapCounts) -> u64| {
            s.config_maps
                .iter()
                .map(|(name, c)| sample(&[("config_map", name)], pick(c)))
                .collect::<Vec<_>>()
        };
        out.push(family(
            "kubernetes_config_map_updates_total",
            "ConfigMap changes seen by the watch",
            MetricKind::Counter,
            per_map(|c| c.updates),
        ));
        out.push(family(
            "kubernetes_config_map_errors_total",
            "ConfigMap watch errors",
            MetricKind::Counter,
            per_map(|c| c.errors),
        ));
        out
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    fn family<'a>(fams: &'a [MetricFamily], name: &str) -> &'a MetricFamily {
        fams.iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("no family {name}"))
    }

    #[test]
    fn starts_at_zero() {
        let m = KubernetesMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.leading, 0);
        assert_eq!(s.api_up, -1);
        assert_eq!(s.lease, None);
    }

    #[test]
    fn leadership_counts_edges_only() {
        let m = KubernetesMetrics::new();
        m.set_leading(true);
        m.set_leading(true);
        m.set_leading(false);
        m.set_leading(false);
        m.set_leading(true);
        let s = m.snapshot();
        assert_eq!(s.leading, 1);
        assert_eq!(s.leader_acquired, 2);
        assert_eq!(s.leader_lost, 1);
    }

    #[test]
    fn counters_and_labels() {
        let m = KubernetesMetrics::new();
        m.set_lease("app-leader");
        m.set_lease("ignored");
        m.lease_error();
        m.event(true);
        m.event(false);
        m.set_api_up(true);
        m.watch_config_map("flags");
        m.config_map("flags", true);
        m.config_map("flags", false);
        let s = m.snapshot();
        assert_eq!(s.lease.as_deref(), Some("app-leader"));
        assert_eq!(s.lease_errors, 1);
        assert_eq!((s.events_published, s.events_failed), (1, 1));
        assert_eq!(s.api_up, 1);
        assert_eq!(
            s.config_maps["flags"],
            ConfigMapCounts {
                updates: 1,
                errors: 1
            }
        );
    }

    #[test]
    fn exports_families_with_prefix_and_kinds() {
        let m = KubernetesMetrics::new();
        m.set_lease("app-leader");
        m.set_leading(true);
        m.watch_config_map("flags");
        m.config_map("flags", true);
        let fams = m.collect();
        for f in &fams {
            assert!(f.name.starts_with("kubernetes_"), "{}", f.name);
            assert!(!f.help.is_empty());
        }
        let leader = family(&fams, "kubernetes_leader");
        assert!(matches!(leader.kind, MetricKind::Gauge));
        assert_eq!(leader.samples[0].value, 1.0);
        assert_eq!(
            leader.samples[0].labels,
            vec![("lease".to_owned(), "app-leader".to_owned())]
        );
        let acquired = family(&fams, "kubernetes_leader_acquired_total");
        assert!(matches!(acquired.kind, MetricKind::Counter));
        let updates = family(&fams, "kubernetes_config_map_updates_total");
        assert_eq!(
            updates.samples[0].labels,
            vec![("config_map".to_owned(), "flags".to_owned())]
        );
        assert_eq!(updates.samples[0].value, 1.0);
        for name in [
            "kubernetes_leader_lost_total",
            "kubernetes_lease_errors_total",
            "kubernetes_events_published_total",
            "kubernetes_events_failed_total",
            "kubernetes_api_up",
            "kubernetes_config_map_errors_total",
        ] {
            family(&fams, name);
        }
    }

    #[test]
    fn no_lease_families_without_leader_election() {
        let fams = KubernetesMetrics::new().collect();
        assert!(fams.iter().all(|f| !f.name.starts_with("kubernetes_lea")));
        assert_eq!(
            family(&fams, "kubernetes_api_up").samples.len(),
            0,
            "unknown: no sample"
        );
    }
}
