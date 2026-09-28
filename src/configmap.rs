//! Live ConfigMaps.
//!
//! The plugin watches each ConfigMap in `[kubernetes.config_maps] watch`.
//! The app reads the latest `data` from [`ConfigMapStore`]. `binaryData` is
//! not read.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use futures::StreamExt;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{ConfigMapEvent, KubeApi};
use crate::error::KubeError;
use crate::metrics::KubernetesMetrics;

/// Wait before a new watch when a watch stream ends.
pub(crate) const REWATCH_DELAY: Duration = Duration::from_secs(5);

/// ConfigMap data.
pub type ConfigMapData = Arc<BTreeMap<String, String>>;

/// The latest data of each watched ConfigMap. Clones share state.
#[derive(Debug, Clone)]
pub struct ConfigMapStore {
    maps: Arc<RwLock<BTreeMap<String, Entry>>>,
    generation: Arc<watch::Sender<u64>>,
}

#[derive(Debug, Clone, Default)]
struct Entry {
    synced: bool,
    data: Option<ConfigMapData>,
}

impl ConfigMapStore {
    /// Makes a store for these names. No data yet.
    #[must_use]
    pub fn new<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let maps = names
            .into_iter()
            .map(|n| (n.into(), Entry::default()))
            .collect();
        Self {
            maps: Arc::new(RwLock::new(maps)),
            generation: Arc::new(watch::channel(0).0),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Entry>> {
        self.maps.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// The watched names.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.read().keys().cloned().collect()
    }

    /// The latest data. `None` when the ConfigMap does not exist, is not
    /// watched, or no event came yet.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<ConfigMapData> {
        self.read().get(name).and_then(|e| e.data.clone())
    }

    /// One value.
    #[must_use]
    pub fn value(&self, name: &str, key: &str) -> Option<String> {
        self.get(name).and_then(|d| d.get(key).cloned())
    }

    /// One value, decoded from JSON. `Ok(None)` when the key is missing.
    ///
    /// # Errors
    /// Returns [`KubeError::Decode`] when the value is not valid JSON for `T`.
    pub fn json<T: DeserializeOwned>(&self, name: &str, key: &str) -> Result<Option<T>, KubeError> {
        let Some(raw) = self.value(name, key) else {
            return Ok(None);
        };
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|e| KubeError::Decode {
                name: name.to_owned(),
                key: key.to_owned(),
                message: e.to_string(),
            })
    }

    /// `true` after the first watch event for `name`.
    #[must_use]
    pub fn is_synced(&self, name: &str) -> bool {
        self.read().get(name).is_some_and(|e| e.synced)
    }

    /// A receiver whose value grows by one on each change.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    /// Applies one watch event. Unknown names are ignored.
    pub fn apply(&self, name: &str, event: ConfigMapEvent) {
        let data = match event {
            ConfigMapEvent::Applied(data) => Some(Arc::new(data)),
            ConfigMapEvent::Deleted => None,
        };
        let mut maps = self.maps.write().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = maps.get_mut(name) else {
            return;
        };
        entry.synced = true;
        entry.data = data;
        drop(maps);
        self.generation.send_modify(|g| *g = g.wrapping_add(1));
    }

    /// Reads the store from the app state.
    #[must_use]
    pub fn from_state(state: &autumn_web::AppState) -> Option<Arc<Self>> {
        state.extension::<Self>()
    }
}

/// Watches one ConfigMap into `store` until `cancel`.
pub(crate) fn spawn_watch(
    api: Arc<dyn KubeApi>,
    namespace: String,
    name: String,
    store: ConfigMapStore,
    metrics: Arc<KubernetesMetrics>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    metrics.watch_config_map(&name);
    tokio::spawn(async move {
        // Warn once per failure run; the count is in the metrics.
        let mut failing = false;
        loop {
            let mut stream = api.watch_config_map(&namespace, &name);
            loop {
                let item = tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    item = stream.next() => item,
                };
                match item {
                    Some(Ok(event)) => {
                        if failing {
                            tracing::info!(config_map = %name, "kubernetes: ConfigMap watch works again");
                            failing = false;
                        }
                        store.apply(&name, event);
                        metrics.config_map(&name, true);
                    }
                    Some(Err(e)) => {
                        metrics.config_map(&name, false);
                        if failing {
                            tracing::debug!(config_map = %name, error = %e, "kubernetes: ConfigMap watch error");
                        } else {
                            tracing::warn!(config_map = %name, class = e.class(), error = %e, "kubernetes: ConfigMap watch error");
                            failing = true;
                        }
                    }
                    None => break,
                }
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(REWATCH_DELAY) => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::MemoryKubeApi;
    use serde::Deserialize;

    fn data(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Flags {
        beta: bool,
    }

    #[test]
    fn store_reads_and_decodes() {
        let store = ConfigMapStore::new(["flags"]);
        assert_eq!(store.names(), vec!["flags".to_owned()]);
        assert!(!store.is_synced("flags"));
        assert_eq!(store.get("flags"), None);
        store.apply(
            "flags",
            ConfigMapEvent::Applied(data(&[
                ("json", r#"{"beta":true}"#),
                ("bad", "{"),
                ("s", "x"),
            ])),
        );
        assert!(store.is_synced("flags"));
        assert_eq!(store.value("flags", "s").as_deref(), Some("x"));
        assert_eq!(store.value("flags", "none"), None);
        assert_eq!(
            store.json::<Flags>("flags", "json").unwrap(),
            Some(Flags { beta: true })
        );
        assert_eq!(store.json::<Flags>("flags", "none").unwrap(), None);
        let err = store.json::<Flags>("flags", "bad").unwrap_err();
        assert!(
            matches!(err, KubeError::Decode { ref name, ref key, .. } if name == "flags" && key == "bad"),
            "{err}"
        );
        store.apply("flags", ConfigMapEvent::Deleted);
        assert_eq!(store.get("flags"), None);
        assert!(store.is_synced("flags"));
    }

    #[test]
    fn unknown_names_are_ignored() {
        let store = ConfigMapStore::new(Vec::<String>::new());
        store.apply("x", ConfigMapEvent::Applied(data(&[("k", "v")])));
        assert_eq!(store.get("x"), None);
        assert_eq!(*store.subscribe().borrow(), 0);
    }

    #[test]
    fn generation_grows_on_each_change() {
        let store = ConfigMapStore::new(["a"]);
        let rx = store.subscribe();
        store.apply("a", ConfigMapEvent::Applied(data(&[])));
        store.apply("a", ConfigMapEvent::Deleted);
        assert_eq!(*rx.borrow(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn watch_task_follows_the_api() {
        let api = MemoryKubeApi::new();
        let store = ConfigMapStore::new(["flags"]);
        let metrics = Arc::new(KubernetesMetrics::new());
        let cancel = CancellationToken::new();
        let task = spawn_watch(
            Arc::new(api.clone()),
            "ns".into(),
            "flags".into(),
            store.clone(),
            Arc::clone(&metrics),
            cancel.clone(),
        );
        let mut rx = store.subscribe();
        rx.changed().await.unwrap();
        assert!(store.is_synced("flags"));
        assert_eq!(store.get("flags"), None, "missing at start");
        api.put_config_map("ns", "flags", data(&[("k", "v1")]));
        rx.changed().await.unwrap();
        assert_eq!(store.value("flags", "k").as_deref(), Some("v1"));
        api.delete_config_map("ns", "flags");
        rx.changed().await.unwrap();
        assert_eq!(store.get("flags"), None);
        assert_eq!(metrics.snapshot().config_maps["flags"].updates, 3);
        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn watch_error_is_counted_then_retried() {
        let api = MemoryKubeApi::new();
        api.set_down(true);
        api.put_config_map("ns", "flags", data(&[("k", "v")]));
        let store = ConfigMapStore::new(["flags"]);
        let metrics = Arc::new(KubernetesMetrics::new());
        let cancel = CancellationToken::new();
        let task = spawn_watch(
            Arc::new(api.clone()),
            "ns".into(),
            "flags".into(),
            store.clone(),
            Arc::clone(&metrics),
            cancel.clone(),
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(metrics.snapshot().config_maps["flags"].errors >= 1);
        assert!(!store.is_synced("flags"));
        api.set_down(false);
        tokio::time::sleep(REWATCH_DELAY + Duration::from_secs(1)).await;
        assert_eq!(store.value("flags", "k").as_deref(), Some("v"));
        cancel.cancel();
        task.await.unwrap();
    }
}
