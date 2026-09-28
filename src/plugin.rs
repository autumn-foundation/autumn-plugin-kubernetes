//! `KubernetesPlugin` and the started runtime.

use std::borrow::Cow;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use autumn_web::actuator::{HealthIndicator, MetricsSource};
use autumn_web::app::AppBuilder;
use autumn_web::plugin::Plugin;
use autumn_web::{AppState, ProcessRole};
use futures::future::BoxFuture;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{KubeApi, KubeClientApi, PodRef};
use crate::config::{KubernetesConfig, SECTION};
use crate::configmap::{self, ConfigMapStore};
use crate::error::KubeError;
use crate::events::{self, EventSink};
use crate::health::{HealthTarget, KubernetesHealth};
use crate::leader::{ElectorHandle, LeaderElector, LeaderTask, LeaderTasks, Leadership};
use crate::metrics::KubernetesMetrics;
use crate::pod::PodInfo;

/// Plugin name for duplicate detection.
pub const PLUGIN_NAME: &str = "autumn-plugin-kubernetes";
/// Time limit for the startup API check when `required = true`.
pub const STARTUP_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
/// Time for leader tasks to return after their token is cancelled.
pub const TASK_STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// Time limit for the `Stopping` event at shutdown.
pub const STOP_EVENT_TIMEOUT: Duration = Duration::from_secs(2);

type Connector = Arc<
    dyn Fn(Option<String>) -> BoxFuture<'static, Result<kube::Client, KubeError>> + Send + Sync,
>;

/// Kubernetes plugin.
///
/// ```rust,ignore
/// autumn_web::app()
///     .plugin(KubernetesPlugin::new().leader_task(LeaderTask::new("sweeper", sweep)))
///     .run()
///     .await;
/// ```
pub struct KubernetesPlugin {
    config: Option<KubernetesConfig>,
    api: Option<Arc<dyn KubeApi>>,
    connector: Option<Connector>,
    pod: Option<PodInfo>,
    readiness: bool,
    leader_tasks: Vec<LeaderTask>,
}

impl std::fmt::Debug for KubernetesPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubernetesPlugin")
            .field("config", &self.config)
            .field("readiness", &self.readiness)
            .field("leader_tasks", &self.leader_tasks)
            .finish_non_exhaustive()
    }
}

impl Default for KubernetesPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl KubernetesPlugin {
    /// Makes the plugin. It reads `[kubernetes]` at startup, with the app
    /// profile and `AUTUMN_KUBERNETES__*` env vars.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            config: None,
            api: None,
            connector: None,
            pod: None,
            readiness: false,
            leader_tasks: Vec::new(),
        }
    }

    /// Makes the plugin with this config. It reads no files.
    #[must_use]
    pub fn with_config(config: KubernetesConfig) -> Self {
        Self {
            config: Some(config),
            ..Self::new()
        }
    }

    /// Uses this API. Default: a `kube::Client` from the cluster.
    #[must_use]
    pub fn with_api(self, api: impl KubeApi) -> Self {
        self.with_api_arc(Arc::new(api))
    }

    /// Uses this shared API.
    #[must_use]
    pub fn with_api_arc(mut self, api: Arc<dyn KubeApi>) -> Self {
        self.api = Some(api);
        self
    }

    /// Uses this function to make the client. It gets the pod name.
    /// Default: in cluster, else the kubeconfig. Return
    /// [`KubeError::NoCluster`] for "no cluster here".
    #[must_use]
    pub fn with_connector<F, Fut>(mut self, connect: F) -> Self
    where
        F: Fn(Option<String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<kube::Client, KubeError>> + Send + 'static,
    {
        self.connector = Some(Arc::new(move |pod| Box::pin(connect(pod))));
        self
    }

    /// Uses this pod info. Default: [`PodInfo::from_env`].
    #[must_use]
    pub fn with_pod_info(mut self, pod: PodInfo) -> Self {
        self.pod = Some(pod);
        self
    }

    /// Puts the health indicator in `/ready` too. Default: `/health` only.
    ///
    /// autumn reads this when the app builds, before the config loads. So it
    /// is a builder setting, not a config key.
    #[must_use]
    pub const fn readiness(mut self, on: bool) -> Self {
        self.readiness = on;
        self
    }

    /// Adds a task that runs only on the leader. Needs
    /// `leader_election.enabled = true`.
    #[must_use]
    pub fn leader_task(mut self, task: LeaderTask) -> Self {
        self.leader_tasks.push(task);
        self
    }

    /// Starts the plugin outside an app build, with the role of `state`.
    ///
    /// Use it in tests. [`Plugin::build`] does the same at app startup.
    ///
    /// # Errors
    /// Returns a config or connection error.
    pub async fn start(self, state: &AppState) -> Result<KubernetesRuntime, KubeError> {
        self.start_with_role(state, state.role()).await
    }

    /// Starts the plugin as if the process has `role`.
    ///
    /// # Errors
    /// Same as [`Self::start`].
    pub async fn start_with_role(
        self,
        state: &AppState,
        role: ProcessRole,
    ) -> Result<KubernetesRuntime, KubeError> {
        let metrics = Arc::new(KubernetesMetrics::new());
        let health = Arc::new(KubernetesHealth::new(self.readiness, Arc::clone(&metrics)));
        self.start_inner(state, role, metrics, health).await
    }

    /// Connects with the connector or the default client.
    async fn connect(&self, pod: &PodInfo) -> Result<(Arc<dyn KubeApi>, &'static str), KubeError> {
        if let Some(api) = &self.api {
            return Ok((Arc::clone(api), "custom"));
        }
        if let Some(connect) = &self.connector {
            let client = connect(pod.name.clone()).await?;
            return Ok((
                Arc::new(KubeClientApi::new(client, pod.name.clone())),
                "custom",
            ));
        }
        let api = KubeClientApi::connect(pod.name.clone()).await?;
        let mode = if pod.in_cluster && std::env::var_os("KUBECONFIG").is_none() {
            "in_cluster"
        } else {
            "kubeconfig"
        };
        Ok((Arc::new(api), mode))
    }

    #[allow(clippy::too_many_lines)] // One linear setup sequence.
    async fn start_inner(
        self,
        state: &AppState,
        role: ProcessRole,
        metrics: Arc<KubernetesMetrics>,
        health: Arc<KubernetesHealth>,
    ) -> Result<KubernetesRuntime, KubeError> {
        let config = match &self.config {
            Some(c) => c.clone(),
            // autumn sets the state profile from the config; "default" means none.
            None => KubernetesConfig::load(Some(state.profile()).filter(|p| *p != "default"))?,
        };
        config.validate()?;
        if !self.leader_tasks.is_empty() && config.enabled && !config.leader_election.enabled {
            return Err(KubeError::Config(
                "leader tasks need leader_election.enabled = true".to_owned(),
            ));
        }
        let pod = self.pod.clone().unwrap_or_else(PodInfo::from_env);
        state.insert_extension(pod.clone());
        let idle = |mode: &'static str| KubernetesRuntime {
            mode,
            namespace: None,
            cancel: CancellationToken::new(),
            watches: Vec::new(),
            elector: None,
            tasks: None,
            events: None,
            metrics: Arc::clone(&metrics),
            health: Arc::clone(&health),
            leadership: None,
            config_maps: None,
        };
        let detached = |mode: &'static str| {
            health.set(HealthTarget {
                api: None,
                mode,
                namespace: None,
                leadership: None,
                config_maps: None,
            });
            tracing::info!(mode, "kubernetes plugin started without a cluster");
            idle(mode)
        };
        if !config.enabled {
            return Ok(detached("disabled"));
        }
        let (api, mode) = match self.connect(&pod).await {
            Ok(found) => found,
            Err(KubeError::NoCluster(why)) if !config.required => {
                tracing::debug!(reason = %why, "kubernetes: no cluster found");
                return Ok(detached("detached"));
            }
            Err(e) => return Err(e),
        };
        if config.required {
            match tokio::time::timeout(STARTUP_CHECK_TIMEOUT, api.server_version()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(KubeError::Api(
                        "API server did not answer /version in time".to_owned(),
                    ));
                }
            }
        }
        let namespace = [Some(config.namespace.clone()), pod.namespace.clone()]
            .into_iter()
            .flatten()
            .find(|n| !n.is_empty())
            .unwrap_or_else(|| api.default_namespace());
        if let Some(client) = api.client() {
            state.insert_extension(client);
        }
        let pod_ref = pod
            .name
            .clone()
            .filter(|_| config.events)
            .map(|name| PodRef {
                namespace: pod.namespace.clone().unwrap_or_else(|| namespace.clone()),
                name,
                uid: pod.uid.clone(),
            });
        let events = EventSink::new(Arc::clone(&api), pod_ref, Arc::clone(&metrics));
        let cancel = CancellationToken::new();

        let config_maps = (!config.config_maps.watch.is_empty()).then(|| {
            let store = ConfigMapStore::new(config.config_maps.watch.clone());
            state.insert_extension(store.clone());
            store
        });
        let watches: Vec<JoinHandle<()>> = config_maps
            .iter()
            .flat_map(|store| {
                store.names().into_iter().map(|name| {
                    configmap::spawn_watch(
                        Arc::clone(&api),
                        namespace.clone(),
                        name,
                        store.clone(),
                        Arc::clone(&metrics),
                        cancel.clone(),
                    )
                })
            })
            .collect();

        let le = &config.leader_election;
        let (elector, tasks, leadership) = if le.enabled && le.campaigns_for(role) {
            let identity = if le.identity.is_empty() {
                default_identity(&pod)
            } else {
                le.identity.clone()
            };
            let handle =
                LeaderElector::new(Arc::clone(&api), namespace.clone(), identity, le.clone())?
                    .with_metrics(Arc::clone(&metrics))
                    .with_events(events.clone())
                    .start();
            let leadership = handle.leadership();
            state.insert_extension(leadership.clone());
            let tasks = (!self.leader_tasks.is_empty()).then(|| {
                LeaderTasks::start(
                    leadership.clone(),
                    self.leader_tasks,
                    state.clone(),
                    TASK_STOP_TIMEOUT,
                )
            });
            (Some(handle), tasks, Some(leadership))
        } else {
            if le.enabled {
                tracing::info!(
                    role = role.as_str(),
                    "kubernetes: this role does not take part in leader election"
                );
            }
            (None, None, None)
        };

        health.set(HealthTarget {
            api: Some(Arc::clone(&api)),
            mode,
            namespace: Some(namespace.clone()),
            leadership: leadership.clone(),
            config_maps: config_maps.clone(),
        });
        events.spawn(events::started(mode));
        tracing::info!(mode, namespace = %namespace, role = role.as_str(), "kubernetes plugin started");
        Ok(KubernetesRuntime {
            mode,
            namespace: Some(namespace),
            cancel,
            watches,
            elector,
            tasks,
            events: Some(events),
            metrics,
            health,
            leadership,
            config_maps,
        })
    }
}

/// `<pod name or host name>-<8 hex digits>`. The suffix keeps two processes
/// in one pod apart.
fn default_identity(pod: &PodInfo) -> String {
    let base = pod.name.clone().unwrap_or_else(|| "autumn".to_owned());
    format!("{base}-{:08x}", crate::leader::random_u64() & 0xffff_ffff)
}

/// A started plugin. Call [`KubernetesRuntime::shutdown`] to stop.
pub struct KubernetesRuntime {
    mode: &'static str,
    namespace: Option<String>,
    cancel: CancellationToken,
    watches: Vec<JoinHandle<()>>,
    elector: Option<ElectorHandle>,
    tasks: Option<LeaderTasks>,
    events: Option<EventSink>,
    metrics: Arc<KubernetesMetrics>,
    health: Arc<KubernetesHealth>,
    leadership: Option<Leadership>,
    config_maps: Option<ConfigMapStore>,
}

impl std::fmt::Debug for KubernetesRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubernetesRuntime")
            .field("mode", &self.mode)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl KubernetesRuntime {
    /// `in_cluster`, `kubeconfig`, `custom`, `detached`, or `disabled`.
    #[must_use]
    pub const fn mode(&self) -> &'static str {
        self.mode
    }

    /// The namespace in use. `None` when detached or disabled.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// The election handle, when this replica takes part.
    #[must_use]
    pub fn leadership(&self) -> Option<Leadership> {
        self.leadership.clone()
    }

    /// The ConfigMap store, when ConfigMaps are watched.
    #[must_use]
    pub fn config_maps(&self) -> Option<ConfigMapStore> {
        self.config_maps.clone()
    }

    /// The metrics.
    #[must_use]
    pub fn metrics(&self) -> Arc<KubernetesMetrics> {
        Arc::clone(&self.metrics)
    }

    /// The health indicator.
    #[must_use]
    pub fn health(&self) -> Arc<KubernetesHealth> {
        Arc::clone(&self.health)
    }

    /// Writes `Stopping`, stops leader tasks, releases the lease, and stops
    /// the watches.
    pub async fn shutdown(self) {
        if let Some(events) = &self.events
            && tokio::time::timeout(STOP_EVENT_TIMEOUT, events.emit(events::stopping()))
                .await
                .is_err()
        {
            tracing::debug!("kubernetes: Stopping event timed out");
        }
        if let Some(tasks) = self.tasks {
            tasks.stop().await;
        }
        if let Some(elector) = self.elector {
            elector.stop().await;
        }
        self.cancel.cancel();
        for handle in self.watches {
            if let Err(e) = handle.await {
                tracing::warn!(error = %e, "kubernetes: watch task ended with an error");
            }
        }
    }
}

impl Plugin for KubernetesPlugin {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(PLUGIN_NAME)
    }

    fn build(self, app: AppBuilder) -> AppBuilder {
        let metrics = Arc::new(KubernetesMetrics::new());
        let health = Arc::new(KubernetesHealth::new(self.readiness, Arc::clone(&metrics)));
        let pending = Arc::new(Mutex::new(Some(self)));
        let started: Arc<Mutex<Option<KubernetesRuntime>>> = Arc::default();
        let (m, h, s) = (
            Arc::clone(&metrics),
            Arc::clone(&health),
            Arc::clone(&started),
        );
        let stop = Arc::clone(&started);
        app.config_section(SECTION)
            .metrics_source("kubernetes", metrics as Arc<dyn MetricsSource>)
            .health_indicator("kubernetes", health as Arc<dyn HealthIndicator>)
            .on_startup(move |state| {
                let plugin = pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                let (m, h, s) = (Arc::clone(&m), Arc::clone(&h), Arc::clone(&s));
                async move {
                    let Some(plugin) = plugin else {
                        return Ok(());
                    };
                    let role = state.role();
                    let rt = plugin.start_inner(&state, role, m, h).await?;
                    *s.lock().unwrap_or_else(PoisonError::into_inner) = Some(rt);
                    Ok(())
                }
            })
            .on_shutdown(move || {
                let rt = stop.lock().unwrap_or_else(PoisonError::into_inner).take();
                async move {
                    if let Some(rt) = rt {
                        rt.shutdown().await;
                    }
                }
            })
    }
}
