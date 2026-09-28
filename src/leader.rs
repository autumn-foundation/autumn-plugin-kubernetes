//! Leader election on a `coordination.k8s.io/v1` Lease, and leader tasks.
//!
//! The rules are in [`crate::policy`] (verified with Verus):
//! - Expiry uses the local time when the record last changed. Remote clocks
//!   have no effect.
//! - Belief ends `renew_deadline` after the last good write was sent, or at
//!   once when another holder or a write conflict is seen.
//! - Writes use `resourceVersion`. A conflict loses the race.

use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use autumn_web::AppState;
use futures::future::BoxFuture;
use k8s_openapi::jiff::{SignedDuration, Timestamp};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::api::{KubeApi, LeaseRecord};
use crate::config::LeaderElectionConfig;
use crate::error::KubeError;
use crate::events::EventSink;
use crate::metrics::KubernetesMetrics;
use crate::policy::{self, Action};

/// What this replica knows about the lease.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaderState {
    /// `true` while this replica believes it leads.
    pub leading: bool,
    /// The last holder seen. `None` when free or not read yet.
    pub holder: Option<String>,
}

/// A read handle on the election. Clones share state.
#[derive(Debug, Clone)]
pub struct Leadership {
    identity: Arc<str>,
    lease: Arc<str>,
    rx: watch::Receiver<LeaderState>,
}

impl Leadership {
    /// Returns `true` while this replica believes it leads. `false` when the
    /// elector has stopped (for example after a panic).
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.rx.has_changed().is_ok() && self.rx.borrow().leading
    }

    /// This replica's holder identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The lease name.
    #[must_use]
    pub fn lease_name(&self) -> &str {
        &self.lease
    }

    /// The last holder seen.
    #[must_use]
    pub fn holder(&self) -> Option<String> {
        self.rx.borrow().holder.clone()
    }

    /// A receiver that sees each change.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<LeaderState> {
        self.rx.clone()
    }

    /// Waits until this replica leads. Returns `false` if the elector stops
    /// first.
    pub async fn wait_until_leader(&self) -> bool {
        let mut rx = self.rx.clone();
        rx.wait_for(|s| s.leading).await.is_ok() && self.is_leader()
    }

    /// Waits until this replica does not lead. Returns at once when it does
    /// not lead now.
    pub async fn wait_until_follower(&self) {
        let mut rx = self.rx.clone();
        // A closed channel means the elector stopped: not leading.
        let _ = rx.wait_for(|s| !s.leading).await;
    }

    /// Reads the handle from the app state. `None` when leader election is
    /// off.
    #[must_use]
    pub fn from_state(state: &AppState) -> Option<Arc<Self>> {
        state.extension::<Self>()
    }
}

/// Runs leader election for one lease.
pub struct LeaderElector {
    api: Arc<dyn KubeApi>,
    namespace: String,
    identity: String,
    config: LeaderElectionConfig,
    metrics: Arc<KubernetesMetrics>,
    events: Option<EventSink>,
}

impl std::fmt::Debug for LeaderElector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderElector")
            .field("namespace", &self.namespace)
            .field("lease", &self.config.lease_name)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl LeaderElector {
    /// Makes an elector. `config.enabled` is not read.
    ///
    /// # Errors
    /// Returns [`KubeError::Config`] for a bad lease name, identity, or timing.
    pub fn new(
        api: Arc<dyn KubeApi>,
        namespace: impl Into<String>,
        identity: impl Into<String>,
        config: LeaderElectionConfig,
    ) -> Result<Self, KubeError> {
        let namespace = namespace.into();
        let identity = identity.into();
        let bad = |m: String| Err(KubeError::Config(m));
        if !crate::config::is_dns_label(&namespace) {
            return bad(format!(
                "leader election namespace {namespace:?} is not a DNS-1123 label"
            ));
        }
        if identity.trim().is_empty() {
            return bad("leader election identity is empty".to_owned());
        }
        let check = crate::config::KubernetesConfig {
            leader_election: LeaderElectionConfig {
                enabled: true,
                ..config.clone()
            },
            ..crate::config::KubernetesConfig::default()
        };
        check.validate()?;
        Ok(Self {
            api,
            namespace,
            identity,
            config,
            metrics: Arc::new(KubernetesMetrics::new()),
            events: None,
        })
    }

    /// Uses these metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<KubernetesMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_events(mut self, events: EventSink) -> Self {
        self.events = Some(events);
        self
    }

    /// Starts the election loop.
    #[must_use]
    pub fn start(self) -> ElectorHandle {
        self.metrics.set_lease(&self.config.lease_name);
        let (tx, rx) = watch::channel(LeaderState::default());
        let leadership = Leadership {
            identity: Arc::from(self.identity.as_str()),
            lease: Arc::from(self.config.lease_name.as_str()),
            rx,
        };
        let cancel = CancellationToken::new();
        let run = Loop {
            base: Instant::now(),
            wall_base: Timestamp::now(),
            observed: None,
            sent_ok: None,
            holder_is_me: false,
            ever_won: false,
            last_holder: None,
            tx,
            elector: self,
        };
        let task = tokio::spawn(run.run(cancel.clone()));
        ElectorHandle {
            leadership,
            cancel,
            task,
        }
    }
}

/// A random value for jitter. No extra crate: `RandomState` has random keys.
pub(crate) fn random_u64() -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish()
}

/// The state of one election loop.
struct Loop {
    elector: LeaderElector,
    tx: watch::Sender<LeaderState>,
    /// Start of the local monotonic time line.
    base: Instant,
    /// Wall time at `base`. Record times are `wall_base + elapsed`.
    wall_base: Timestamp,
    /// `resourceVersion` last seen, and the local time it was first seen.
    observed: Option<(Option<String>, u64)>,
    /// Local time when the last good write was sent.
    sent_ok: Option<u64>,
    /// The last read or write showed this replica as holder.
    holder_is_me: bool,
    /// A write was good at least once. Release only then.
    ever_won: bool,
    last_holder: Option<String>,
}

impl Loop {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn wall_now(&self) -> Timestamp {
        let elapsed = SignedDuration::try_from(self.base.elapsed()).unwrap_or(SignedDuration::MAX);
        self.wall_base
            .checked_add(elapsed)
            .unwrap_or_else(|_| Timestamp::now())
    }

    const fn cfg(&self) -> &LeaderElectionConfig {
        &self.elector.config
    }

    fn leading_now(&self) -> bool {
        policy::leading(
            self.now_ms(),
            self.sent_ok,
            self.cfg().renew_deadline_ms(),
            self.holder_is_me,
        )
    }

    /// When belief ends, if this replica leads now.
    fn belief_deadline(&self) -> Option<Instant> {
        if !self.leading_now() {
            return None;
        }
        let end = self.sent_ok?.saturating_add(self.cfg().renew_deadline_ms());
        Some(self.base + Duration::from_millis(end))
    }

    /// Sends the current state. Counts and reports each edge.
    fn publish(&self, stopping: bool) {
        let leading = self.leading_now();
        let holder = self.last_holder.clone();
        let mut edge = None;
        self.tx.send_if_modified(|st| {
            if st.leading != leading {
                edge = Some(leading);
            }
            let changed = st.leading != leading || st.holder != holder;
            st.leading = leading;
            st.holder.clone_from(&holder);
            changed
        });
        let Some(now_leading) = edge else {
            return;
        };
        let e = &self.elector;
        e.metrics.set_leading(now_leading);
        if now_leading {
            tracing::info!(lease = %e.config.lease_name, identity = %e.identity, "kubernetes: became leader");
            if let Some(ev) = &e.events {
                ev.spawn(crate::events::leader_elected(
                    &e.config.lease_name,
                    &e.identity,
                ));
            }
        } else {
            tracing::warn!(lease = %e.config.lease_name, identity = %e.identity, "kubernetes: stopped leading");
            if !stopping && let Some(ev) = &e.events {
                ev.spawn(crate::events::leader_lost(
                    &e.config.lease_name,
                    &e.identity,
                ));
            }
        }
    }

    fn won(&mut self, sent: u64, stored: &LeaseRecord) {
        self.sent_ok = Some(sent);
        self.holder_is_me = true;
        self.ever_won = true;
        self.observed = Some((stored.resource_version.clone(), self.now_ms()));
        self.last_holder = Some(self.elector.identity.clone());
    }

    fn fresh_record(&self, from: &LeaseRecord) -> LeaseRecord {
        let now = self.wall_now();
        LeaseRecord {
            holder: Some(self.elector.identity.clone()),
            lease_duration_secs: i32::try_from(self.cfg().lease_duration_secs).unwrap_or(i32::MAX),
            acquire_time: Some(now),
            renew_time: Some(now),
            transitions: policy::next_transitions(from.transitions, false),
            resource_version: from.resource_version.clone(),
        }
    }

    /// One read, and at most one write.
    async fn tick(&mut self) -> Result<(), KubeError> {
        let sent = self.now_ms();
        let e = &self.elector;
        let current = e.api.get_lease(&e.namespace, &e.config.lease_name).await?;
        let Some(rec) = current else {
            let mut fresh = self.fresh_record(&LeaseRecord::default());
            fresh.transitions = 0;
            let e = &self.elector;
            let stored = e
                .api
                .create_lease(&e.namespace, &e.config.lease_name, &fresh)
                .await?;
            self.won(sent, &stored);
            return Ok(());
        };
        let now = self.now_ms();
        let changed = self
            .observed
            .as_ref()
            .is_none_or(|(rv, _)| *rv != rec.resource_version);
        let at = policy::observed_at(self.observed.as_ref().map(|o| o.1), changed, now);
        self.observed = Some((rec.resource_version.clone(), at));
        self.last_holder = rec.holder.clone().filter(|h| !h.is_empty());
        let me = rec.held_by(&self.elector.identity);
        if !me {
            // Another holder: belief ends at once.
            self.holder_is_me = false;
        }
        let duration =
            policy::effective_duration_ms(rec.lease_duration_secs, self.cfg().lease_duration_ms());
        let next = match policy::decide(
            me,
            rec.is_free(),
            policy::observed_age_ms(now, at),
            duration,
        ) {
            Action::Wait => return Ok(()),
            Action::Renew => {
                let mut next = rec.clone();
                next.renew_time = Some(self.wall_now());
                next.lease_duration_secs =
                    i32::try_from(self.cfg().lease_duration_secs).unwrap_or(i32::MAX);
                next
            }
            Action::Acquire => self.fresh_record(&rec),
        };
        let e = &self.elector;
        let stored = e
            .api
            .replace_lease(&e.namespace, &e.config.lease_name, &next)
            .await?;
        self.won(sent, &stored);
        Ok(())
    }

    fn failed(&mut self, err: &KubeError) {
        self.elector.metrics.lease_error();
        if matches!(err, KubeError::Conflict(_)) {
            // Another writer won. Do not trust the old holder view.
            self.holder_is_me = false;
        }
        tracing::debug!(lease = %self.elector.config.lease_name, error = %err, "kubernetes: lease call failed");
    }

    async fn run(mut self, cancel: CancellationToken) {
        let call_timeout = Duration::from_millis(self.cfg().renew_deadline_ms());
        'outer: loop {
            let deadline = self.belief_deadline();
            let outcome = tokio::select! {
                biased;
                () = cancel.cancelled() => break 'outer,
                r = tokio::time::timeout(call_timeout, self.tick()) => Some(r),
                () = sleep_until_opt(deadline) => None,
            };
            match outcome {
                Some(Ok(Ok(()))) | None => {}
                Some(Ok(Err(e))) => self.failed(&e),
                Some(Err(_)) => self.failed(&KubeError::Api("lease call timed out".to_owned())),
            }
            self.publish(false);
            let wait = policy::jittered_ms(self.cfg().retry_period_ms(), random_u64());
            let wake = Instant::now() + Duration::from_millis(wait);
            loop {
                let at = self.belief_deadline().filter(|d| *d < wake).unwrap_or(wake);
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break 'outer,
                    () = tokio::time::sleep_until(at) => {}
                }
                self.publish(false);
                if Instant::now() >= wake {
                    break;
                }
            }
        }
        // Belief ends before the release write.
        self.holder_is_me = false;
        self.sent_ok = None;
        self.publish(true);
        if self.cfg().release_on_shutdown && self.ever_won {
            let release = tokio::time::timeout(call_timeout, self.release()).await;
            match release {
                Ok(Ok(())) => {}
                Ok(Err(e)) => self.failed(&e),
                Err(_) => self.failed(&KubeError::Api("lease release timed out".to_owned())),
            }
        }
    }

    /// Clears the holder when this replica still holds the lease.
    async fn release(&self) -> Result<(), KubeError> {
        let e = &self.elector;
        let Some(rec) = e.api.get_lease(&e.namespace, &e.config.lease_name).await? else {
            return Ok(());
        };
        if !rec.held_by(&e.identity) {
            return Ok(());
        }
        let now = self.wall_now();
        let next = LeaseRecord {
            holder: None,
            lease_duration_secs: 1,
            acquire_time: Some(now),
            renew_time: Some(now),
            ..rec
        };
        e.api
            .replace_lease(&e.namespace, &e.config.lease_name, &next)
            .await?;
        tracing::info!(lease = %e.config.lease_name, "kubernetes: lease released");
        Ok(())
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// A running elector.
#[derive(Debug)]
pub struct ElectorHandle {
    leadership: Leadership,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl ElectorHandle {
    /// A read handle.
    #[must_use]
    pub fn leadership(&self) -> Leadership {
        self.leadership.clone()
    }

    /// Stops the loop. Belief ends first. Then, with `release_on_shutdown`,
    /// the lease is cleared so another replica takes over at once.
    pub async fn stop(self) {
        self.cancel.cancel();
        if let Err(e) = self.task.await {
            tracing::warn!(error = %e, "kubernetes: elector task ended with an error");
        }
    }

    /// Stops the loop like a crash: no release. For failover tests. This
    /// handle then shows "not leading", as a dead process believes nothing.
    pub fn abort(self) {
        self.task.abort();
    }
}

type TaskFn = Arc<dyn Fn(AppState, CancellationToken) -> BoxFuture<'static, ()> + Send + Sync>;

/// Work that runs only on the leader.
///
/// The task starts when this replica becomes leader. The token is cancelled
/// when leadership ends or the app stops. The task must then return soon.
/// One run per term. A task that returns early does not run again until the
/// next term.
#[derive(Clone)]
pub struct LeaderTask {
    name: String,
    run: TaskFn,
}

impl std::fmt::Debug for LeaderTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderTask")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl LeaderTask {
    /// Makes a task.
    pub fn new<F, Fut>(name: impl Into<String>, run: F) -> Self
    where
        F: Fn(AppState, CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self {
            name: name.into(),
            run: Arc::new(move |state, cancel| Box::pin(run(state, cancel))),
        }
    }

    /// The task name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Runs leader tasks for each term.
#[derive(Debug)]
pub struct LeaderTasks {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl LeaderTasks {
    /// Starts the supervisor. `stop_timeout` limits the wait for tasks to
    /// return after their token is cancelled. Then they are aborted.
    #[must_use]
    pub fn start(
        leadership: Leadership,
        tasks: Vec<LeaderTask>,
        state: AppState,
        stop_timeout: Duration,
    ) -> Self {
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervise(
            leadership,
            tasks,
            state,
            stop_timeout,
            cancel.clone(),
        ));
        Self { cancel, task }
    }

    /// Cancels running tasks, waits for them, and stops.
    pub async fn stop(self) {
        self.cancel.cancel();
        if let Err(e) = self.task.await {
            tracing::warn!(error = %e, "kubernetes: leader task supervisor ended with an error");
        }
    }
}

async fn supervise(
    leadership: Leadership,
    tasks: Vec<LeaderTask>,
    state: AppState,
    stop_timeout: Duration,
    cancel: CancellationToken,
) {
    let mut rx = leadership.subscribe();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            r = rx.wait_for(|s| s.leading) => if r.is_err() { return },
        }
        let term = cancel.child_token();
        let handles: Vec<(String, JoinHandle<()>)> = tasks
            .iter()
            .map(|t| {
                tracing::info!(task = %t.name, "kubernetes: leader task started");
                (
                    t.name.clone(),
                    tokio::spawn((t.run)(state.clone(), term.clone())),
                )
            })
            .collect();
        tokio::select! {
            biased;
            () = cancel.cancelled() => {}
            _ = rx.wait_for(|s| !s.leading) => {}
        }
        term.cancel();
        let deadline = Instant::now() + stop_timeout;
        for (name, mut handle) in handles {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                tracing::warn!(task = %name, "kubernetes: leader task did not stop in time; aborted");
                handle.abort();
            }
        }
        if cancel.is_cancelled() {
            return;
        }
    }
}
