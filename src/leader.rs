//! Leader election on a `coordination.k8s.io/v1` Lease, and leader tasks.
//!
//! The rules are in [`crate::policy`] (verified with Verus):
//! - Expiry uses the local time when the record last changed. Remote clocks
//!   have no effect.
//! - Belief stops `renew_deadline` after the elector sends its last
//!   successful write. Belief also stops immediately when the elector sees
//!   another holder or a write conflict.
//! - Readers check the belief deadline on their own clock. A starved elector
//!   cannot leave a stale "leading".
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
    /// `true` while this replica believes it leads, up to `valid_until`.
    pub leading: bool,
    /// The last holder seen. `None` when free or not read yet.
    pub holder: Option<String>,
    /// Belief stops at this time, also when the elector cannot run.
    /// `None`: no time limit (detached local leader).
    pub valid_until: Option<Instant>,
}

impl LeaderState {
    /// Returns `true` when this state says "leading" at time `now`.
    #[must_use]
    pub fn leading_at(&self, now: Instant) -> bool {
        self.leading && self.valid_until.is_none_or(|until| now < until)
    }
}

/// Waits until the state is "leading" now. Returns `false` when the elector
/// stops.
async fn wait_leading(rx: &mut watch::Receiver<LeaderState>) -> bool {
    loop {
        if rx.has_changed().is_err() {
            return false;
        }
        if rx.borrow_and_update().leading_at(Instant::now()) {
            return true;
        }
        if rx.changed().await.is_err() {
            return false;
        }
    }
}

/// Waits until the state is not "leading": the flag goes off, the deadline
/// passes, or the elector stops.
async fn wait_not_leading(rx: &mut watch::Receiver<LeaderState>) {
    loop {
        if rx.has_changed().is_err() {
            return;
        }
        let state = rx.borrow_and_update().clone();
        if !state.leading_at(Instant::now()) {
            return;
        }
        tokio::select! {
            biased;
            r = rx.changed() => if r.is_err() { return },
            () = sleep_until_opt(state.valid_until) => {}
        }
    }
}

/// A read handle on the election. Clones share state.
#[derive(Debug, Clone)]
pub struct Leadership {
    identity: Arc<str>,
    lease: Arc<str>,
    rx: watch::Receiver<LeaderState>,
}

impl Leadership {
    /// Returns `true` while this replica believes it leads. It reads the
    /// belief deadline on the caller's clock, so a starved elector cannot
    /// leave a stale "leading". `false` when the elector has stopped.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.rx.has_changed().is_ok() && self.rx.borrow().leading_at(Instant::now())
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
        wait_leading(&mut self.rx.clone()).await
    }

    /// Waits until this replica does not lead. Returns at once when it does
    /// not lead now.
    pub async fn wait_until_follower(&self) {
        wait_not_leading(&mut self.rx.clone()).await;
    }

    /// Reads the handle from the app state. `None` when leader election is
    /// off.
    #[must_use]
    pub fn from_state(state: &AppState) -> Option<Arc<Self>> {
        state.extension::<Self>()
    }
}

/// A handle that always leads, for detached local development. Leadership
/// ends when the sender sends `leading = false` or is dropped.
pub(crate) fn local_leader(
    identity: &str,
    lease: &str,
) -> (watch::Sender<LeaderState>, Leadership) {
    let (tx, rx) = watch::channel(LeaderState {
        leading: true,
        holder: Some(identity.to_owned()),
        valid_until: None,
    });
    let leadership = Leadership {
        identity: Arc::from(identity),
        lease: Arc::from(lease),
        rx,
    };
    (tx, leadership)
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
        let no_release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let run = Loop {
            no_release: Arc::clone(&no_release),
            base: Instant::now(),
            wall_base: Timestamp::now(),
            observed: None,
            sent_ok: None,
            holder_is_me: false,
            ever_won: false,
            failing: false,
            last_holder: None,
            tx,
            elector: self,
        };
        let task = tokio::spawn(run.run(cancel.clone()));
        ElectorHandle {
            leadership,
            cancel,
            no_release,
            task: Some(task),
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
    /// Set by a drop or an abort: stop with no release write. Leader tasks
    /// may still run then, so the lease must expire, not be freed.
    no_release: Arc<std::sync::atomic::AtomicBool>,
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
    /// The last call failed. Warn once per failure run.
    failing: bool,
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
        let valid_until = self.belief_deadline();
        let holder = self.last_holder.clone();
        let mut edge = None;
        self.tx.send_if_modified(|st| {
            if st.leading != leading {
                edge = Some(leading);
            }
            let changed =
                st.leading != leading || st.holder != holder || st.valid_until != valid_until;
            st.leading = leading;
            st.holder.clone_from(&holder);
            st.valid_until = valid_until;
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
        let lease = &self.elector.config.lease_name;
        if matches!(err, KubeError::Conflict(_)) {
            // Another writer won the race. That is normal in an election:
            // count it apart, and do not warn. Do not trust the old view.
            self.holder_is_me = false;
            self.elector.metrics.lease_conflict();
            tracing::debug!(lease = %lease, "kubernetes: lease write lost a race");
            return;
        }
        self.elector.metrics.lease_error();
        if self.failing {
            tracing::debug!(lease = %lease, error = %err, "kubernetes: lease call failed");
        } else {
            // Warn once per failure run. The count is in the metrics.
            tracing::warn!(lease = %lease, class = err.class(), error = %err, "kubernetes: lease call failed");
            self.failing = true;
        }
    }

    fn succeeded(&mut self) {
        if self.failing {
            tracing::info!(lease = %self.elector.config.lease_name, "kubernetes: lease calls work again");
            self.failing = false;
        }
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
                Some(Ok(Ok(()))) => self.succeeded(),
                None => {}
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
        let released_ok = !self.no_release.load(std::sync::atomic::Ordering::SeqCst);
        if self.cfg().release_on_shutdown && self.ever_won && released_ok {
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
///
/// A drop stops it in the background with no release: leader tasks can
/// still run, so the lease expires after `lease_duration`. Call
/// [`ElectorHandle::stop`] (after the tasks stop) to release it.
#[derive(Debug)]
pub struct ElectorHandle {
    leadership: Leadership,
    cancel: CancellationToken,
    no_release: Arc<std::sync::atomic::AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl Drop for ElectorHandle {
    fn drop(&mut self) {
        if self.task.is_some() {
            self.no_release
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.cancel.cancel();
    }
}

impl ElectorHandle {
    /// A read handle.
    #[must_use]
    pub fn leadership(&self) -> Leadership {
        self.leadership.clone()
    }

    /// Stops the loop. Belief stops first. If `release_on_shutdown` is true,
    /// the elector then clears the holder. Another replica can then take the
    /// lease immediately.
    pub async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take()
            && let Err(e) = task.await
        {
            tracing::warn!(error = %e, "kubernetes: elector task ended with an error");
        }
    }

    /// Stops the loop like a crash: no release. For failover tests. After
    /// the runtime next runs the aborted task, `is_leader` returns `false`.
    pub fn abort(mut self) {
        self.no_release
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

type TaskFn = Arc<dyn Fn(AppState, CancellationToken) -> BoxFuture<'static, ()> + Send + Sync>;

/// Work that runs only on the leader.
///
/// The task starts when this replica becomes leader. The supervisor cancels
/// the token when leadership stops or the app stops. The task must then
/// return before the stop timeout: `lease_duration - renew_deadline` less 20%
/// (4 s with the defaults), at most 10 s. After that time, the supervisor
/// aborts the task. The task runs one time in each term. If it returns early,
/// it runs again only in the next term.
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

/// Runs leader tasks for each term. A drop cancels the running tasks.
#[derive(Debug)]
pub struct LeaderTasks {
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl Drop for LeaderTasks {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl LeaderTasks {
    /// Starts the supervisor. `stop_timeout` is the time that tasks get to
    /// return after the supervisor cancels their token. After this time, the
    /// supervisor aborts the tasks.
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
        Self {
            cancel,
            task: Some(task),
        }
    }

    /// Cancels running tasks, waits for them, and stops.
    pub async fn stop(mut self) {
        self.cancel.cancel();
        let Some(task) = self.task.take() else {
            return;
        };
        if let Err(e) = task.await {
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
            leading = wait_leading(&mut rx) => if !leading { return },
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
            () = wait_not_leading(&mut rx) => {}
        }
        term.cancel();
        let deadline = Instant::now().checked_add(stop_timeout);
        for (name, mut handle) in handles {
            let done = if let Some(at) = deadline {
                tokio::time::timeout_at(at, &mut handle).await.is_ok()
            } else {
                let _ = (&mut handle).await;
                true
            };
            if !done {
                tracing::warn!(task = %name, "kubernetes: leader task did not stop in time; aborted");
                handle.abort();
                // Wait for the abort, so no old task runs in the next term.
                let _ = handle.await;
            }
        }
        if cancel.is_cancelled() || rx.has_changed().is_err() {
            return;
        }
    }
}
