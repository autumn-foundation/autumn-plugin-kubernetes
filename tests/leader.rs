//! Leader election and leader tasks on the fake API (AC4, AC5, AC6).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::similar_names
)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_plugin_kubernetes::api::{KubeApi, LeaseRecord, MemoryKubeApi};
use autumn_plugin_kubernetes::metrics::KubernetesMetrics;
use autumn_plugin_kubernetes::{KubeError, LeaderElector, LeaderTask, LeaderTasks, Leadership};
use autumn_web::AppState;
use common::{LEASE, NS, advance, elect, leader_config, wait_until};
use tokio::time::Instant;

const S: fn(u64) -> Duration = Duration::from_secs;

#[tokio::test(start_paused = true)]
async fn single_candidate_creates_and_leads() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    let lead = a.leadership();
    assert_eq!(lead.identity(), "a");
    assert_eq!(lead.lease_name(), LEASE);
    assert!(lead.wait_until_leader().await);
    assert!(lead.is_leader());
    assert_eq!(lead.holder().as_deref(), Some("a"));
    let rec = api.lease(NS, LEASE).unwrap();
    assert!(rec.held_by("a"));
    assert_eq!(rec.lease_duration_secs, 15);
    assert_eq!(rec.transitions, 0);
    assert!(rec.acquire_time.is_some() && rec.renew_time.is_some());
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn leader_renews_every_retry_period() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    assert!(a.leadership().wait_until_leader().await);
    let first = api.lease(NS, LEASE).unwrap();
    let writes = api.lease_writes();
    advance(S(10)).await;
    // 10 s / (2 s to 2.4 s) = 4 or 5 renews.
    assert!(api.lease_writes() - writes >= 4, "{}", api.lease_writes());
    let later = api.lease(NS, LEASE).unwrap();
    assert!(later.renew_time > first.renew_time);
    assert_eq!(
        later.acquire_time, first.acquire_time,
        "renew keeps acquire time"
    );
    assert_eq!(later.transitions, 0);
    assert!(a.leadership().is_leader());
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn follower_waits_while_leader_renews() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    assert!(a.leadership().wait_until_leader().await);
    let b = elect(&api, "b");
    let lb = b.leadership();
    for _ in 0..600 {
        advance(Duration::from_millis(100)).await;
        assert!(!lb.is_leader());
    }
    assert_eq!(lb.holder().as_deref(), Some("a"));
    a.stop().await;
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn crash_fails_over_after_lease_duration() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    assert!(a.leadership().wait_until_leader().await);
    let b = elect(&api, "b");
    advance(S(5)).await;
    a.abort();
    let t0 = Instant::now();
    let lb = b.leadership();
    wait_until(S(30), || lb.is_leader()).await;
    let took = t0.elapsed();
    // Last renew: at most 2.4 s before the crash. b sees it within one
    // jittered retry (2.4 s). Takeover: 15 s later, at b's next retry
    // (up to 2.4 s). So 12.6 s to 19.8 s.
    assert!(took >= Duration::from_millis(12_500), "too early: {took:?}");
    assert!(took <= S(20), "too late: {took:?}");
    let rec = api.lease(NS, LEASE).unwrap();
    assert!(rec.held_by("b"));
    assert_eq!(rec.transitions, 1);
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn graceful_release_hands_over_fast() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    assert!(a.leadership().wait_until_leader().await);
    let b = elect(&api, "b");
    advance(S(3)).await;
    let la = a.leadership();
    a.stop().await;
    assert!(!la.is_leader());
    let rec = api.lease(NS, LEASE).unwrap();
    assert!(rec.is_free(), "released: {rec:?}");
    assert_eq!(rec.lease_duration_secs, 1);
    let t0 = Instant::now();
    let lb = b.leadership();
    wait_until(S(5), || lb.is_leader()).await;
    assert!(
        t0.elapsed() <= Duration::from_millis(2_500),
        "{:?}",
        t0.elapsed()
    );
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn release_off_keeps_holder() {
    let api = MemoryKubeApi::new();
    let mut cfg = leader_config();
    cfg.release_on_shutdown = false;
    let a = LeaderElector::new(Arc::new(api.clone()), NS, "a", cfg)
        .unwrap()
        .start();
    assert!(a.leadership().wait_until_leader().await);
    a.stop().await;
    assert!(api.lease(NS, LEASE).unwrap().held_by("a"));
}

#[tokio::test(start_paused = true)]
async fn api_outage_ends_belief_at_renew_deadline_then_recovers() {
    let api = MemoryKubeApi::new();
    let metrics = Arc::new(KubernetesMetrics::new());
    let a = LeaderElector::new(Arc::new(api.clone()), NS, "a", leader_config())
        .unwrap()
        .with_metrics(Arc::clone(&metrics))
        .start();
    let la = a.leadership();
    assert!(la.wait_until_leader().await);
    advance(S(3)).await;
    api.set_down(true);
    advance(Duration::from_millis(7_500)).await;
    assert!(la.is_leader(), "belief lasts up to the renew deadline");
    advance(Duration::from_millis(2_600)).await;
    assert!(!la.is_leader(), "belief ends at the renew deadline");
    assert!(metrics.snapshot().lease_errors >= 3);
    api.set_down(false);
    wait_until(S(5), || la.is_leader()).await;
    assert_eq!(api.lease(NS, LEASE).unwrap().transitions, 0, "same holder");
    let s = metrics.snapshot();
    assert_eq!((s.leader_acquired, s.leader_lost), (2, 1));
    assert_eq!(s.lease.as_deref(), Some(LEASE));
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn hung_api_call_ends_belief_at_renew_deadline() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    let la = a.leadership();
    assert!(la.wait_until_leader().await);
    api.set_latency(S(60));
    let t0 = Instant::now();
    wait_until(S(20), || !la.is_leader()).await;
    assert!(
        t0.elapsed() <= Duration::from_millis(10_100),
        "{:?}",
        t0.elapsed()
    );
    api.set_latency(Duration::ZERO);
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn other_holder_ends_belief_at_once_then_expires() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    let la = a.leadership();
    assert!(la.wait_until_leader().await);
    let mut intruder = api.lease(NS, LEASE).unwrap();
    intruder.holder = Some("intruder".into());
    api.put_lease(NS, LEASE, intruder);
    wait_until(Duration::from_millis(2_500), || !la.is_leader()).await;
    assert_eq!(la.holder().as_deref(), Some("intruder"));
    // The intruder never renews. `a` takes over after 15 s.
    let t0 = Instant::now();
    wait_until(S(20), || la.is_leader()).await;
    assert!(t0.elapsed() >= S(12), "{:?}", t0.elapsed());
    assert_eq!(api.lease(NS, LEASE).unwrap().transitions, 1);
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn record_duration_from_another_holder_is_used() {
    let api = MemoryKubeApi::new();
    api.put_lease(
        NS,
        LEASE,
        LeaseRecord {
            holder: Some("old".into()),
            lease_duration_secs: 30,
            ..LeaseRecord::default()
        },
    );
    let a = elect(&api, "a");
    let la = a.leadership();
    advance(S(25)).await;
    assert!(!la.is_leader(), "30 s lease is still live");
    wait_until(S(10), || la.is_leader()).await;
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn free_lease_is_taken_at_once() {
    let api = MemoryKubeApi::new();
    api.put_lease(
        NS,
        LEASE,
        LeaseRecord {
            holder: None,
            lease_duration_secs: 15,
            transitions: 4,
            ..LeaseRecord::default()
        },
    );
    let a = elect(&api, "a");
    assert!(a.leadership().wait_until_leader().await);
    assert_eq!(api.lease(NS, LEASE).unwrap().transitions, 5);
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn forbidden_never_leads_and_counts_errors() {
    let api = MemoryKubeApi::new();
    api.set_leases_forbidden(true);
    let metrics = Arc::new(KubernetesMetrics::new());
    let a = LeaderElector::new(Arc::new(api.clone()), NS, "a", leader_config())
        .unwrap()
        .with_metrics(Arc::clone(&metrics))
        .start();
    advance(S(10)).await;
    assert!(!a.leadership().is_leader());
    assert!(metrics.snapshot().lease_errors >= 3);
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn three_candidates_never_overlap() {
    let api = MemoryKubeApi::new();
    let mut handles: Vec<Option<autumn_plugin_kubernetes::ElectorHandle>> = ["a", "b", "c"]
        .iter()
        .map(|id| Some(elect(&api, id)))
        .collect();
    let views: Vec<_> = handles
        .iter()
        .map(|h| h.as_ref().unwrap().leadership())
        .collect();
    let mut leaders_seen = std::collections::BTreeSet::new();
    for step in 0..1_500u32 {
        advance(Duration::from_millis(100)).await;
        let leading: Vec<_> = views.iter().filter(|v| v.is_leader()).collect();
        assert!(leading.len() <= 1, "two leaders at step {step}");
        if let Some(l) = leading.first() {
            leaders_seen.insert(l.identity().to_owned());
        }
        // Crash the leader at 20 s and at 60 s.
        if step == 200 || step == 600 {
            let idx = views.iter().position(Leadership::is_leader).unwrap();
            handles[idx].take().unwrap().abort();
        }
    }
    assert_eq!(leaders_seen.len(), 3, "{leaders_seen:?}");
    assert!(views.iter().any(Leadership::is_leader));
    assert_eq!(api.lease(NS, LEASE).unwrap().transitions, 2);
    for h in handles.into_iter().flatten() {
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn wait_and_subscribe() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    let la = a.leadership();
    let mut rx = la.subscribe();
    assert!(la.wait_until_leader().await);
    assert!(rx.borrow_and_update().leading);
    let b = elect(&api, "b");
    let lb = b.leadership();
    lb.wait_until_follower().await;
    a.stop().await;
    assert!(!rx.borrow().leading);
    b.abort();
    assert!(
        !lb.wait_until_leader().await,
        "stopped elector ends the wait"
    );
}

#[test]
fn new_rejects_bad_config() {
    let api: Arc<dyn KubeApi> = Arc::new(MemoryKubeApi::new());
    let mut cfg = leader_config();
    cfg.renew_deadline_secs = 20;
    assert!(matches!(
        LeaderElector::new(Arc::clone(&api), NS, "a", cfg),
        Err(KubeError::Config(_))
    ));
    let mut cfg = leader_config();
    cfg.lease_name = "Bad".into();
    assert!(LeaderElector::new(Arc::clone(&api), NS, "a", cfg).is_err());
    assert!(LeaderElector::new(Arc::clone(&api), NS, "", leader_config()).is_err());
    assert!(LeaderElector::new(Arc::clone(&api), "", "a", leader_config()).is_err());
    let e = LeaderElector::new(api, NS, "a", leader_config()).unwrap();
    assert!(format!("{e:?}").contains(LEASE));
}

// ------------------------------------------------------------ leader tasks

#[derive(Default)]
struct Counts {
    started: AtomicU32,
    stopped: AtomicU32,
}

fn counting_task(counts: &Arc<Counts>) -> LeaderTask {
    let counts = Arc::clone(counts);
    LeaderTask::new("count", move |_state: AppState, cancel| {
        let counts = Arc::clone(&counts);
        async move {
            counts.started.fetch_add(1, Ordering::SeqCst);
            cancel.cancelled().await;
            counts.stopped.fetch_add(1, Ordering::SeqCst);
        }
    })
}

#[tokio::test(start_paused = true)]
async fn task_runs_only_while_leading_once_per_term() {
    let api = MemoryKubeApi::new();
    let b = elect(&api, "b");
    assert!(b.leadership().wait_until_leader().await);
    let a = elect(&api, "a");
    let counts = Arc::new(Counts::default());
    let tasks = LeaderTasks::start(
        a.leadership(),
        vec![counting_task(&counts)],
        AppState::for_test(),
        S(5),
    );
    advance(S(5)).await;
    assert_eq!(
        counts.started.load(Ordering::SeqCst),
        0,
        "follower runs nothing"
    );
    b.stop().await;
    let la = a.leadership();
    wait_until(S(5), || counts.started.load(Ordering::SeqCst) == 1).await;
    assert!(la.is_leader());
    // Lose the lease: the token fires.
    let mut intruder = api.lease(NS, LEASE).unwrap();
    intruder.holder = Some("intruder".into());
    api.put_lease(NS, LEASE, intruder);
    wait_until(S(5), || counts.stopped.load(Ordering::SeqCst) == 1).await;
    // Next term: one more run.
    wait_until(S(20), || counts.started.load(Ordering::SeqCst) == 2).await;
    tasks.stop().await;
    assert_eq!(
        counts.stopped.load(Ordering::SeqCst),
        2,
        "stop cancels the task"
    );
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn stuck_task_is_aborted_after_timeout() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    let started = Arc::new(AtomicU32::new(0));
    let s2 = Arc::clone(&started);
    let stuck = LeaderTask::new("stuck", move |_state: AppState, _cancel| {
        let s2 = Arc::clone(&s2);
        async move {
            s2.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
    });
    assert_eq!(stuck.name(), "stuck");
    assert!(format!("{stuck:?}").contains("stuck"));
    let tasks = LeaderTasks::start(a.leadership(), vec![stuck], AppState::for_test(), S(1));
    wait_until(S(5), || started.load(Ordering::SeqCst) == 1).await;
    let t0 = Instant::now();
    tasks.stop().await;
    assert!(
        t0.elapsed() <= Duration::from_millis(1_100),
        "{:?}",
        t0.elapsed()
    );
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn task_that_returns_waits_for_next_term() {
    let api = MemoryKubeApi::new();
    let a = elect(&api, "a");
    let runs = Arc::new(AtomicU32::new(0));
    let r2 = Arc::clone(&runs);
    let once = LeaderTask::new("once", move |_state: AppState, _cancel| {
        let r2 = Arc::clone(&r2);
        async move {
            r2.fetch_add(1, Ordering::SeqCst);
        }
    });
    let tasks = LeaderTasks::start(a.leadership(), vec![once], AppState::for_test(), S(1));
    advance(S(10)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    tasks.stop().await;
    a.stop().await;
}
