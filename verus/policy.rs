//! Verus spec and proof for `src/policy.rs`.
//!
//! Keep this file in step with `src/policy.rs`.
//! Run: `verus verus/policy.rs`.
//!
//! Time is in milliseconds on one local monotonic clock per process.
//! The safety lemma assumes all clocks run at the same rate.

use vstd::prelude::*;

verus! {

/// Upper limit for the lease duration (one hour).
pub const MAX_LEASE_MS: u64 = 3_600_000;
/// Jitter adds at most `retry / JITTER_DIVISOR` (20%).
pub const JITTER_DIVISOR: u64 = 5;

// ------------------------------------------------------------------ timing

/// Spec: the lease timing is safe to run.
///
/// - `retry > 0`.
/// - `renew > 1.2 * retry`, so one jittered retry fits in the deadline.
/// - `lease > renew`, so belief ends before a takeover is possible.
/// - `lease <= MAX_LEASE_MS`, so the arithmetic cannot overflow.
pub open spec fn spec_timing_valid(lease: int, renew: int, retry: int) -> bool {
    &&& retry > 0
    &&& renew * 5 > retry * 6
    &&& lease > renew
    &&& lease <= MAX_LEASE_MS
}

pub fn timing_valid(lease_ms: u64, renew_ms: u64, retry_ms: u64) -> (ok: bool)
    ensures
        ok == spec_timing_valid(lease_ms as int, renew_ms as int, retry_ms as int),
{
    if lease_ms > MAX_LEASE_MS || retry_ms == 0 || lease_ms <= renew_ms || retry_ms >= renew_ms {
        return false;
    }
    // Here retry < renew < lease <= MAX_LEASE_MS. No overflow.
    renew_ms * 5 > retry_ms * 6
}

// ------------------------------------------------------------------ jitter

/// Spec: the jitter bound.
pub open spec fn spec_jitter_max(base: int) -> int {
    base + base / (JITTER_DIVISOR as int)
}

/// Adds a jitter of 0 to 20% to `base`. `rand` is any random value.
pub fn jittered_ms(base_ms: u64, rand: u64) -> (r: u64)
    requires
        base_ms <= MAX_LEASE_MS,
    ensures
        base_ms <= r,
        r <= spec_jitter_max(base_ms as int),
{
    let span = base_ms / JITTER_DIVISOR + 1;
    base_ms + rand % span
}

/// Valid timing: a jittered retry ends before the renew deadline.
pub proof fn lemma_jittered_retry_fits(lease: int, renew: int, retry: int)
    requires
        spec_timing_valid(lease, renew, retry),
    ensures
        spec_jitter_max(retry) < renew,
{
    // 5 * (retry + retry / 5) <= 6 * retry < 5 * renew.
    assert(5 * (retry / 5) <= retry) by (nonlinear_arith)
        requires retry > 0;
    assert(5 * spec_jitter_max(retry) <= 6 * retry);
}

// ---------------------------------------------------------------- decision

/// Next step of a candidate.
pub enum Action {
    /// This replica holds the lease. Write a new renew time.
    Renew,
    /// The lease is free or expired. Write this replica as holder.
    Acquire,
    /// Another replica holds a live lease. Do nothing.
    Wait,
}

/// Spec: a lease held by another replica is expired.
///
/// `age` is the local time since this replica saw the record change.
pub open spec fn spec_expired(age: int, duration: int) -> bool {
    age >= duration
}

pub fn decide(holder_is_me: bool, holder_empty: bool, observed_age_ms: u64, duration_ms: u64) -> (a: Action)
    ensures
        holder_is_me ==> a is Renew,
        !holder_is_me && (holder_empty || spec_expired(observed_age_ms as int, duration_ms as int)) ==> a is Acquire,
        !holder_is_me && !holder_empty && !spec_expired(observed_age_ms as int, duration_ms as int) ==> a is Wait,
{
    if holder_is_me {
        Action::Renew
    } else if holder_empty || observed_age_ms >= duration_ms {
        Action::Acquire
    } else {
        Action::Wait
    }
}

/// The lease duration to use for expiry. The record value wins when it is
/// positive, up to `MAX_LEASE_MS`. Else the local config value. The cap stops
/// a foreign writer from holding the lease for years.
pub fn effective_duration_ms(record_secs: i32, fallback_ms: u64) -> (r: u64)
    ensures
        record_secs > 0 ==> r == spec_min((record_secs as int) * 1000, MAX_LEASE_MS as int),
        record_secs > 0 ==> r <= MAX_LEASE_MS,
        record_secs <= 0 ==> r == fallback_ms,
{
    if record_secs > 0 {
        let ms = (record_secs as u64) * 1000;
        if ms < MAX_LEASE_MS { ms } else { MAX_LEASE_MS }
    } else {
        fallback_ms
    }
}

/// `leaseTransitions` after a write. It grows by one when the holder changes.
pub fn next_transitions(old: u32, holder_is_me: bool) -> (r: u32)
    ensures
        holder_is_me ==> r == old,
        !holder_is_me && old < u32::MAX ==> r == old + 1,
        !holder_is_me && old == u32::MAX ==> r == old,
{
    if holder_is_me {
        old
    } else if old < u32::MAX {
        old + 1
    } else {
        old
    }
}

/// Local time when the record was last seen to change.
pub fn observed_at(prev_at: Option<u64>, changed: bool, now_ms: u64) -> (r: u64)
    ensures
        changed || prev_at is None ==> r == now_ms,
        !changed && prev_at is Some ==> r == prev_at->Some_0,
{
    match prev_at {
        Some(at) if !changed => at,
        _ => now_ms,
    }
}

/// Time since the record was seen to change. Zero if the clock is behind.
pub fn observed_age_ms(now_ms: u64, at_ms: u64) -> (r: u64)
    ensures
        now_ms >= at_ms ==> r == now_ms - at_ms,
        now_ms < at_ms ==> r == 0,
{
    if now_ms >= at_ms { now_ms - at_ms } else { 0 }
}

// ------------------------------------------------------------------ belief

/// Spec: the replica believes it leads.
///
/// `sent` is the local time when the last successful write was sent.
pub open spec fn spec_leading(now: int, sent: Option<int>, renew: int, holder_is_me: bool) -> bool {
    holder_is_me && sent is Some && now < sent->Some_0 + renew
}

pub fn leading(now_ms: u64, sent_ms: Option<u64>, renew_ms: u64, holder_is_me: bool) -> (r: bool)
    ensures
        r == spec_leading(
            now_ms as int,
            match sent_ms { Some(s) => Some(s as int), None => None },
            renew_ms as int,
            holder_is_me,
        ),
{
    match sent_ms {
        Some(s) => holder_is_me && (now_ms < s || now_ms - s < renew_ms),
        None => false,
    }
}

/// Spec: another replica may write itself as holder at time `now`.
///
/// `observed` is when it saw the leader's latest write. A write with an
/// older `resourceVersion` fails with a conflict, so a takeover that
/// succeeds always saw the latest write.
pub open spec fn spec_can_take_over(now: int, observed: int, duration: int) -> bool {
    spec_expired(now - observed, duration)
}

/// Safety: belief and takeover never overlap.
///
/// The leader sent its last good write at `sent`. The API server stored
/// it at `written`. The other replica saw it at `observed`. All times are
/// on one shared time line (equal clock rates).
pub proof fn lemma_no_overlap(now: int, sent: int, written: int, observed: int, renew: int, duration: int)
    requires
        sent <= written,
        written <= observed,
        renew < duration,
    ensures
        !(spec_leading(now, Some(sent), renew, true) && spec_can_take_over(now, observed, duration)),
{
}

/// Valid timing gives `renew < duration`, so `lemma_no_overlap` applies to
/// the lease that this replica writes.
pub proof fn lemma_valid_timing_is_safe(now: int, sent: int, written: int, observed: int, lease: int, renew: int, retry: int)
    requires
        spec_timing_valid(lease, renew, retry),
        sent <= written,
        written <= observed,
    ensures
        !(spec_leading(now, Some(sent), renew, true) && spec_can_take_over(now, observed, lease)),
{
    lemma_no_overlap(now, sent, written, observed, renew, lease);
}

/// A replica that sees another holder stops its belief at once.
pub proof fn lemma_other_holder_ends_belief(now: int, sent: Option<int>, renew: int)
    ensures
        !spec_leading(now, sent, renew, false),
{
}

// ------------------------------------------------------------ grace period

pub open spec fn spec_min(a: int, b: int) -> int {
    if a <= b { a } else { b }
}

/// `terminationGracePeriodSeconds` by the Autumn formula:
/// `preStop hook + prestop_grace + shutdown_timeout + buffer`. Saturating.
pub fn grace_period_secs(prestop_hook: u64, prestop_grace: u64, shutdown_timeout: u64, buffer: u64) -> (r: u64)
    ensures
        r == spec_min(
            prestop_hook as int + prestop_grace as int + shutdown_timeout as int + buffer as int,
            u64::MAX as int,
        ),
        r >= prestop_hook,
        r >= prestop_grace,
        r >= shutdown_timeout,
        r >= buffer,
{
    prestop_hook
        .saturating_add(prestop_grace)
        .saturating_add(shutdown_timeout)
        .saturating_add(buffer)
}

fn main() {
}

} // verus!
