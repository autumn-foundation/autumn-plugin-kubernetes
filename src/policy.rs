//! Pure rules for leader election and grace periods. Verified core.
//!
//! `verus/policy.rs` holds the same code with specs and proofs. Keep both in
//! step. The property tests below check this copy against the same specs.
//!
//! Time is in milliseconds on the local monotonic clock.

/// Upper limit for the lease duration (one hour).
pub const MAX_LEASE_MS: u64 = 3_600_000;
/// Jitter adds at most `retry / JITTER_DIVISOR` (20%).
pub const JITTER_DIVISOR: u64 = 5;

/// Next step of a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// This replica holds the lease. Write a new renew time.
    Renew,
    /// The lease is free or expired. Write this replica as holder.
    Acquire,
    /// Another replica holds a live lease. Do nothing.
    Wait,
}

/// Returns `true` when the lease timing is safe to run.
///
/// Rules: `retry > 0`, `renew > 1.2 * retry`, `lease > renew`, and
/// `lease <= MAX_LEASE_MS`.
#[must_use]
pub const fn timing_valid(lease_ms: u64, renew_ms: u64, retry_ms: u64) -> bool {
    if lease_ms > MAX_LEASE_MS || retry_ms == 0 || lease_ms <= renew_ms || retry_ms >= renew_ms {
        return false;
    }
    // Here retry < renew < lease <= MAX_LEASE_MS. No overflow.
    renew_ms * 5 > retry_ms * 6
}

/// Adds a jitter of 0 to 20% to `base_ms`. `rand` is any random value.
///
/// `base_ms` must be `MAX_LEASE_MS` or less. Config validation makes sure.
#[must_use]
pub const fn jittered_ms(base_ms: u64, rand: u64) -> u64 {
    let span = base_ms / JITTER_DIVISOR + 1;
    base_ms.saturating_add(rand % span)
}

/// Decides the next step.
///
/// `observed_age_ms` is the local time since this replica saw the record
/// change. Remote timestamps are not used, so clock skew has no effect.
#[must_use]
pub const fn decide(
    holder_is_me: bool,
    holder_empty: bool,
    observed_age_ms: u64,
    duration_ms: u64,
) -> Action {
    if holder_is_me {
        Action::Renew
    } else if holder_empty || observed_age_ms >= duration_ms {
        Action::Acquire
    } else {
        Action::Wait
    }
}

/// The lease duration for expiry. A positive record value wins. Else the
/// local config value.
#[must_use]
pub const fn effective_duration_ms(record_secs: i32, fallback_ms: u64) -> u64 {
    if record_secs > 0 {
        record_secs.unsigned_abs() as u64 * 1000
    } else {
        fallback_ms
    }
}

/// `leaseTransitions` after a write. It grows by one when the holder
/// changes. It stops at `u32::MAX`.
#[must_use]
pub const fn next_transitions(old: u32, holder_is_me: bool) -> u32 {
    if holder_is_me {
        old
    } else {
        old.saturating_add(1)
    }
}

/// Local time when the record was last seen to change.
#[must_use]
pub const fn observed_at(prev_at: Option<u64>, changed: bool, now_ms: u64) -> u64 {
    match prev_at {
        Some(at) if !changed => at,
        _ => now_ms,
    }
}

/// Time since the record was seen to change. Zero if the clock is behind.
#[must_use]
pub const fn observed_age_ms(now_ms: u64, at_ms: u64) -> u64 {
    now_ms.saturating_sub(at_ms)
}

/// Returns `true` while the replica believes it leads.
///
/// `sent_ms` is when the last successful write was sent. Belief ends
/// `renew_ms` after it, or at once when another holder is seen.
#[must_use]
pub const fn leading(now_ms: u64, sent_ms: Option<u64>, renew_ms: u64, holder_is_me: bool) -> bool {
    match sent_ms {
        Some(s) => holder_is_me && (now_ms < s || now_ms - s < renew_ms),
        None => false,
    }
}

/// `terminationGracePeriodSeconds` by the Autumn formula:
/// `preStop hook + prestop_grace + shutdown_timeout + buffer`. Saturating.
#[must_use]
pub const fn grace_period_secs(
    prestop_hook: u64,
    prestop_grace: u64,
    shutdown_timeout: u64,
    buffer: u64,
) -> u64 {
    prestop_hook
        .saturating_add(prestop_grace)
        .saturating_add(shutdown_timeout)
        .saturating_add(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Specs from `verus/policy.rs`, on `i128` (no overflow).

    fn spec_timing_valid(lease: i128, renew: i128, retry: i128) -> bool {
        retry > 0 && renew * 5 > retry * 6 && lease > renew && lease <= i128::from(MAX_LEASE_MS)
    }

    /// Valid `(lease, renew, retry)` triples.
    fn valid_timing() -> impl Strategy<Value = (u64, u64, u64)> {
        (1..=MAX_LEASE_MS / 2)
            .prop_flat_map(|retry| (Just(retry), (retry * 6 / 5 + 1)..MAX_LEASE_MS))
            .prop_flat_map(|(retry, renew)| ((renew + 1)..=MAX_LEASE_MS, Just(renew), Just(retry)))
    }

    fn spec_leading(now: i128, sent: Option<i128>, renew: i128, me: bool) -> bool {
        me && sent.is_some_and(|s| now < s + renew)
    }

    #[test]
    fn verus_constants_match() {
        let spec = include_str!("../verus/policy.rs");
        for line in [
            "pub const MAX_LEASE_MS: u64 = 3_600_000;".to_owned(),
            format!("pub const JITTER_DIVISOR: u64 = {JITTER_DIVISOR};"),
        ] {
            assert!(spec.contains(&line), "verus/policy.rs lacks: {line}");
        }
        assert_eq!(MAX_LEASE_MS, 3_600_000);
    }

    #[test]
    fn default_timing_is_valid() {
        // Defaults: 15 s lease, 10 s renew deadline, 2 s retry.
        assert!(timing_valid(15_000, 10_000, 2_000));
    }

    #[test]
    fn timing_rejects_each_bad_rule() {
        assert!(!timing_valid(15_000, 10_000, 0), "retry 0");
        assert!(!timing_valid(10_000, 10_000, 2_000), "lease == renew");
        assert!(!timing_valid(15_000, 12_000, 10_000), "renew <= 1.2 retry");
        assert!(
            !timing_valid(MAX_LEASE_MS + 1, 10_000, 2_000),
            "lease too long"
        );
        // Regression: Verus found an overflow on a huge retry.
        assert!(!timing_valid(15_000, 10_000, u64::MAX));
    }

    #[test]
    fn decide_table() {
        assert_eq!(decide(true, false, 0, 15_000), Action::Renew);
        assert_eq!(decide(false, true, 0, 15_000), Action::Acquire);
        assert_eq!(decide(false, false, 15_000, 15_000), Action::Acquire);
        assert_eq!(decide(false, false, 14_999, 15_000), Action::Wait);
    }

    #[test]
    fn transitions_grow_only_on_holder_change() {
        assert_eq!(next_transitions(3, true), 3);
        assert_eq!(next_transitions(3, false), 4);
        assert_eq!(next_transitions(u32::MAX, false), u32::MAX);
    }

    #[test]
    fn effective_duration_prefers_record() {
        assert_eq!(effective_duration_ms(20, 15_000), 20_000);
        assert_eq!(effective_duration_ms(0, 15_000), 15_000);
        assert_eq!(effective_duration_ms(-5, 15_000), 15_000);
    }

    #[test]
    fn observation_keeps_time_until_change() {
        assert_eq!(observed_at(None, false, 50), 50);
        assert_eq!(observed_at(Some(10), false, 50), 10);
        assert_eq!(observed_at(Some(10), true, 50), 50);
        assert_eq!(observed_age_ms(50, 10), 40);
        assert_eq!(observed_age_ms(5, 10), 0);
    }

    #[test]
    fn belief_ends_at_deadline() {
        assert!(leading(1_000, Some(0), 10_000, true));
        assert!(leading(9_999, Some(0), 10_000, true));
        assert!(!leading(10_000, Some(0), 10_000, true));
        assert!(!leading(1_000, Some(0), 10_000, false));
        assert!(!leading(1_000, None, 10_000, true));
    }

    #[test]
    fn grace_period_follows_autumn_formula() {
        // Autumn guide: 5 + 5 + 30 + 10 = 50 s.
        assert_eq!(grace_period_secs(5, 5, 30, 10), 50);
        assert_eq!(grace_period_secs(u64::MAX, 1, 0, 0), u64::MAX);
    }

    proptest! {
        #[test]
        fn timing_valid_matches_spec(lease in 0..=MAX_LEASE_MS + 10, renew in 0..=MAX_LEASE_MS + 10, retry in prop_oneof![0..=MAX_LEASE_MS, Just(u64::MAX)]) {
            prop_assert_eq!(
                timing_valid(lease, renew, retry),
                spec_timing_valid(lease.into(), renew.into(), retry.into())
            );
        }

        #[test]
        fn jitter_in_bounds(base in 0..=MAX_LEASE_MS, rand: u64) {
            let r = jittered_ms(base, rand);
            prop_assert!(base <= r && r <= base + base / JITTER_DIVISOR);
        }

        #[test]
        fn valid_timing_fits_jittered_retry((lease, renew, retry) in valid_timing(), rand: u64) {
            prop_assert!(timing_valid(lease, renew, retry));
            prop_assert!(jittered_ms(retry, rand) < renew);
        }

        #[test]
        fn decide_matches_spec(me: bool, empty: bool, age: u64, dur: u64) {
            let a = decide(me, empty, age, dur);
            if me {
                prop_assert_eq!(a, Action::Renew);
            } else if empty || age >= dur {
                prop_assert_eq!(a, Action::Acquire);
            } else {
                prop_assert_eq!(a, Action::Wait);
            }
        }

        #[test]
        fn leading_matches_spec(now: u64, sent: Option<u64>, renew: u64, me: bool) {
            prop_assert_eq!(
                leading(now, sent, renew, me),
                spec_leading(now.into(), sent.map(i128::from), renew.into(), me)
            );
        }

        #[test]
        fn no_overlap_on_shared_time_line(
            sent in 0..1_000_000u64, d1 in 0..100_000u64, d2 in 0..100_000u64,
            now in 0..2_000_000u64, (lease, renew, retry) in valid_timing(),
        ) {
            prop_assert!(timing_valid(lease, renew, retry));
            let observed = sent + d1 + d2;
            let believes = leading(now, Some(sent), renew, true);
            let takes_over = decide(false, false, observed_age_ms(now, observed), lease) == Action::Acquire
                && now >= observed;
            prop_assert!(!(believes && takes_over));
        }

        #[test]
        fn grace_matches_spec(a: u64, b: u64, c: u64, d: u64) {
            let sum = u128::from(a) + u128::from(b) + u128::from(c) + u128::from(d);
            let r = grace_period_secs(a, b, c, d);
            prop_assert_eq!(u128::from(r), sum.min(u128::from(u64::MAX)));
        }
    }
}
