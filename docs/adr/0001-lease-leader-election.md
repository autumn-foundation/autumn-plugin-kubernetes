# ADR 0001: Own Lease elector with a verified core

- Status: Accepted
- Date: 2026-09-28

## Context

Apps need one replica to run singleton work. autumn-web 0.7 has
`#[scheduled]` coordination on Postgres advisory locks. Many apps on
Kubernetes have no Postgres, or do not want a lock connection per task.

## Options

1. **`kube-leader-election` or `kube-lease-manager` crates.** Both work with
   kube 4. Each has one maintainer. We cannot prove their rules. They do not
   use our `KubeApi` fake.
2. **Own elector on `kube::Api<Lease>`** with a pure, verified policy core.
3. **Plug into `#[scheduled]`.** Implement `SchedulerCoordinator`.

## Decision

Option 2.

## Reasons

- The safety rule is small and fits Verus: `lemma_no_overlap` and
  `lemma_valid_timing_is_safe` prove that belief ends before a takeover.
- Expiry uses the local time when the record last changed (like client-go),
  not the remote `renewTime`. Clock skew has no effect.
- The elector uses `KubeApi`. Tests use `MemoryKubeApi` with paused time.
  The same contract runs on a real API server.
- Option 3 is not possible: `SchedulerLease::local` is `pub(crate)` and
  `coordinator_from_config` reads only the config. There is no install hook.
- `kube` and `k8s-openapi` stay the only Kubernetes crates.

## Results

- Good: proven rules; faster failover on shutdown (`release_on_shutdown`).
- Good: a dead elector (panic, abort) reads as "not leading".
- Bad: `#[scheduled]` tasks do not use the Lease. Use `LeaderTask`.
- Bad: the proof assumes equal clock rates. A paused VM can outlive its
  belief. Leader tasks must be safe to run twice when this matters.

## Upstream seam (proposed)

A public `AppBuilder::with_scheduler_coordinator(impl SchedulerCoordinator)`
and a public `SchedulerLease::new(backend, leader_id, release)`. Then the
plugin can back `#[scheduled]` with the Lease.
