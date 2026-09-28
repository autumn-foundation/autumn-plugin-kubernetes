# ADR 0004: Detached mode, and health-only by default

- Status: Accepted
- Date: 2026-09-28

## Decision

- With no cluster config and `required = false`, the plugin runs
  **detached**. `PodInfo` still works. Health is `Up` with `mode: detached`.
- In detached mode, leader tasks do not run. `lead_when_detached = true`
  makes the process lead, for local development.
- The health indicator is health-only by default. `readiness(true)` adds it
  to `/ready`. It is a builder setting: autumn reads indicator groups when
  the app builds, before the config loads.

## Reasons

- Local development must work with no cluster.
- If detached replicas led by default, a deploy outside Kubernetes (ECS,
  Nomad) would run a singleton on every replica, with no warning.
- A control plane outage must not remove every pod from its Service.
