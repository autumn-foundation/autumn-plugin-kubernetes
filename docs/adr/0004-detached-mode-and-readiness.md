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
- Detached replicas do not lead by default. If they did, each replica of a
  deployment outside Kubernetes (ECS, Nomad) would run the singleton task.
- A control plane outage must not remove every pod from its Service.
- The API check result is reused (5 s up, 1 s down) and refreshed in the
  background. Health requests from outside do not load the API server.
- A kubeconfig that exists but does not load is an error, not detached
  mode. Detached mode needs no kubeconfig and no pod.
