# ADR 0002: `KubeApi` trait, a fake, and one contract

- Status: Accepted
- Date: 2026-09-28

## Context

Leader election needs failure tests: outages, hung calls, conflicts,
crashes, three candidates. A real cluster is slow for this and has no
paused time. A fake can drift from the real server.

## Decision

- All Kubernetes calls go through the `KubeApi` trait (`src/api/`).
- `KubeClientApi` wraps `kube::Client`. `MemoryKubeApi` is a public fake with
  fault injection (outage, latency, 403, failing writes and events).
- `tests/common/contract.rs` holds one contract. Both implementations must
  pass it. `tests/apiserver.rs` runs it on a real `kube-apiserver`.
- `scripts/envtest.sh` starts etcd and `kube-apiserver` binaries. No
  containers, no kubelet. CI and laptops use the same script.

## Results

- Good: the contract found a bug in the fake. Later, the design changed:
  the elector creates with a PUT and writes with a merge patch (ADR 0003),
  and the contract checks both rules on the fake and the real server.
- Good: a denied watch yields `Forbidden` and the stream stays open, on
  both. The fake yields an error each second while a fault holds.
- Good: a token user `system:serviceaccount:it-app:shop` proves that the
  generated Role is enough and denies other names.
- Bad: no controllers run. Pods from the Deployment do not start. Tests use
  server-side apply and dry run for manifests.
