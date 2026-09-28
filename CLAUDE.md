# CLAUDE.md

Kubernetes plugin for autumn-web 0.7. Style for all docs and comments: ASD-STE100.

## Layout

| Path | Contents |
|---|---|
| `src/policy.rs` | Verified core: lease decision, belief, timing rules, jitter, grace period. Pure. |
| `verus/policy.rs` | Verus spec and proofs for `src/policy.rs`. Keep both in step. |
| `src/api/` | `KubeApi` trait, `KubeClientApi` (kube), `MemoryKubeApi` (fake). |
| `src/config.rs` | `[kubernetes]` config, profile files, `AUTUMN_KUBERNETES__*` env. |
| `src/pod.rs` | `PodInfo` (Downward API). |
| `src/leader.rs` | Elector loop, `Leadership`, `LeaderTask`, `LeaderTasks`. |
| `src/configmap.rs` | `ConfigMapStore` and the watch task. |
| `src/events.rs` | Pod events. Never fails the app. |
| `src/health.rs`, `src/metrics.rs` | Actuator health and Prometheus metrics. |
| `src/manifest.rs` | YAML generator. |
| `src/plugin.rs` | `KubernetesPlugin`, `KubernetesRuntime`. |
| `tests/` | `leader.rs`, `plugin.rs`, `manifest.rs`, `contract.rs` (fake). `apiserver.rs` (real server). `common/contract.rs`: one contract for all `KubeApi`s. |
| `scripts/envtest.sh` | Starts etcd and kube-apiserver. No containers. |
| `docs/` | `plan.md`, `ac-evidence.md`, ADRs. |

## Commands

```bash
git config core.hooksPath .githooks   # pre-commit: fmt, clippy, test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
# Real API server:
scripts/envtest.sh start              # prints export KUBE_IT_DIR=...
KUBE_IT_DIR=$PWD/target/envtest cargo test --test apiserver
scripts/envtest.sh stop
KUBE_IT_DIR=$PWD/target/envtest cargo llvm-cov --all-targets --summary-only
verus verus/policy.rs
K8S_OPENAPI_ENABLED_VERSION=1.32 cargo check --lib   # each of 1.32..1.36
```

## Rules

- Write the test first. See it fail. Then write the code.
- A change to `src/policy.rs` needs the same change in `verus/policy.rs`. Run Verus.
- Only `src/api/` calls Kubernetes. Other code uses `KubeApi`.
- `MemoryKubeApi` must pass `tests/common/contract.rs`. Add a new rule there first, and check it on the real server.
- Do not turn on a k8s-openapi version feature in `[dependencies]`. Only in `[dev-dependencies]`.
- The library must build on k8s-openapi `v1_32` to `v1_36`. If a field type changes, use JSON (see `role_binding`).
- A metrics label is a lease or ConfigMap name from config. Never a URL or pod name.
- Health output has no URLs, tokens, or raw error text. Use `KubeError::class`.
- No `unwrap` or `expect` in `src/` outside tests.
- Time in tests: `#[tokio::test(start_paused = true)]`. The elector uses `tokio::time`.
- Leader belief must end before a takeover is possible. Do not extend belief on an error.
- Tests that clear `proxy_url` do it only for the loopback test server.
