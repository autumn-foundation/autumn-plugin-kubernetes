# autumn-plugin-kubernetes

Kubernetes plugin for [autumn-web](https://autumn-web.app) 0.7.

- **Pod identity** from the Downward API.
- **Leader election** on a `coordination.k8s.io/v1` Lease. Leader tasks run on one replica only.
- **Live ConfigMaps.** Read the latest data with no restart.
- **Kubernetes Events** on the pod: `Started`, `LeaderElected`, `LeaderLost`, `Stopping`.
- **Health indicator** and **Prometheus metrics**.
- **Manifests:** Deployment, Service, PDB, and least-privilege RBAC from your Autumn config.

It uses [`kube`](https://kube.rs) 4 and `k8s-openapi` 0.28.

## Install

```toml
[dependencies]
autumn-plugin-kubernetes = "0.1"
# Your app picks the Kubernetes API version (k8s-openapi rule).
k8s-openapi = { version = "0.28", features = ["v1_32"] }
```

Or turn on the `latest` feature of this crate (apps only, not libraries).
Supported: Kubernetes 1.32 to 1.36.

```rust
use autumn_plugin_kubernetes::{KubernetesPlugin, LeaderTask};

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(routes![/* ... */])
        .plugin(KubernetesPlugin::new().leader_task(LeaderTask::new("sweeper", sweep)))
        .run()
        .await;
}

async fn sweep(state: AppState, cancel: CancellationToken) {
    // Runs on the leader only. Return soon after `cancel` fires.
}
```

## How it works

```mermaid
flowchart LR
  subgraph Pod
    P[KubernetesPlugin] --> PI[PodInfo]
    P --> E[Leader elector]
    P --> W[ConfigMap watchers]
    P --> H[Health + metrics]
    E -->|leading| T[Leader tasks]
  end
  E -->|get / create / replace Lease| API[(Kubernetes API)]
  W -->|watch ConfigMaps| API
  P -->|Events| API
  H -->|GET /version| API
```

```mermaid
stateDiagram-v2
  [*] --> Follower
  Follower --> Leader: lease free, or expired
  Leader --> Leader: renew OK
  Leader --> Follower: renew deadline passed, conflict, or other holder seen
  Leader --> Released: shutdown (holder cleared)
  Released --> [*]
```

Leader election rules (proven in `verus/policy.rs`):

- A follower takes over only when the lease record has not changed for `lease_duration` on its **own** clock. Clock skew between nodes has no effect.
- A leader stops believing it leads `renew_deadline` after its last good write was **sent**. Then its tasks get the cancel signal.
- `renew_deadline < lease_duration`. So, with equal clock rates, two replicas never both believe they lead.
- Writes use `resourceVersion`. A conflict loses the race and ends belief at once.

A paused VM or a large clock-rate drift can break the last rule. Make a leader task safe to run twice when this matters.

## Config

```toml
[kubernetes]
enabled = true          # false: only PodInfo
required = false        # true: stop startup when no cluster answers
namespace = ""          # "": pod namespace, then the client default
events = true           # write Kubernetes Events on the pod

[kubernetes.leader_election]
enabled = false
lease_name = ""         # necessary when enabled
identity = ""           # "": <pod name>-<random suffix>
lease_duration_secs = 15
renew_deadline_secs = 10
retry_period_secs = 2   # rule: renew > 1.2 * retry, lease > renew
release_on_shutdown = true
lead_when_detached = false  # local development only
roles = []              # [] = all; else "combined", "web", "worker"

[kubernetes.config_maps]
watch = []              # ConfigMap names in the namespace
```

Layers, low to high: `autumn.toml`, `[profile.<name>.kubernetes]`, `autumn-<profile>.toml`, env vars.
Env: `AUTUMN_KUBERNETES__LEADER_ELECTION__ENABLED=true`. Lists are comma separated.

`KubernetesPlugin::readiness(true)` puts the health indicator in `/ready`.
The default is `/actuator/health` only, so an API outage does not stop traffic.

## Modes

| Mode | When |
|---|---|
| `in_cluster` | In a pod, with no `KUBECONFIG`. |
| `kubeconfig` | `KUBECONFIG` or `~/.kube/config`. `HTTPS_PROXY` applies (kube has no `NO_PROXY`). |
| `custom` | `with_api` or `with_connector`. |
| `detached` | No cluster found and `required = false`. Leader tasks do not run unless `lead_when_detached = true`. |
| `disabled` | `enabled = false`. |

## Use in handlers

```rust
let pod = PodInfo::from_state(&state);                 // Option<Arc<PodInfo>>
let lead = Leadership::from_state(&state);             // Option<Arc<Leadership>>
let flags = ConfigMapStore::from_state(&state);        // Option<Arc<ConfigMapStore>>
let beta: Option<bool> = flags.and_then(|s| s.json("flags", "beta").ok().flatten());
let client = state.extension::<kube::Client>();        // your own API calls
```

## Health and metrics

Health indicator `kubernetes`: `mode`, `namespace`, `version`, `leader`, `config_maps`.
On failure it shows a short `error` class only. It shows no URLs or tokens.

| Metric | Kind | Labels |
|---|---|---|
| `kubernetes_leader` | gauge | `lease` |
| `kubernetes_leader_acquired_total` | counter | `lease` |
| `kubernetes_leader_lost_total` | counter | `lease` |
| `kubernetes_lease_errors_total` | counter | `lease` |
| `kubernetes_events_published_total` | counter | |
| `kubernetes_events_failed_total` | counter | |
| `kubernetes_api_up` | gauge | |
| `kubernetes_config_map_updates_total` | counter | `config_map` |
| `kubernetes_config_map_errors_total` | counter | `config_map` |

## Manifests

```bash
cargo run --example manifests -- shop ghcr.io/acme/shop:1.0.0 prod > k8s.yaml
kubectl apply -f k8s.yaml
```

Or in code: `ManifestSpec::from_config(name, image, &autumn_config, &kube_config).render_yaml()`.

- Probes from `health.live_path`, `ready_path`, `startup_path`. Readiness fails on the first 503.
- `terminationGracePeriodSeconds = preStop + prestop_grace + shutdown_timeout + buffer` (Autumn formula).
- `preStop` uses the `sleep` action. The image needs no shell.
- `AUTUMN_SERVER__HOST=0.0.0.0`. Autumn binds `127.0.0.1` by default, and probes need all interfaces.
- The Role has only the verbs that the config needs. Lease and ConfigMap rules use `resourceNames`.

## Tests

```rust
use autumn_plugin_kubernetes::api::MemoryKubeApi;
let api = MemoryKubeApi::new();
let plugin = KubernetesPlugin::new().with_api(api.clone());
```

`MemoryKubeApi` passes the same contract tests as a real API server.

## Limits

- `#[scheduled]` tasks do not use the Lease. autumn-web 0.7 has no public scheduler hook. See ADR 0001.
- `binaryData` in a ConfigMap is not read. Secrets are not watched. Mount them as files.

## Development

See `CLAUDE.md`. License: Apache-2.0.
