# ADR 0003: Manifest generator

- Status: Accepted
- Date: 2026-09-28

## Decision

- Build objects with `k8s-openapi` types. Output JSON values and YAML.
- YAML with `serde-saphyr`. `kube-client` uses it already. No new YAML crate.
  It is a 0.0.x crate: keep its version the same as the one that
  `kube-client` uses.
- The library sets no k8s-openapi version feature (k8s-openapi rule). The
  app picks one, or turns on this crate's `latest` feature. CI checks the
  library on 1.32 to 1.36.
- `RoleBinding` is JSON: `roleRef.apiGroup` is `String` before 1.36 and
  `Option<String>` in 1.36.
- `AUTUMN_SERVER__HOST=0.0.0.0`: Autumn binds `127.0.0.1` by default. Probes
  from the kubelet need all interfaces.
- `preStop` uses the `sleep` action, not `exec sleep`. Distroless images have
  no shell.
- Grace period: `preStop + prestop_grace + shutdown_timeout + buffer`
  (Autumn cloud-native guide). Saturating. Proven in Verus.
- RBAC: one Lease rule, `create`/`get`/`patch`/`update`, limited to the
  lease name. The elector creates with a PUT (create on update), so RBAC can
  limit `create` by name. A POST create cannot have a name limit. Renew and
  release use a JSON merge patch with a `resourceVersion` check, so labels,
  annotations, and owner references that other tools set stay.
- ConfigMaps: `list`/`watch` with `resourceNames`. The watch uses a
  `metadata.name` field selector, so the names apply.
- Events: `create`/`patch` in `events.k8s.io`.
- When the config needs no API access, the generator makes no Role. The
  token mounts when the plugin is enabled (`api_access`), because in-cluster
  config needs it.
- PDB only with 2 or more replicas. With 1, it blocks node drains.
- Container security: no privilege escalation, non-root, drop all
  capabilities, `RuntimeDefault` seccomp.

## Results

- Good: `kubectl apply --dry-run=server` and server-side apply accept the
  output (`tests/apiserver.rs`).
- Bad: `runAsNonRoot` fails for images that run as root. Change the image,
  or edit the output.
