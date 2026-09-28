#!/usr/bin/env bash
# Starts etcd and kube-apiserver for tests. No containers.
#
#   scripts/envtest.sh start   # prints: export KUBE_IT_DIR=...
#   scripts/envtest.sh stop
#
# Two kubeconfigs go in $KUBE_IT_DIR:
#   admin.kubeconfig  user in system:masters.
#   app.kubeconfig    user system:serviceaccount:it-app:shop (RBAC only).
set -euo pipefail

K8S_VERSION="${K8S_VERSION:-v1.34.1}"
ETCD_VERSION="${ETCD_VERSION:-v3.5.21}"
BIN_DIR="${ENVTEST_BIN:-$HOME/.cache/autumn-envtest/$K8S_VERSION}"
RUN_DIR="${KUBE_IT_DIR:-$(pwd)/target/envtest}"
PORT="${ENVTEST_PORT:-16443}"
ETCD_PORT="${ENVTEST_ETCD_PORT:-12379}"

fetch() {
  mkdir -p "$BIN_DIR"
  if [ ! -x "$BIN_DIR/kube-apiserver" ]; then
    curl -fsSL -o "$BIN_DIR/kube-apiserver" \
      "https://dl.k8s.io/release/$K8S_VERSION/bin/linux/amd64/kube-apiserver"
    chmod +x "$BIN_DIR/kube-apiserver"
  fi
  if [ ! -x "$BIN_DIR/etcd" ]; then
    local tmp
    tmp="$(mktemp -d)"
    curl -fsSL -o "$tmp/etcd.tgz" \
      "https://storage.googleapis.com/etcd/$ETCD_VERSION/etcd-$ETCD_VERSION-linux-amd64.tar.gz"
    tar xzf "$tmp/etcd.tgz" -C "$tmp"
    cp "$tmp/etcd-$ETCD_VERSION-linux-amd64/etcd" "$BIN_DIR/etcd"
    rm -rf "$tmp"
  fi
}

kubeconfig() { # file user token
  cat > "$1" <<KC
apiVersion: v1
kind: Config
clusters:
- name: envtest
  cluster:
    server: https://127.0.0.1:$PORT
    certificate-authority: $RUN_DIR/certs/apiserver.crt
users:
- name: $2
  user:
    token: $3
contexts:
- name: envtest
  context:
    cluster: envtest
    user: $2
    namespace: default
current-context: envtest
KC
}

start() {
  fetch
  stop >/dev/null 2>&1 || true
  rm -rf "$RUN_DIR"
  mkdir -p "$RUN_DIR/etcd" "$RUN_DIR/certs"
  openssl genrsa -out "$RUN_DIR/sa.key" 2048 2>/dev/null
  local admin_token app_token
  admin_token="admin-$(openssl rand -hex 16)"
  app_token="app-$(openssl rand -hex 16)"
  cat > "$RUN_DIR/tokens.csv" <<TOK
$admin_token,admin,admin,system:masters
$app_token,system:serviceaccount:it-app:shop,shop,"system:serviceaccounts,system:serviceaccounts:it-app,system:authenticated"
TOK
  "$BIN_DIR/etcd" --data-dir "$RUN_DIR/etcd" \
    --listen-client-urls "http://127.0.0.1:$ETCD_PORT" \
    --advertise-client-urls "http://127.0.0.1:$ETCD_PORT" \
    --listen-peer-urls "http://127.0.0.1:$((ETCD_PORT + 1))" \
    > "$RUN_DIR/etcd.log" 2>&1 &
  echo $! > "$RUN_DIR/etcd.pid"
  "$BIN_DIR/kube-apiserver" \
    --etcd-servers "http://127.0.0.1:$ETCD_PORT" \
    --cert-dir "$RUN_DIR/certs" \
    --secure-port "$PORT" --bind-address 127.0.0.1 --advertise-address 127.0.0.1 \
    --service-account-issuer https://kubernetes.default.svc \
    --service-account-key-file "$RUN_DIR/sa.key" \
    --service-account-signing-key-file "$RUN_DIR/sa.key" \
    --service-cluster-ip-range 10.0.0.0/24 \
    --authorization-mode RBAC \
    --token-auth-file "$RUN_DIR/tokens.csv" \
    --disable-admission-plugins ServiceAccount \
    > "$RUN_DIR/apiserver.log" 2>&1 &
  echo $! > "$RUN_DIR/apiserver.pid"
  for _ in $(seq 1 120); do
    if curl -fsk -H "Authorization: Bearer $admin_token" \
      "https://127.0.0.1:$PORT/readyz" >/dev/null 2>&1; then
      kubeconfig "$RUN_DIR/admin.kubeconfig" admin "$admin_token"
      kubeconfig "$RUN_DIR/app.kubeconfig" shop "$app_token"
      echo "export KUBE_IT_DIR=$RUN_DIR"
      return 0
    fi
    sleep 1
  done
  echo "kube-apiserver did not become ready; see $RUN_DIR/apiserver.log" >&2
  return 1
}

stop() {
  for p in apiserver etcd; do
    if [ -f "$RUN_DIR/$p.pid" ]; then
      kill "$(cat "$RUN_DIR/$p.pid")" 2>/dev/null || true
      rm -f "$RUN_DIR/$p.pid"
    fi
  done
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 [start|stop]" >&2; exit 2 ;;
esac
