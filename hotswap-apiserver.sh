#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubelite-5k}"
CONTAINER_NAME="${CLUSTER_NAME}-control-plane"
K8S_WORKTREE="${K8S_WORKTREE:-${HOME}/kubernetes}"
APISERVER_BIN="${APISERVER_BIN:-${K8S_WORKTREE}/kube-apiserver}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-/dev/shm/${CLUSTER_NAME}/kubeconfig}"

t0=$(date +%s%3N)
echo "=== [1/3] Building kube-apiserver in ${K8S_WORKTREE} ==="
(
  cd "${K8S_WORKTREE}"
  go build -o "${APISERVER_BIN}" ./cmd/kube-apiserver
)
t1=$(date +%s%3N)
echo "  Build completed in $((t1 - t0)) ms"

echo "=== [2/3] Hot-swapping ${APISERVER_BIN} into ${CONTAINER_NAME} ==="
t2=$(date +%s%3N)
docker cp "${APISERVER_BIN}" "${CONTAINER_NAME}:/usr/local/bin/kube-apiserver-custom.new"
docker exec "${CONTAINER_NAME}" sh -c '
  set -e
  chmod +x /usr/local/bin/kube-apiserver-custom.new
  if [ -f /tmp/kube-apiserver.yaml.bak ]; then
    cp /tmp/kube-apiserver.yaml.bak /tmp/kube-apiserver.yaml.work
  else
    cp /etc/kubernetes/manifests/kube-apiserver.yaml /tmp/kube-apiserver.yaml.bak
    cp /tmp/kube-apiserver.yaml.bak /tmp/kube-apiserver.yaml.work
  fi
  rm -f /etc/kubernetes/manifests/kube-apiserver.yaml
  APISERVER_CID=$(crictl ps --name kube-apiserver -q | head -n1 || true)
  if [ -n "${APISERVER_CID}" ]; then
    crictl stop -t 0 "${APISERVER_CID}" >/dev/null 2>&1 || true
    crictl rm -f "${APISERVER_CID}" >/dev/null 2>&1 || true
  fi
  mv -f /usr/local/bin/kube-apiserver-custom.new /usr/local/bin/kube-apiserver-custom
  if ! grep -q "/usr/local/bin/kube-apiserver-custom" /tmp/kube-apiserver.yaml.work; then
    sed -i "s|- kube-apiserver$|- /usr/local/bin/kube-apiserver-custom|; s|    volumeMounts:$|    volumeMounts:\n    - mountPath: /usr/local/bin/kube-apiserver-custom\n      name: custom-apiserver\n      readOnly: true|; s|  volumes:$|  volumes:\n  - hostPath:\n      path: /usr/local/bin/kube-apiserver-custom\n      type: File\n    name: custom-apiserver|" /tmp/kube-apiserver.yaml.work
  fi
  mv -f /tmp/kube-apiserver.yaml.work /etc/kubernetes/manifests/kube-apiserver.yaml
'

echo "=== [3/3] Waiting for kube-apiserver /readyz ==="
until kubectl --kubeconfig="${KUBECONFIG_PATH}" get --raw=/readyz >/dev/null 2>&1; do
  sleep 0.2
done
t3=$(date +%s%3N)
echo "  Swap + /readyz completed in $((t3 - t2)) ms (total: $((t3 - t0)) ms)"
