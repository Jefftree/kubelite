#!/usr/bin/env bash
set -euo pipefail

SCALE="${SCALE:-5k}"
if [[ "${SCALE}" == "65k" ]]; then
  CLUSTER_NAME="${CLUSTER_NAME:-kubelite-65k}"
  NUM_NODES="${NUM_NODES:-65000}"
  KIND_CFG_NAME="kind-65k-scale.yaml"
else
  CLUSTER_NAME="${CLUSTER_NAME:-kubelite-5k}"
  NUM_NODES="${NUM_NODES:-5000}"
  KIND_CFG_NAME="kind-5k-scale.yaml"
fi

NODE_PREFIX="${NODE_PREFIX:-kubelite-}"
KIND_IMAGE="${KIND_IMAGE:-kindest/node:v1.36.1}"
DATA_DIR="${DATA_DIR:-${ETCD_DATA_DIR:-/var/lib/kubelite}}"
KUBECONFIG_FALLBACK="${HOME}/.kube/kubelite-config"
CUSTOM_APISERVER_BIN="${CUSTOM_APISERVER_BIN:-}"
HOST_MAX_CPU=$(( $(nproc) - 1 ))
DEFAULT_CP_END=$(( HOST_MAX_CPU < 95 ? HOST_MAX_CPU : 95 ))
CP_CPUS="${CP_CPUS:-0-${DEFAULT_CP_END}}"
CP_GOMAXPROCS="${CP_GOMAXPROCS:-96}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== [1/5] Preparing persistent etcd and token directories (${DATA_DIR}) ==="
docker run --rm --privileged -v "$(dirname "${DATA_DIR}"):/host_var_lib" --entrypoint sh "${KIND_IMAGE}" -c "
  sysctl -w fs.inotify.max_user_instances=8192 fs.inotify.max_user_watches=4194304 net.ipv4.ip_local_port_range='1024 65535' >/dev/null 2>&1 || true
  mkdir -p '/host_var_lib/$(basename "${DATA_DIR}")/etcd-main' '/host_var_lib/$(basename "${DATA_DIR}")/etcd-events' '/host_var_lib/$(basename "${DATA_DIR}")/etcd-leases' '/host_var_lib/$(basename "${DATA_DIR}")/tokens' &&
  rm -rf '/host_var_lib/$(basename "${DATA_DIR}")/etcd-main/'* '/host_var_lib/$(basename "${DATA_DIR}")/etcd-events/'* '/host_var_lib/$(basename "${DATA_DIR}")/etcd-leases/'* '/host_var_lib/$(basename "${DATA_DIR}")/tokens/'* &&
  chmod -R 777 '/host_var_lib/$(basename "${DATA_DIR}")'
"

echo "=== [2/5] Generating ${NUM_NODES} per-node static tokens (system:node:<name> in system:nodes) ==="
awk -v n="${NUM_NODES}" -v prefix="${NODE_PREFIX}" 'BEGIN {
  fmt = (n >= 10000) ? "%s%05d" : "%s%04d"
  for (i = 0; i < n; i++) {
    name = sprintf(fmt, prefix, i)
    printf "token-%s,system:node:%s,uid-%s,\"system:nodes\"\n", name, name, name
  }
}' > "${DATA_DIR}/tokens/known_tokens.csv"
chmod -R a+rX "${DATA_DIR}/tokens"

echo "=== [3/5] Creating 1-node kind control-plane (${CLUSTER_NAME}) with ${SCALE} scale flags ==="
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  echo "Deleting existing kind cluster ${CLUSTER_NAME}..."
  kind delete cluster --name "${CLUSTER_NAME}"
fi

RENDERED_KIND_CFG="${DATA_DIR}/${KIND_CFG_NAME}.rendered"
sed "s|/var/lib/kubelite|${DATA_DIR}|g" "${SCRIPT_DIR}/${KIND_CFG_NAME}" > "${RENDERED_KIND_CFG}"

# Start kind create in background and inject split etcd static pods as soon as kubelet starts
(
  for _ in $(seq 1 90); do
    if docker inspect "${CLUSTER_NAME}-control-plane" >/dev/null 2>&1; then
      ETCD_IMAGE=$(docker exec "${CLUSTER_NAME}-control-plane" sh -c "grep -m1 'image:.*etcd:' /etc/kubernetes/manifests/etcd.yaml 2>/dev/null | awk '{print \$2}'" || true)
      if [[ -n "${ETCD_IMAGE}" ]]; then
        echo "Injecting split etcd-events static pod (${ETCD_IMAGE} on http://127.0.0.1:2383)..."
        docker exec -i "${CLUSTER_NAME}-control-plane" sh -c "cat > /etc/kubernetes/manifests/etcd-events.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  labels:
    component: etcd-events
    tier: control-plane
  name: etcd-events
  namespace: kube-system
spec:
  hostNetwork: true
  priority: 2000001000
  priorityClassName: system-node-critical
  containers:
  - name: etcd-events
    image: ${ETCD_IMAGE}
    imagePullPolicy: IfNotPresent
    command:
    - etcd
    - --name=etcd-events
    - --data-dir=/var/lib/etcd-events
    - --listen-client-urls=http://127.0.0.1:2383
    - --advertise-client-urls=http://127.0.0.1:2383
    - --listen-peer-urls=http://127.0.0.1:2384
    - --initial-advertise-peer-urls=http://127.0.0.1:2384
    - --initial-cluster=etcd-events=http://127.0.0.1:2384
    - --listen-metrics-urls=http://0.0.0.0:2386
    - --quota-backend-bytes=21474836480
    - --snapshot-count=10000
    - --watch-progress-notify-interval=5s
    - --enable-pprof=true
    volumeMounts:
    - mountPath: /var/lib/etcd-events
      name: etcd-events-data
  volumes:
  - hostPath:
      path: /var/lib/etcd-events
      type: DirectoryOrCreate
    name: etcd-events-data
EOF
        if [[ "${SCALE}" == "65k" ]]; then
          echo "Injecting split etcd-leases static pod (${ETCD_IMAGE} on http://127.0.0.1:2385)..."
          docker exec -i "${CLUSTER_NAME}-control-plane" sh -c "cat > /etc/kubernetes/manifests/etcd-leases.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  labels:
    component: etcd-leases
    tier: control-plane
  name: etcd-leases
  namespace: kube-system
spec:
  hostNetwork: true
  priority: 2000001000
  priorityClassName: system-node-critical
  containers:
  - name: etcd-leases
    image: ${ETCD_IMAGE}
    imagePullPolicy: IfNotPresent
    command:
    - etcd
    - --name=etcd-leases
    - --data-dir=/var/lib/etcd-leases
    - --listen-client-urls=http://127.0.0.1:2385
    - --advertise-client-urls=http://127.0.0.1:2385
    - --listen-peer-urls=http://127.0.0.1:2387
    - --initial-advertise-peer-urls=http://127.0.0.1:2387
    - --initial-cluster=etcd-leases=http://127.0.0.1:2387
    - --listen-metrics-urls=http://0.0.0.0:2388
    - --quota-backend-bytes=214748364800
    - --snapshot-count=50000
    - --watch-progress-notify-interval=5s
    - --enable-pprof=true
    volumeMounts:
    - mountPath: /var/lib/etcd-leases
      name: etcd-leases-data
  volumes:
  - hostPath:
      path: /var/lib/etcd-leases
      type: DirectoryOrCreate
    name: etcd-leases-data
EOF
        fi
        break
      fi
    fi
    sleep 1
  done
) &
INJECT_PID=$!

kind create cluster \
  --name "${CLUSTER_NAME}" \
  --image "${KIND_IMAGE}" \
  --config "${RENDERED_KIND_CFG}"
wait "${INJECT_PID}" || true

echo "=== [3.5/5] Pinning control-plane container to CPUs ${CP_CPUS}, GOMAXPROCS=${CP_GOMAXPROCS}, and failureThreshold=1000 ==="
docker update --cpuset-cpus="${CP_CPUS}" "${CLUSTER_NAME}-control-plane" || true
docker exec "${CLUSTER_NAME}-control-plane" sh -c "
ip route replace 10.128.0.0/9 dev eth0 scope link || true
for p in /etc/kubernetes/manifests/kube-apiserver.yaml /etc/kubernetes/manifests/kube-controller-manager.yaml /etc/kubernetes/manifests/kube-scheduler.yaml; do
  sed -i 's/failureThreshold: 8/failureThreshold: 1000/g' \"\$p\"
  if ! grep -q GOMAXPROCS \"\$p\"; then
    sed -i 's/    image: /    env:\n    - name: GOMAXPROCS\n      value: \"${CP_GOMAXPROCS}\"\n    image: /' \"\$p\"
  fi
done
"

echo "=== [4/5] Configuring RBAC & pinning system DaemonSets to control-plane ==="
CTX="kind-${CLUSTER_NAME}"
sleep 2
until kubectl --context "${CTX}" get nodes >/dev/null 2>&1; do
  sleep 1
done

# Extract admin client cert & key inside the control-plane container for localhost scheduler/KCM pprof & metrics scraping
docker exec "${CLUSTER_NAME}-control-plane" sh -c '
  awk "/client-certificate-data:/ {print \$2}" /etc/kubernetes/admin.conf | base64 -d > /tmp/admin.crt
  awk "/client-key-data:/ {print \$2}" /etc/kubernetes/admin.conf | base64 -d > /tmp/admin.key
'

# Pin kindnet and kube-proxy DaemonSets to the real control-plane node so they don't spawn pods on simulated nodes
kubectl --context "${CTX}" -n kube-system patch daemonset kindnet \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},"containers":[{"name":"kindnet-cni","resources":{"requests":{"cpu":"100m","memory":"512Mi"},"limits":{"cpu":"4000m","memory":"32Gi"}}}]}}}}'
if [[ "${SCALE}" == "65k" ]]; then
  kubectl --context "${CTX}" -n kube-system delete daemonset kube-proxy --ignore-not-found
  CP_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER_NAME}-control-plane")
  docker exec "${CLUSTER_NAME}-control-plane" sh -c "
    iptables -t nat -F KUBE-SERVICES 2>/dev/null || true
    iptables -t nat -C PREROUTING -d 10.130.0.1/32 -p tcp --dport 443 -j DNAT --to-destination ${CP_IP}:6443 2>/dev/null || iptables -t nat -I PREROUTING 1 -d 10.130.0.1/32 -p tcp --dport 443 -j DNAT --to-destination ${CP_IP}:6443
    iptables -t nat -C OUTPUT -d 10.130.0.1/32 -p tcp --dport 443 -j DNAT --to-destination ${CP_IP}:6443 2>/dev/null || iptables -t nat -I OUTPUT 1 -d 10.130.0.1/32 -p tcp --dport 443 -j DNAT --to-destination ${CP_IP}:6443
  "
else
  kubectl --context "${CTX}" -n kube-system patch daemonset kube-proxy \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""}}}}}'
fi

# Taint the control-plane node NoSchedule so 100% of ClusterLoader2 workload pods schedule onto simulated nodes
kubectl --context "${CTX}" taint nodes "${CLUSTER_NAME}-control-plane" \
  node-role.kubernetes.io/control-plane:NoSchedule --overwrite

# Bind system:nodes to system:node and system:node-proxier + extra watch/token resources
kubectl --context "${CTX}" create clusterrolebinding kubelite-node \
  --clusterrole=system:node --group=system:nodes --dry-run=client -o yaml | kubectl --context "${CTX}" apply -f -
kubectl --context "${CTX}" create clusterrolebinding kubelite-node-proxier \
  --clusterrole=system:node-proxier --group=system:nodes --dry-run=client -o yaml | kubectl --context "${CTX}" apply -f -

kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubelite-extra-watches
rules:
- apiGroups: [""]
  resources: ["configmaps", "secrets", "services", "nodes", "nodes/status", "events"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: [""]
  resources: ["serviceaccounts/token"]
  verbs: ["create"]
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["csidrivers", "csinodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["node.k8s.io"]
  resources: ["runtimeclasses"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["discovery.k8s.io"]
  resources: ["endpointslices"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["networking.k8s.io"]
  resources: ["servicecidrs"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kubelite-extra-watches
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: kubelite-extra-watches
subjects:
- apiGroup: rbac.authorization.k8s.io
  kind: Group
  name: system:nodes
EOF

echo "=== [5/5] Saving kubeconfig to ${DATA_DIR}/kubeconfig (and fallback ${KUBECONFIG_FALLBACK}) ==="
kind get kubeconfig --name "${CLUSTER_NAME}" > "${DATA_DIR}/kubeconfig"
chmod 644 "${DATA_DIR}/kubeconfig"
mkdir -p "$(dirname "${KUBECONFIG_FALLBACK}")"
cp -f "${DATA_DIR}/kubeconfig" "${KUBECONFIG_FALLBACK}"

if [[ -n "${CUSTOM_APISERVER_BIN}" && -f "${CUSTOM_APISERVER_BIN}" ]]; then
  CLUSTER_NAME="${CLUSTER_NAME}" APISERVER_BIN="${CUSTOM_APISERVER_BIN}" KUBECONFIG_PATH="${DATA_DIR}/kubeconfig" "${SCRIPT_DIR}/hotswap-apiserver.sh"
fi

CP_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER_NAME}-control-plane")
echo "  Context:           ${CTX}"
echo "  Kubeconfig:        ${DATA_DIR}/kubeconfig (fallback: ${KUBECONFIG_FALLBACK})"
echo "  Control Plane IP:  https://${CP_IP}:6443"
echo "  Tokens File:       ${DATA_DIR}/tokens/known_tokens.csv"
echo "  Etcd Main Dir:     ${DATA_DIR}/etcd-main"
echo "  Etcd Events Dir:   ${DATA_DIR}/etcd-events"
