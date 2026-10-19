---
name: kubelite-5k-benchmark
description: >-
  End-to-end guide for running 5,000-node Kubernetes control-plane scale
  benchmarks with kubelite, hot-swapping custom kube-apiserver /
  kube-controller-manager / kube-scheduler binaries into 1-node or 3-node HA kind
  clusters, capturing CPU/heap pprof profiles, and comparing against the Real 5k
  CI Oracle (2107878895293304832).
---

# Kubelite 5,000-Node Control-Plane Benchmark & Profiling Skill

## 1. Booting the Control Plane and 5,000 Simulated Nodes

```bash
# 1-node kind control plane (default) or 3-node HA control plane (--ha)
./kubelite up --scale=5k
# For 3-node HA etcd + apiserver quorum:
# ./kubelite up --scale=5k --ha

# Launch 5,000 simulated nodes pinned to non-control-plane cores
# Automatically uses /var/lib/kubelite/kubeconfig and /var/lib/kubelite/tokens/known_tokens.csv
GOMAXPROCS=48 taskset -c 96-143 ./kubelite --nodes=5000 > /tmp/kubelite.log 2>&1 &

# Wait for all 5,000 simulated nodes to report Ready
until [[ "$(kubectl --kubeconfig=/var/lib/kubelite/kubeconfig get nodes --no-headers 2>/dev/null | grep -c ' Ready')" -ge 5000 ]]; do
  sleep 2
done
```

## 2. Swapping a Custom `kube-apiserver` (or `kube-scheduler` / `kube-controller-manager`) into Kind

Always build with `CGO_ENABLED=0` so the binary runs inside the minimal static-pod image, and update every control-plane container (`kubelite-5k-control-plane`, plus `kubelite-5k-control-plane2` and `kubelite-5k-control-plane3` when running `--ha`):

```bash
# 1. Build static binary from your kubernetes checkout
(cd /path/to/kubernetes && CGO_ENABLED=0 go build -o /tmp/kube-apiserver ./cmd/kube-apiserver)
file /tmp/kube-apiserver | grep -v "dynamically linked"

# 2. Swap into all control-plane containers in the kind cluster
CLUSTER_NAME="${CLUSTER_NAME:-kubelite-5k}"
for c in $(docker ps --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}" --filter "label=io.x-k8s.kind.role=control-plane" --format '{{.Names}}' | sort); do
  docker cp /tmp/kube-apiserver "${c}:/usr/local/bin/kube-apiserver-custom.new"
  docker exec "${c}" sh -c '
    set -e
    chmod +x /usr/local/bin/kube-apiserver-custom.new
    cp /etc/kubernetes/manifests/kube-apiserver.yaml /tmp/kube-apiserver.yaml.work
    sed -i "s/failureThreshold: 8/failureThreshold: 1000/g" /tmp/kube-apiserver.yaml.work
    rm -f /etc/kubernetes/manifests/kube-apiserver.yaml
    CID=$(crictl ps --name kube-apiserver -q | head -n1 || true)
    [ -n "${CID}" ] && crictl stop -t 0 "${CID}" >/dev/null 2>&1 && crictl rm -f "${CID}" >/dev/null 2>&1 || true
    mv -f /usr/local/bin/kube-apiserver-custom.new /usr/local/bin/kube-apiserver-custom
    if ! grep -q "/usr/local/bin/kube-apiserver-custom" /tmp/kube-apiserver.yaml.work; then
      sed -i "s|- kube-apiserver$|- /usr/local/bin/kube-apiserver-custom|; s|    volumeMounts:$|    volumeMounts:\n    - mountPath: /usr/local/bin/kube-apiserver-custom\n      name: custom-apiserver\n      readOnly: true|; s|  volumes:$|  volumes:\n  - hostPath:\n      path: /usr/local/bin/kube-apiserver-custom\n      type: File\n    name: custom-apiserver|" /tmp/kube-apiserver.yaml.work
    fi
    mv -f /tmp/kube-apiserver.yaml.work /etc/kubernetes/manifests/kube-apiserver.yaml
  '
done

# 3. Wait for /readyz
until kubectl --kubeconfig=/var/lib/kubelite/kubeconfig get --raw=/readyz >/dev/null 2>&1; do
  sleep 0.5
done
```

## 3. Running ClusterLoader2 Workloads

### Fast 10-Minute Pod Saturation & Churn Run (`CL2_LOAD_TEST_THROUGHPUT=200`)

```bash
cat > /tmp/cl2-fast-overrides.yaml << 'EOF'
CL2_REALISTIC_POD: true
CL2_LOAD_TEST_THROUGHPUT: 200
CL2_DELETE_TEST_THROUGHPUT: 200
CL2_RATE_LIMIT_POD_CREATION: false
CL2_ENABLE_PVS: false
CL2_USE_SIMPLE_LATENCY_QUERY: true
EOF

GOMAXPROCS=48 taskset -c 96-143 /path/to/perf-tests/clusterloader2/clusterloader \
  --kubeconfig=/var/lib/kubelite/kubeconfig \
  --provider=local \
  --nodes=5000 \
  --testconfig=/path/to/perf-tests/clusterloader2/testing/load/config.yaml \
  --testoverrides=/tmp/cl2-fast-overrides.yaml \
  --report-dir=/tmp/cl2-report
```

### Full Upstream 5,000-Node CI Fidelity Run (`CL2_LOAD_TEST_THROUGHPUT=50`)

Matches `ci-kubernetes-e2e-gce-scale-performance-5000` (`2107878895293304832`) 1:1:

```bash
cat > /tmp/cl2-ci-overrides.yaml << 'EOF'
CL2_REALISTIC_POD: true
CL2_LOAD_TEST_THROUGHPUT: 50
CL2_DELETE_TEST_THROUGHPUT: 50
CL2_ENABLE_PVS: true
CL2_USE_SIMPLE_LATENCY_QUERY: true
EOF
```

## 4. Capturing `pprof` CPU & Heap Profiles Inside `kind`

`up-5k-control-plane.sh` extracts `/tmp/admin.crt` and `/tmp/admin.key` inside every control-plane container. Always stream profiles out with `docker exec ... cat` rather than `docker cp`:

```bash
CP="kubelite-5k-control-plane"

# kube-apiserver (:6443) 30s CPU profile, live heap (inuse_space), and cumulative allocations (alloc_space)
kubectl --kubeconfig=/var/lib/kubelite/kubeconfig get --raw="/debug/pprof/profile?seconds=30" > /tmp/apiserver-cpu.pprof
kubectl --kubeconfig=/var/lib/kubelite/kubeconfig get --raw="/debug/pprof/heap" > /tmp/apiserver-heap.pprof
go tool pprof -top -cum -sample_index=inuse_space /tmp/apiserver-heap.pprof | head -n 30
go tool pprof -top -cum -sample_index=alloc_space /tmp/apiserver-heap.pprof | head -n 30

# kube-scheduler (:10259) and kube-controller-manager (:10257)
docker exec "${CP}" sh -c 'curl -sk --cert /tmp/admin.crt --key /tmp/admin.key "https://127.0.0.1:10259/debug/pprof/profile?seconds=30" > /tmp/sched-cpu.pprof'
docker exec "${CP}" cat /tmp/sched-cpu.pprof > /tmp/sched-cpu.pprof

docker exec "${CP}" sh -c 'curl -sk --cert /tmp/admin.crt --key /tmp/admin.key "https://127.0.0.1:10257/debug/pprof/profile?seconds=30" > /tmp/kcm-cpu.pprof'
docker exec "${CP}" cat /tmp/kcm-cpu.pprof > /tmp/kcm-cpu.pprof

# etcd-main (:2382 plain HTTP metrics/pprof) and etcd-events (:2386)
docker exec "${CP}" sh -c 'curl -s "http://127.0.0.1:2382/debug/pprof/profile?seconds=30" > /tmp/etcd-cpu.pprof'
docker exec "${CP}" cat /tmp/etcd-cpu.pprof > /tmp/etcd-cpu.pprof

# Verify 0 container restarts across the run
docker exec "${CP}" crictl ps -a
```
