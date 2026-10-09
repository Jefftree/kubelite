# `kubelite`: Single-VM 5,000-Node Kubernetes Scale Simulator

`kubelite` is a single-binary 5,000-node Kubernetes control-plane simulator paired with a 1-node `kind` control plane tuned to reproduce upstream `pull-kubernetes-gce-master-scale-performance-5000` (`5,002` VMs total: 1x `c4-standard-96` control plane + 5,000x `e2-medium` workers) on a single 144-vCPU VM (`c4-standard-144` or `c4-highmem-144`).

## Architecture & CPU Partitioning

* **Control Plane (`Cores 0-95`, `GOMAXPROCS=96`):**
  * Runs inside `kubelite-5k-control-plane` pinned to `96` vCPUs (`--cpuset-cpus="0-95"`) to match `c4-standard-96` 1:1.
  * Split `etcd-main` (`:2379`) and `etcd-events` (`:2383`, plain HTTP via `--etcd-servers-overrides=/events#http://127.0.0.1:2383`) backed by separate persistent `hyperdisk-balanced` directories (`/var/lib/kubelite-5k/etcd-main` and `/var/lib/kubelite-5k/etcd-events`).
  * Empirical `scale-performance-5000` flags: `--max-requests-inflight=640`, `--max-mutating-requests-inflight=0`, `--delete-collection-workers=16`, `--etcd-compaction-interval=2m30s`, and 5k `watch-cache-sizes`.
* **Simulator & Load Generator (`Cores 96-143`, `GOMAXPROCS=48`):**
  * `10,000` independent HTTP/2 TLS connections (`2` per simulated node: `kubelet` + `kube-proxy` transports) authenticated as `system:node:<name>` in `system:nodes` to exercise the `Node` authorizer and `system-nodes` APF priority level.
  * `55,000` per-node watches (`11` per node across `pods`, `nodes`, `services`, `endpointslices`, `configmaps`, `csidrivers`, `csinodes`, `runtimeclasses`, `servicecidrs`) with protobuf encoding plus refcounted per-pod `ConfigMap`/`Secret` watches. Each node's `fieldSelector=spec.nodeName=<name>` `Pods` watch uses `k8s.io/client-go/tools/watch.RetryWatcher` to directly drive pod lifecycle transitions.
  * `500` `PUT /leases/kubelite-XXXX` heartbeats/sec (`10s` interval) via `k8s.io/component-helpers/apimachinery/lease.NewController` and `55.5` `PATCH /nodes/kubelite-XXXX/status` updates/sec (`90s` interval, `4.9 KB` `NodeStatus` with 25 `ContainerImage` entries) via `k8s.io/component-helpers/node/util.PatchNodeStatus`.
  * Multi-stage pod status progression (`--pod-status-stages=3`): `ContainerCreating` -> `StartedNotReady` -> `RunningReady` plus pre-deletion `Terminating` status patch and `Pulled`/`Created`/`Started` events.

## Quickstart

```bash
# 1. Boot the 96-vCPU 1-node kind control plane with split etcd-main & etcd-events
./up-5k-control-plane.sh

# 2. Build and start kubelite on cores 96-143
CGO_ENABLED=0 go build -o kubelite .
GOMAXPROCS=48 taskset -c 96-143 ./kubelite \
  --kubeconfig=/dev/shm/kubelite-5k/kubeconfig \
  --nodes=5000 \
  --pod-status-stages=3 \
  --pod-workers=512 \
  --max-inflight-pod-mutations=96

# 3. Run ClusterLoader2 5,000-node load test on cores 96-143
GOMAXPROCS=48 taskset -c 96-143 clusterloader2 \
  --kubeconfig=/dev/shm/kubelite-5k/kubeconfig \
  --provider=kind \
  --nodes=5000 \
  --testconfig=testing/load/config.yaml \
  --testoverrides=./cl2-5k-overrides.yaml \
  --report-dir=/tmp/cl2-load-report
```
