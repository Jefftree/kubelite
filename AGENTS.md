# Kubelite Agent Guide

Single-binary Go simulator (`main.go`) that reproduces 5,000-node Kubernetes worker load (`10,000` HTTP/2 TLS connections, `56,204` watch streams, per-node `Bearer` token authentication, multi-stage `PodStatus` patches, and `ServiceAccount` `TokenRequest` calls) against a real `kind` control plane.

For full control-plane binary hot-swap, `pprof` profiling, and `ClusterLoader2` benchmarking recipes, load [`.agents/skills/kubelite-5k-benchmark/SKILL.md`](.agents/skills/kubelite-5k-benchmark/SKILL.md).

## Commands

```bash
# Build and test
CGO_ENABLED=0 go build -o kubelite .
go test -race -v ./...

# Boot 5,000-node kind control plane (1-node default, or 3-node HA with --ha)
./kubelite up --scale=5k
./kubelite up --scale=5k --ha

# Start 5,000 simulated nodes (auto-detects /var/lib/kubelite/kubeconfig and /var/lib/kubelite/tokens/known_tokens.csv)
GOMAXPROCS=48 taskset -c 96-143 ./kubelite --nodes=5000

# Trigger node disconnect / WatchList reconnect storm via :18080 control API
./kubelite disconnect
./kubelite reconnect --watch-mode=watchlist --node-agents=true
./kubelite status
```

## Real 5,000-Node CI Oracle (`2107878895293304832`)

| Metric | Kubelite (5k) | Real 5k GCE CI | Delta |
| :--- | :--- | :--- | :--- |
| `PATCH pods/status` per Pod | 9.37 | 9.39 | -0.2% |
| `GET pods` per Pod | 12.05 | 12.12 | -0.6% |
| `DELETE pods` per Pod | 2.00 | 1.98 | +1.2% |
| `POST + PATCH events` per Pod | 14.88 | 15.22 | -2.2% |
| `POST serviceaccounts/token` per Pod (startup) | 1.00 | 1.15 | 0.0% (startup) |
| `PATCH nodes/status` Rate | 19.77 / s | 19.52 / s | +1.3% |
| Active Watch Streams | 56,204 | 56,958 | -1.3% |
| `kube-apiserver` Live Heap (`inuse_space`) | 9,167 MB | 9,118 MB | +0.5% |
| `kube-apiserver` Cumulative `alloc_space` | 4,006 GB | 3,711 GB | +7.4% |
| `kube-apiserver` P99 CPU | 26.36 cores | 25.91 cores | +1.7% |
| `kube-scheduler` P99 CPU | 10.53 cores | 10.78 cores | -2.3% |
| `etcd-main` P99 CPU | 3.22 cores | 3.25 cores | -0.9% |

## Operational Gotchas

- **Static compilation required (`CGO_ENABLED=0`)**: Custom `kube-apiserver`, `kube-controller-manager`, and `kube-scheduler` binaries copied into `kind` containers must be compiled with `CGO_ENABLED=0`. Dynamically linked binaries fail to start inside static pod containers with `no such file or directory`.
- **Liveness probe threshold (`failureThreshold: 1000`)**: Under 5,000-node GC pressure, default `failureThreshold: 8` kills `kube-apiserver` mid-run (exit code `137`), silently resetting cumulative metrics. Always preserve `failureThreshold: 1000` in `/etc/kubernetes/manifests/*.yaml` and verify `docker exec kubelite-5k-control-plane crictl ps -a` shows `0` restarts after every run.
- **Never query `/debug/pprof/goroutine?debug=2` at 5k scale**: Dumping text stack traces across ~150,000 goroutines stops the world for 90–120 seconds and stalls `ClusterLoader2`. Always use protobuf format (`/debug/pprof/goroutine` without `debug=2`).
- **Stream profiles via `docker exec ... cat` instead of `docker cp`**: `docker cp` from a running `kind` container can fail or truncate files on overlayfs. Write profiles inside the container to `/tmp/*.pprof` and stream them out via `docker exec kubelite-5k-control-plane cat /tmp/profile.pprof > profile.pprof`.
- **Persistent state in `/var/lib/kubelite`**: Never store `kubeconfig` or tokens in `/dev/shm`, which `systemd-logind` (`RemoveIPC=yes`) wipes when an SSH session closes.
