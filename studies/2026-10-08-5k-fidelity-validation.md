# 5,000-Node Fidelity Validation Against Real GCE CI

## Summary

Simulating 5,000 Kubernetes nodes with a single-stage status update misses over half of the write-path allocations and two-thirds of the open watch heap on the API server. Real kubelets step every pod through per-init-container and per-container status transitions, fetch the pod before patching status, request a bound service account token per pod, and open per-node watches for every mounted ConfigMap and Secret.

- **Write-path traffic matches within 0.2% to 0.6% per pod:** Stepping through init and app container stages with pre-patch pod reads produces 9.37 status patches and 12.05 pod reads per pod (vs 9.39 and 12.12 in real 5,000-node CI).
- **Apiserver memory and CPU match within 0.5% to 1.7%:** Live apiserver heap reaches 9,167 MB (vs 9,118 MB in real CI) and P99 CPU reaches 26.36 cores (vs 25.91 cores).
- **Cluster bringup drops 13.8x (64m 28s to 4m 40s):** Running 5,000 simulated nodes from a single process reproduces 56,204 active watch streams and 10,000 HTTP/2 TLS connections without booting 5,000 worker VMs.

```mermaid
flowchart LR
    V1["1. Basic 1-Stage Simulator<br/>4.03 Status Patches / Pod<br/>2,197 GB Apiserver Allocs (-41%)"] --> V2["2. Multi-Stage + Volume Watches<br/>9.37 Status Patches / Pod<br/>4,006 GB Apiserver Allocs (+7.4%)"]
    V2 --> Real["3. Real 5,000-Node GCE CI<br/>9.39 Status Patches / Pod<br/>3,711 GB Apiserver Allocs"]

    classDef warn fill:#fff3e0,stroke:#ef6c00,color:#e65100
    classDef good fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20
    class V1 warn
    class V2,Real good
```

| Metric (5,000 Nodes, 30 KB Pods) | Basic Simulator (v1) | Aligned Kubelite (v2) | Real 5k CI (2107878895293304832) |
| :--- | :--- | :--- | :--- |
| Cluster Bringup Time | 4m 40s | 4m 40s (13.8x faster) | 64m 28s |
| Pod Status Patches per Pod | 4.03 | 9.37 (-0.2%) | 9.39 |
| Pod Reads (GET) per Pod | 5.87 | 12.05 (-0.6%) | 12.12 |
| Apiserver Live Heap | 8,345 MB | 9,167 MB (+0.5%) | 9,118 MB |
| Apiserver Cumulative Allocations | 2,197 GB | 4,006 GB (+7.4%) | 3,711 GB |

---

## What Changed

Both runs executed the upstream 5,000-node load suite (60,000+ concurrent 30 KB pods across Deployments, StatefulSets, DaemonSets, and Jobs, plus 1,300 Services) against a 96-vCPU control plane:

1. **Multi-container status progression ([main.go](https://github.com/Jefftree/kubelite/blob/main/main.go)):** Real 30 KB workload pods define 2 init containers and 4 app containers. Stepping each init container and app container through creation, startup, readiness, and termination raised status patches from 4.03 to 9.37 per pod (vs 9.39 in real CI).
2. **Pre-patch pod reads ([status_manager.go](https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/status/status_manager.go)):** Fetching the latest pod object before status patches and terminal deletions raised pod reads from 5.87 to 12.05 per pod (vs 12.12 in real CI).
3. **Per-pod service account token requests ([main.go](https://github.com/Jefftree/kubelite/blob/main/main.go)):** Requesting a bound service account token on every pod startup raised token creations from 0.003 to 1.00 per pod on startup (vs 1.15 in real CI, where the extra 0.15 comes from 48-minute token renewals during the 82-minute GCE run).
4. **Refcounted ConfigMap and Secret watches ([main.go](https://github.com/Jefftree/kubelite/blob/main/main.go)):** Opening per-node watches for every mounted ConfigMap and Secret keeps 56,204 watch streams open at peak saturation (vs 56,958 in real CI, -1.3%).

---

## Results

| Component / Metric | Aligned Kubelite (5,000 Nodes) | Real 5k CI (2107878895293304832) |
| :--- | :--- | :--- |
| Active Watch Streams | 56,204 (-1.3%) | 56,958 |
| Pod Watch Events Dispatched | 16.15M / 121.7 GB (+1.2%) | 15.96M / 119.9 GB |
| Apiserver Goroutines | 146,052 (-1.4%) | 148,169 |
| Apiserver P99 CPU | 26.36 cores (+1.7%) | 25.91 cores |
| Scheduler P99 CPU / Memory | 10.53 cores / 13.18 GB (-2.3%) | 10.78 cores / 13.90 GB |
| Controller-Manager P99 CPU | 3.29 cores (-2.7%) | 3.38 cores |
| Etcd-Main P99 CPU / Max DB Size | 3.22 cores / 5.80 GiB (-0.9%) | 3.25 cores / 6.13 GiB |
| Stateless Pod Startup Latency (P99) | 1,212.6s (-0.02%) | 1,212.8s |
| High-Throughput Pod Startup Latency (P99) | 39.60s (+0.05%) | 39.58s |
