# 5,000-Node Fidelity Validation Against Real GCE CI

## Summary

Simulating 5,000 Kubernetes nodes with a single-stage status controller misses over half of the write-path allocations and two-thirds of the open watch heap on `kube-apiserver`. Real kubelets step every pod through per-init-container and per-container status updates, fetch the pod before patching status, request a bound ServiceAccount token per pod, and open refcounted watches for every mounted ConfigMap and Secret.

Adding those four kubelet behaviors to `kubelite` aligns 5,000-node `kube-apiserver` live heap within 0.5% (9,167 MB vs 9,118 MB) and cumulative allocations within 7.4% (4,006 GB vs 3,711 GB) of upstream `ci-kubernetes-e2e-gce-scale-performance-5000` (Prow build `2107878895293304832`), while cutting cluster bringup time 13.8x (4m 40s vs 64m 28s).

```mermaid
flowchart LR
    V1["v1: 1-Stage Status<br/>4.03 PATCH/pod<br/>2,197 GB allocs (-41%)"] --> V2["v2: Multi-Container + Token<br/>9.37 PATCH/pod<br/>4,006 GB allocs (+7.4%)"]
    V2 --> Real["Real 5,000-Node CI<br/>9.39 PATCH/pod<br/>3,711 GB allocs"]

    classDef warn fill:#fff3e0,stroke:#ef6c00,color:#e65100
    classDef good fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20
    class V1 warn
    class V2,Real good
```

| Metric | Kubelite v1 (3-Stage) | Kubelite v2 (Aligned) | Real 5k CI (`2107878895293304832`) | v2 vs Real Delta |
| :--- | :--- | :--- | :--- | :--- |
| Cluster Bringup Time | 4m 40s | 4m 40s | 64m 28s | 13.8x faster |
| PATCH pods/status per Pod | 4.03 | 9.37 | 9.39 | -0.2% |
| GET pods per Pod | 5.87 | 12.05 | 12.12 | -0.6% |
| apiserver Live Heap (inuse_space) | 8,345 MB | 9,167 MB | 9,118 MB | +0.5% |
| apiserver Cumulative alloc_space | 2,197 GB | 4,006 GB | 3,711 GB | +7.4% |

---

## Ablation

Both runs executed the full upstream `testing/load/config.yaml` suite (`CL2_REALISTIC_POD=true`, `PODS_PER_NODE=12`, 1,300 Services) against an identical 96-vCPU control plane running Kubernetes `v1.38.0-alpha.1`.

- **Multi-container status progression:** Real 30 KB workload pods define 2 init containers and 4 app containers. Stepping each init container (`PodInitializing` to `Completed`) and app container (`ContainerCreating` to `Started` to `Ready`) raised `PATCH pods/status` from 4.03 to 9.37 per pod (matching 9.39 in real CI).
- **Pre-patch GET pods:** Real kubelet `status_manager` fetches the latest Pod object before status patches and terminal deletions, raising `GET pods` from 5.87 to 12.05 per pod (matching 12.12 in real CI).
- **Per-pod TokenRequest:** Issuing `POST /api/v1/namespaces/{ns}/serviceaccounts/{sa}/token` once per pod startup raised token requests from 0.003 to 1.00 per pod (vs 1.15 in real CI).
- **Refcounted volume watches:** Opening per-node `fieldSelector=metadata.name=<name>` watches for every mounted ConfigMap and Secret keeps 56,204 watch streams open at peak saturation (matching 56,958 in real CI, within 1.3%).

---

## Results

### Control Plane Resource Usage and Latencies

| Component / Metric | Kubelite v2 (5,000 Nodes) | Real 5k CI (`2107878895293304832`) | Delta |
| :--- | :--- | :--- | :--- |
| Active Watch Streams | 56,204 | 56,958 | -1.3% |
| Pod Watch Events Dispatched | 16.15M (121.7 GB) | 15.96M (119.9 GB) | +1.2% |
| apiserver Goroutines | 146,052 | 148,169 | -1.4% |
| apiserver P99 CPU | 26.36 cores | 25.91 cores | +1.7% |
| kube-scheduler P99 CPU / RSS | 10.53 cores / 13.18 GB | 10.78 cores / 13.90 GB | -2.3% |
| kube-controller-manager P99 CPU | 3.29 cores | 3.38 cores | -2.7% |
| etcd-main P99 CPU / Max DB Size | 3.22 cores / 5.80 GiB | 3.25 cores / 6.13 GiB | -0.9% |
| Stateless Pod Startup P99 | 1,212.6s | 1,212.8s | -0.02% |
| High-Throughput Pod Startup P99 | 39.60s | 39.58s | +0.05% |

---

## Decision

Enable `--pod-status-stages=3`, pre-patch `GET /pods/{name}`, `--request-pod-tokens=true`, `--node-status-interval=240s`, and refcounted per-pod ConfigMap/Secret watches by default in `kubelite`.
