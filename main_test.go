/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestDisconnectReconnectAndWatchListURL(t *testing.T) {
	var watchListReqs atomic.Int64
	var promListReqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("watch") == "true" {
			if q.Get("sendInitialEvents") == "true" && q.Get("resourceVersionMatch") == "NotOlderThan" {
				watchListReqs.Add(1)
			}
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return
		}
		if r.Header.Get("User-Agent") == prometheusUserAgent {
			promListReqs.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"100"},"items":[]}`))
	}))
	defer srv.Close()

	sim := &Simulator{
		cfg: Config{
			NumNodes:         4,
			FullWatchNodes:   4,
			SharedPodWatch:   true,
			PodCIDRBaseOctet: 64,
			ControlAddr:      "127.0.0.1:0",
		},
		baseURL:     srv.URL,
		nodesByName: make(map[string]*SimNode, 4),
		nodes:       make([]*SimNode, 4),
		podWorkCh:   make(chan *corev1.Pod, 16),
	}
	sim.watchMode.Store("watchlist")
	sim.nodeAgents.Store(true)
	sim.reconnectStagger.Store(int64(10 * time.Millisecond))
	sim.latestRV.Store("100")

	for i := 0; i < 4; i++ {
		tr := &http.Transport{}
		client := &http.Client{Transport: &impersonatingRoundTripper{base: tr, ua: kubeletUserAgent}}
		n := &SimNode{
			Index:         i,
			Name:          formatNodeName("kubelite-", i, 4),
			KubeletHTTP:   client,
			KubeProxyHTTP: client,
			volRefCnt:     make(map[string]int),
			volCancels:    make(map[string]context.CancelFunc),
			podVols:       make(map[types.UID][]string),
		}
		sim.nodes[i] = n
		sim.nodesByName[n.Name] = n
	}

	u := sim.buildWatchRequestURL(srv.URL+"/api/v1/pods?watch=true&fieldSelector=spec.nodeName%3Dkubelite-0000", "", false)
	if !strings.Contains(u, "sendInitialEvents=true") || !strings.Contains(u, "resourceVersionMatch=NotOlderThan") {
		t.Fatalf("expected WatchList query params in %q", u)
	}

	if gotCIDR := sim.nodePodCIDR(64999); gotCIDR != "10.127.121.192/26" {
		t.Fatalf("unexpected 65k PodCIDR for node 64999: %q", gotCIDR)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sctx, scancel := context.WithCancel(ctx)
	sim.sessionCancel = scancel
	for _, n := range sim.nodes {
		sim.startNodeWatches(sctx, n, true, 10*time.Millisecond)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sim.activeWatches.Load() >= 4*17 && watchListReqs.Load() >= 4*14 && promListReqs.Load() >= 4*3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := sim.activeWatches.Load(); got < 4*17 {
		t.Fatalf("expected at least %d active watches before disconnect, got %d (watchListReqs=%d, promListReqs=%d)",
			4*17, got, watchListReqs.Load(), promListReqs.Load())
	}

	sim.Disconnect(ctx, false)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && sim.activeWatches.Load() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sim.activeWatches.Load(); got != 0 {
		t.Fatalf("expected 0 active watches after disconnect, got %d", got)
	}
	if !sim.disconnected.Load() {
		t.Fatalf("expected disconnected=true after Disconnect")
	}

	beforeReconnectWatchLists := watchListReqs.Load()
	sctx2, scancel2 := context.WithCancel(ctx)
	sim.sessionCancel = scancel2
	sim.disconnected.Store(false)
	for _, n := range sim.nodes {
		sim.startNodeWatches(sctx2, n, true, 10*time.Millisecond)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sim.activeWatches.Load() >= 4*14 && watchListReqs.Load()-beforeReconnectWatchLists >= 4*14 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if delta := watchListReqs.Load() - beforeReconnectWatchLists; delta < 4*14 {
		t.Fatalf("expected reconnect to issue at least %d WatchList requests, got %d", 4*14, delta)
	}

	podUID := types.UID("pod-uid-1")
	sim.seenUIDs.Store(podUID, 3)
	unreadyRunningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default", UID: podUID},
		Spec:       corev1.PodSpec{NodeName: "kubelite-0000"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "NodeLost"},
			},
		},
	}
	sim.inspectPod(sim.nodes[0], unreadyRunningPod)
	select {
	case got := <-sim.podWorkCh:
		if got.UID != podUID {
			t.Fatalf("unexpected pod UID %s", got.UID)
		}
	default:
		t.Fatalf("expected Running pod with PodReady=False to be re-queued on reconnect")
	}
}

// Test5kBenchmarkFidelityInvariants locks down the per-pod API call counts,
// watch streams, PodStatus payloads, and PodCIDR addressing that align kubelite
// 1:1 with upstream 5,000-node CI (studies/2026-10-08-5k-fidelity-validation.md).
func Test5kBenchmarkFidelityInvariants(t *testing.T) {
	var (
		mu             sync.Mutex
		podsByName     = make(map[string]*corev1.Pod)
		getPodCalls    atomic.Int64
		patchPodCalls  atomic.Int64
		deletePodCalls atomic.Int64
		tokenCalls     atomic.Int64
		eventCalls     atomic.Int64
		volWatchCalls  atomic.Int64
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("watch") == "true" {
			if strings.Contains(r.URL.Path, "/configmaps") || strings.Contains(r.URL.Path, "/secrets") {
				volWatchCalls.Add(1)
			}
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return
		}

		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/token"):
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(&authenticationv1.TokenRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Status:     authenticationv1.TokenRequestStatus{Token: "fake-jwt"},
			})

		case r.Method == http.MethodPost && strings.HasSuffix(path, "/events"):
			eventCalls.Add(1)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(&corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "ev"}})

		case r.Method == http.MethodGet && strings.Contains(path, "/pods/"):
			getPodCalls.Add(1)
			name := path[strings.LastIndex(path, "/")+1:]
			mu.Lock()
			p := podsByName[name]
			var cp *corev1.Pod
			if p != nil {
				cp = p.DeepCopy()
			}
			mu.Unlock()
			if cp == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(cp)

		case r.Method == http.MethodPatch && strings.HasSuffix(path, "/status"):
			patchPodCalls.Add(1)
			parts := strings.Split(path, "/")
			name := parts[len(parts)-2]
			body, _ := io.ReadAll(r.Body)
			var patchObj struct {
				Status corev1.PodStatus `json:"status"`
			}
			_ = json.Unmarshal(body, &patchObj)
			mu.Lock()
			p := podsByName[name]
			var cp *corev1.Pod
			if p != nil {
				p.Status = patchObj.Status
				cp = p.DeepCopy()
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(cp)

		case r.Method == http.MethodDelete && strings.Contains(path, "/pods/"):
			deletePodCalls.Add(1)
			name := path[strings.LastIndex(path, "/")+1:]
			mu.Lock()
			p := podsByName[name]
			delete(podsByName, name)
			mu.Unlock()
			if p == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(p)

		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	restCfg := &rest.Config{
		Host:          srv.URL,
		ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"},
		RateLimiter:   flowcontrol.NewFakeAlwaysRateLimiter(),
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	sim := &Simulator{
		cfg: Config{
			NumNodes:         5000,
			FullWatchNodes:   0,
			SharedPodWatch:   false,
			PodCIDRBaseOctet: 64,
			PodWorkers:       2,
			MaxInflightMut:   96,
			PodStatusStages:  3,
			PodStartupDelay:  1 * time.Millisecond,
			Jitter:           1 * time.Millisecond,
			SkipPrePatchGet:  false,
			EmitPodEvents:    true,
			RequestPodTokens: true,
			WatchMode:        "rv0",
		},
		baseURL:      srv.URL,
		adminClient:  client,
		nodesByName:  make(map[string]*SimNode, 1),
		nodes:        make([]*SimNode, 1),
		podWorkCh:    make(chan *corev1.Pod, 16),
		podMutateSem: make(chan struct{}, 96),
		eventSem:     make(chan struct{}, 128),
	}
	sim.watchMode.Store("rv0")

	// 1. Verify /24 PodCIDR at 5,000 nodes vs /26 PodCIDR at 65,000 nodes.
	if got := sim.nodePodCIDR(4999); got != "10.83.135.0/24" {
		t.Fatalf("expected 5k /24 PodCIDR 10.83.135.0/24 for node 4999, got %q", got)
	}
	if got := sim.buildWatchRequestURL(srv.URL+"/api/v1/services?watch=true", "", false); !strings.Contains(got, "resourceVersion=0") {
		t.Fatalf("expected default 5k watch mode rv0 to use resourceVersion=0, got %q", got)
	}

	n := &SimNode{
		Index:         0,
		Name:          "kubelite-0000",
		InternalIP:    "10.128.1.2",
		KubeletClient: client,
		KubeletHTTP:   srv.Client(),
		KubeProxyHTTP: srv.Client(),
		volRefCnt:     make(map[string]int),
		volCancels:    make(map[string]context.CancelFunc),
		podVols:       make(map[types.UID][]string),
	}
	sim.nodes[0] = n
	sim.nodesByName[n.Name] = n

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sim.podActuatorWorker(ctx)

	makeRealisticPod := func(name string, uid types.UID) *corev1.Pod {
		req := corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		}
		mounts := []corev1.VolumeMount{
			{Name: "cfg-vol", MountPath: "/etc/cfg", ReadOnly: true},
			{Name: "sec-vol", MountPath: "/etc/sec", ReadOnly: true},
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       "test-ns",
				UID:             uid,
				Generation:      1,
				ResourceVersion: "10",
			},
			Spec: corev1.PodSpec{
				NodeName:           n.Name,
				ServiceAccountName: "default",
				Volumes: []corev1.Volume{
					{Name: "cfg-vol", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "app-cm"}}}},
					{Name: "sec-vol", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "app-sec"}}},
					{Name: "kube-api-access", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
						Sources: []corev1.VolumeProjection{
							{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}}},
						},
					}}},
				},
				InitContainers: []corev1.Container{
					{Name: "init-0", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}, VolumeMounts: mounts},
					{Name: "init-1", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}, VolumeMounts: mounts},
				},
				Containers: []corev1.Container{
					{Name: "app-0", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}, VolumeMounts: mounts},
					{Name: "app-1", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}, VolumeMounts: mounts},
					{Name: "app-2", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}, VolumeMounts: mounts},
					{Name: "app-3", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}, VolumeMounts: mounts},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
				},
			},
		}
	}

	// Create 2 multi-container RealisticPods with opposite UID[0] parities:
	// '0' (ASCII 48, even) and '1' (ASCII 49, odd).
	podEven := makeRealisticPod("pod-even", types.UID("0-even-uid"))
	podOdd := makeRealisticPod("pod-odd", types.UID("1-odd-uid"))
	mu.Lock()
	podsByName[podEven.Name] = podEven.DeepCopy()
	podsByName[podOdd.Name] = podOdd.DeepCopy()
	mu.Unlock()

	sim.inspectPod(n, podEven.DeepCopy())
	sim.inspectPod(n, podOdd.DeepCopy())

	// Wait for both pods to reach PodReady=True (8 + 7 = 15 status patches).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if patchPodCalls.Load() >= 15 && eventCalls.Load() >= 24 && tokenCalls.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := tokenCalls.Load(); got != 2 {
		t.Fatalf("expected 2 TokenRequest calls on startup, got %d", got)
	}
	if got := patchPodCalls.Load(); got != 15 {
		t.Fatalf("expected 15 PodStatus PATCH calls on startup (8 even + 7 odd = 7.5/pod), got %d", got)
	}
	if got := getPodCalls.Load(); got != 17 {
		t.Fatalf("expected 17 GET Pod calls on startup (9 even + 8 odd = 8.5/pod), got %d", got)
	}
	if got := eventCalls.Load(); got != 24 {
		t.Fatalf("expected 24 Event POST calls on startup (12.0/pod), got %d", got)
	}

	// Verify PodStatus payload fidelity (2 InitContainerStatuses, 4 ContainerStatuses, 5 Conditions).
	mu.Lock()
	finalEven := podsByName[podEven.Name].DeepCopy()
	mu.Unlock()
	if len(finalEven.Status.InitContainerStatuses) != 2 || len(finalEven.Status.ContainerStatuses) != 4 || len(finalEven.Status.Conditions) != 5 {
		t.Fatalf("unexpected PodStatus structure: init=%d cont=%d conds=%d",
			len(finalEven.Status.InitContainerStatuses), len(finalEven.Status.ContainerStatuses), len(finalEven.Status.Conditions))
	}
	if !isPodConditionReady(finalEven) {
		t.Fatalf("expected pod-even to have PodReady=True")
	}

	// Verify volume watch refcounting: 3 unique ConfigMap/Secret volume paths shared by 2 pods -> refcount 2 each.
	n.volMu.Lock()
	numWatchedPaths := len(n.volRefCnt)
	for p, cnt := range n.volRefCnt {
		if cnt != 2 {
			n.volMu.Unlock()
			t.Fatalf("expected refcount 2 for volume path %q, got %d", p, cnt)
		}
	}
	n.volMu.Unlock()
	if numWatchedPaths != 3 {
		t.Fatalf("expected 3 unique volume watch paths (app-cm, app-sec, kube-root-ca.crt), got %d", numWatchedPaths)
	}

	// Trigger deletion of both pods (matching CL2 saturation/churn teardown).
	now := metav1.Now()
	mu.Lock()
	podsByName[podEven.Name].DeletionTimestamp = &now
	podsByName[podOdd.Name].DeletionTimestamp = &now
	delEven := podsByName[podEven.Name].DeepCopy()
	delOdd := podsByName[podOdd.Name].DeepCopy()
	mu.Unlock()

	sim.inspectPod(n, delEven)
	sim.inspectPod(n, delOdd)

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if deletePodCalls.Load() >= 2 && eventCalls.Load() >= 26 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Full lifecycle invariant checks across the 2 pods (mean per pod in parentheses):
	// - PATCH pods/status: 15 startup + 4 delete = 19 total (9.5/pod; 9.37 in CL2 where 93.5% of pods delete)
	// - GET pods:          17 startup + 6 delete = 23 total (11.5 direct GET/pod; 12.05 with watch/list in CL2)
	// - POST events:       24 startup + 2 delete = 26 total (13.0 kubelet/pod + 1.88 control-plane/pod = 14.88/pod in CL2)
	// - POST token:        2 total (1.0/pod)
	// - DELETE pods:       2 total (1.0/pod)
	if got := patchPodCalls.Load(); got != 19 {
		t.Fatalf("expected 19 total PodStatus PATCH calls (9.5/pod), got %d", got)
	}
	if got := getPodCalls.Load(); got != 23 {
		t.Fatalf("expected 23 total GET Pod calls (11.5/pod), got %d", got)
	}
	if got := eventCalls.Load(); got != 26 {
		t.Fatalf("expected 26 total Event POST calls (13.0 kubelet/pod), got %d", got)
	}
	if got := deletePodCalls.Load(); got != 2 {
		t.Fatalf("expected 2 total DELETE Pod calls (1.0/pod), got %d", got)
	}

	n.volMu.Lock()
	remainingVolWatches := len(n.volRefCnt)
	n.volMu.Unlock()
	if remainingVolWatches != 0 {
		t.Fatalf("expected 0 remaining volume watches after pod deletion, got %d", remainingVolWatches)
	}
}

func TestLaptopDefaultsAndEmbeddedSubcommands(t *testing.T) {
	if shouldShardLoopbackIPs(true, "darwin", 5000) {
		t.Fatalf("expected darwin to disable 127.x.y.z loopback source-IP sharding")
	}
	if shouldShardLoopbackIPs(true, "linux", 100) {
		t.Fatalf("expected <=1000 nodes on linux to disable 127.x.y.z loopback source-IP sharding")
	}
	if !shouldShardLoopbackIPs(true, "linux", 5000) {
		t.Fatalf("expected >1000 nodes on linux loopback to enable 127.x.y.z source-IP sharding")
	}

	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	t.Setenv("KUBECONFIG", "")
	kubeDir := filepath.Join(tmpHome, ".kube")
	if err := os.MkdirAll(kubeDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	stdKubeconfig := filepath.Join(kubeDir, "config")
	if err := os.WriteFile(stdKubeconfig, []byte("apiVersion: v1\nkind: Config\n"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := resolveKubeconfigPath("/nonexistent/kubelite/kubeconfig"); got != stdKubeconfig {
		t.Fatalf("expected resolveKubeconfigPath to fall back to ~/.kube/config (%q), got %q", stdKubeconfig, got)
	}

	if len(embeddedKind5kYAML) == 0 || len(embeddedKind65kYAML) == 0 || len(embeddedUpScript) == 0 {
		t.Fatalf("expected all //go:embed assets to be non-empty")
	}

	origTokensVarLib := defaultTokensFileVarLib
	t.Cleanup(func() { defaultTokensFileVarLib = origTokensVarLib })
	defaultTokensFileVarLib = filepath.Join(tmpHome, "nonexistent", "known_tokens.csv")
	t.Setenv("KUBELITE_TOKENS_FILE", "")
	if got := resolveTokensFilePath(""); got != "" {
		t.Fatalf("expected empty resolveTokensFilePath when no tokens file exists, got %q", got)
	}
	autoTokensFile := filepath.Join(tmpHome, "known_tokens.csv")
	if err := os.WriteFile(autoTokensFile, []byte("token-kubelite-0000,system:node:kubelite-0000,uid-0,\"system:nodes\"\n"), 0644); err != nil {
		t.Fatalf("WriteFile tokens: %v", err)
	}
	defaultTokensFileVarLib = autoTokensFile
	if got := resolveTokensFilePath(""); got != autoTokensFile {
		t.Fatalf("expected resolveTokensFilePath to auto-detect %q, got %q", autoTokensFile, got)
	}
	if got := resolveTokensFilePath("/explicit/tokens.csv"); got != "/explicit/tokens.csv" {
		t.Fatalf("expected explicit tokens path override, got %q", got)
	}

	upEnvHA, err := buildUpSubcommandEnv([]string{"--ha", "--scale=65k"})
	if err != nil {
		t.Fatalf("buildUpSubcommandEnv(--ha): %v", err)
	}
	upEnvStr := strings.Join(upEnvHA, ",")
	if !strings.Contains(upEnvStr, "HA=true") || !strings.Contains(upEnvStr, "CONTROL_PLANE_NODES=3") || !strings.Contains(upEnvStr, "SCALE=65k") {
		t.Fatalf("unexpected buildUpSubcommandEnv(--ha) output: %v", upEnvHA)
	}
	upEnvCustom, err := buildUpSubcommandEnv([]string{"--control-plane-nodes=5"})
	if err != nil {
		t.Fatalf("buildUpSubcommandEnv(--control-plane-nodes=5): %v", err)
	}
	if !strings.Contains(strings.Join(upEnvCustom, ","), "CONTROL_PLANE_NODES=5") {
		t.Fatalf("expected CONTROL_PLANE_NODES=5 in %v", upEnvCustom)
	}

	ctrlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"action":"` + strings.TrimPrefix(r.URL.Path, "/") + `","query":"` + r.URL.RawQuery + `"}`))
	}))
	defer ctrlSrv.Close()

	addr := strings.TrimPrefix(ctrlSrv.URL, "http://")
	out, err := runControlSubcommand("reconnect", []string{"--control-addr=" + addr, "--watch-mode=watchlist", "--node-agents=true"})
	if err != nil {
		t.Fatalf("runControlSubcommand: %v", err)
	}
	if !strings.Contains(out, `"action":"reconnect"`) || !strings.Contains(out, "watch_mode=watchlist") || !strings.Contains(out, "node_agents=true") {
		t.Fatalf("unexpected control subcommand output: %s", out)
	}
}

func TestUpControlPlaneMultiNodeHARendering(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	for _, sub := range []string{"tokens", "etcd-1/main", "etcd-1/events", "etcd-1/leases", "etcd-2/main", "etcd-2/events", "etcd-2/leases", "etcd-3/main", "etcd-3/events", "etcd-3/leases"} {
		if err := os.MkdirAll(filepath.Join(dataDir, sub), 0755); err != nil {
			t.Fatalf("MkdirAll %s: %v", sub, err)
		}
	}
	logFile := filepath.Join(tmpDir, "calls.log")

	mockDocker := `#!/usr/bin/env bash
echo "docker $*" >> "` + logFile + `"
if [[ "$1" == "inspect" ]]; then
  echo "172.18.0.2"
  exit 0
fi
if [[ "$1" == "exec" ]]; then
  if [[ "$*" == *"grep -m1"* ]]; then
    echo "registry.k8s.io/etcd:3.5.16-0"
  fi
  exit 0
fi
exit 0
`
	mockKind := `#!/usr/bin/env bash
echo "kind $*" >> "` + logFile + `"
if [[ "$1" == "get" && "$2" == "kubeconfig" ]]; then
  echo "apiVersion: v1"
  echo "kind: Config"
fi
exit 0
`
	mockKubectl := `#!/usr/bin/env bash
echo "kubectl $*" >> "` + logFile + `"
exit 0
`
	for name, content := range map[string]string{"docker": mockDocker, "kind": mockKind, "kubectl": mockKubectl} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(content), 0755); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}

	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
	t.Setenv("HOME", tmpDir)
	if err := runUpSubcommand([]string{"--ha", "--nodes=10", "--data-dir=" + dataDir}); err != nil {
		t.Fatalf("runUpSubcommand(--ha): %v", err)
	}

	renderedBytes, err := os.ReadFile(filepath.Join(dataDir, "kind-5k-scale.yaml.rendered"))
	if err != nil {
		t.Fatalf("ReadFile rendered kind config: %v", err)
	}
	rendered := string(renderedBytes)
	if got := strings.Count(rendered, "- role: control-plane"); got != 3 {
		t.Fatalf("expected 3 control-plane nodes in rendered kind config, got %d:\n%s", got, rendered)
	}
	for _, wantMount := range []string{
		dataDir + "/etcd-1/main",
		dataDir + "/etcd-2/main",
		dataDir + "/etcd-3/main",
		dataDir + "/etcd-1/events",
		dataDir + "/etcd-2/events",
		dataDir + "/etcd-3/events",
	} {
		if !strings.Contains(rendered, wantMount) {
			t.Fatalf("expected rendered kind config to contain %q:\n%s", wantMount, rendered)
		}
	}

	callsBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("ReadFile calls.log: %v", err)
	}
	calls := string(callsBytes)
	for _, wantCP := range []string{"kubelite-5k-control-plane", "kubelite-5k-control-plane2", "kubelite-5k-control-plane3"} {
		if !strings.Contains(calls, "taint nodes "+wantCP+" node-role.kubernetes.io/control-plane:NoSchedule") {
			t.Fatalf("expected taint for %s in calls:\n%s", wantCP, calls)
		}
	}
}
