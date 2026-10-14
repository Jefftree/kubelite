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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
