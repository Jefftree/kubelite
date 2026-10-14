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

// Command kubelite simulates Kubernetes worker nodes (kubelet and kube-proxy)
// inside a single process to reproduce 5,000-node to 65,000-node control-plane
// watch fanout, Go runtime scheduler contention, and write-path load without
// provisioning worker VMs:
//   - Two dedicated HTTP/2 TLS connections per full-watch node (authenticating
//     via per-node static tokens as system:node:<name> or impersonating).
//   - 11 baseline watches per full-watch node plus refcounted per-pod
//     ConfigMap/Secret watches (matching kubelet watchBasedManager), draining
//     non-pod watch streams into io.Discard.
//   - Configurable --full-watch-nodes and --shared-pod-watch with sparse Lease
//     heartbeats so 65,000 nodes stay Ready and actuate Pods on a single VM.
//   - Per-init-container and per-container PodStatus PATCH stages preceded by
//     GET /pods/{name}, per-pod ServiceAccount TokenRequest calls, and an HTTP
//     chaos server (/disconnect, /reconnect, /status) for WatchList storms.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	retrywatcher "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/component-helpers/apimachinery/lease"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const (
	kubeletUserAgent        = "kubelet/v1.36.1 (linux/amd64) kubernetes/kubelite"
	kubeProxyUserAgent      = "kube-proxy/v1.36.1 (linux/amd64) kubernetes/kubelite"
	ciliumUserAgent         = "cilium-agent/v1.16.0 (linux/amd64) kubernetes/kubelite"
	netdUserAgent           = "netd/v0.0.0 (linux/amd64) kubernetes/kubelite"
	autoscalingUserAgent    = "autoscaling-metrics-pipeline/v0.0.0 (linux/amd64) kubernetes/kubelite"
	prometheusUserAgent     = "Prometheus/"
	simulatedLabelKey       = "kubelite.k8s.io/simulated"
	taintSimulatedKey       = "kubelite.io/simulated"
	pauseImageID            = "registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097"
	defaultKubeconfigVarLib = "/var/lib/kubelite/kubeconfig"
	sparseTransportCnt      = 256
	watchListSteadyStateMin = 60 * time.Second
)

var defaultNodeImages = func() []corev1.ContainerImage {
	imgs := make([]corev1.ContainerImage, 25)
	imgs[0] = corev1.ContainerImage{Names: []string{"registry.k8s.io/pause:3.9", pauseImageID}, SizeBytes: 321520}
	for i := 1; i < len(imgs); i++ {
		imgs[i] = corev1.ContainerImage{
			Names:     []string{fmt.Sprintf("registry.k8s.io/e2e-test-images/agnhost:2.%d", i), fmt.Sprintf("registry.k8s.io/e2e-test-images/agnhost@sha256:%064x", i+1000)},
			SizeBytes: int64(25000000 + i*1048576),
		}
	}
	return imgs
}()

type Config struct {
	Kubeconfig, APIServerURL, TokensFile, NodePrefix, PVCNamespacePrefix string
	ControlAddr, WatchMode                                               string
	NumNodes, RegisterConcurrency, PodWorkers, MaxInflightMut            int
	PodStatusStages, FullWatchNodes, NodeAccelerators, PodCIDRBaseOctet  int
	LeaseInterval, SparseLeaseInterval                                   time.Duration
	NodeStatusInterval, SparseNodeStatusInterval                         time.Duration
	PodStartupDelay, Jitter, ReconnectStagger                            time.Duration
	TaintSimulated, CleanupOnExit                                        bool
	SharedPodWatch, SkipPrePatchGet, EmitPodEvents, RequestPodTokens     bool
	SimulateNodeAgents                                                   bool
}

type SimNode struct {
	Index                      int
	Name, InternalIP, Zone     string
	UID                        types.UID
	KubeletClient              *kubernetes.Clientset
	KubeletHTTP, KubeProxyHTTP *http.Client
	TransitionTime             metav1.Time
	Capacity                   corev1.ResourceList
	volMu                      sync.Mutex
	volRefCnt                  map[string]int
	volCancels                 map[string]context.CancelFunc
	podVols                    map[types.UID][]string
}

type impersonatingRoundTripper struct {
	base             http.RoundTripper
	bearer, user, ua string
	groups           []string
}

func (rt *impersonatingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	if rt.bearer != "" {
		req2.Header.Set("Authorization", "Bearer "+rt.bearer)
	}
	if rt.user != "" {
		req2.Header.Set("Impersonate-User", rt.user)
		req2.Header["Impersonate-Group"] = rt.groups
	} else {
		req2.Header.Del("Impersonate-User")
		req2.Header.Del("Impersonate-Group")
	}
	if req2.Header.Get("User-Agent") == "" {
		req2.Header.Set("User-Agent", rt.ua)
	}
	return rt.base.RoundTrip(req2)
}

type Simulator struct {
	cfg              Config
	baseURL          string
	isLoopback       bool
	adminClient      *kubernetes.Clientset
	tlsConfig        *tls.Config
	nodeTLSConfig    *tls.Config
	sparseTransports []*http.Transport
	nodesByName      map[string]*SimNode
	nodes            []*SimNode
	podMutateSem     chan struct{}
	eventSem         chan struct{}
	podWorkCh        chan *corev1.Pod
	seenUIDs         sync.Map // types.UID -> int (1=starting, 2=deleting, 3=ready)
	latestRV         atomic.Value

	sessionMu        sync.Mutex
	sessionCancel    context.CancelFunc
	disconnected     atomic.Bool
	watchMode        atomic.Value // string: "watchlist", "rv0", "latest"
	nodeAgents       atomic.Bool
	reconnectStagger atomic.Int64
	activeWatches    atomic.Int64
	disconnectCount  atomic.Int64
	reconnectCount   atomic.Int64
}

//go:embed kind-5k-scale.yaml
var embeddedKind5kYAML []byte

//go:embed kind-65k-scale.yaml
var embeddedKind65kYAML []byte

//go:embed up-5k-control-plane.sh
var embeddedUpScript []byte

//go:embed hotswap-apiserver.sh
var embeddedHotswapScript []byte

func fallbackKubeconfigPaths() []string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return nil
	}
	return []string{
		filepath.Join(home, ".kube", "kubelite-config"),
		filepath.Join(home, ".kube", "config"),
	}
}

func defaultKubeconfigPath() string {
	if env := os.Getenv("KUBECONFIG"); env != "" {
		return env
	}
	if _, err := os.Stat(defaultKubeconfigVarLib); err == nil {
		return defaultKubeconfigVarLib
	}
	for _, fb := range fallbackKubeconfigPaths() {
		if _, err := os.Stat(fb); err == nil {
			return fb
		}
	}
	return defaultKubeconfigVarLib
}

func resolveKubeconfigPath(configured string) string {
	if configured == "" {
		configured = defaultKubeconfigPath()
	}
	if _, err := os.Stat(configured); err == nil {
		return configured
	}
	for _, fb := range fallbackKubeconfigPaths() {
		if fb != configured {
			if _, err := os.Stat(fb); err == nil {
				return fb
			}
		}
	}
	return configured
}

func formatNodeName(prefix string, index, totalNodes int) string {
	if totalNodes >= 10000 {
		return fmt.Sprintf("%s%05d", prefix, index)
	}
	return fmt.Sprintf("%s%04d", prefix, index)
}

func loadNodeTokens(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tokens := make(map[string]string, 65536)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ",", 3)
		if len(parts) >= 2 {
			tokens[strings.TrimSpace(parts[1])] = strings.TrimSpace(parts[0])
		}
	}
	return tokens, sc.Err()
}

func closeClientConnections(c *http.Client) {
	if c == nil || c.Transport == nil {
		return
	}
	if rt, ok := c.Transport.(*impersonatingRoundTripper); ok {
		if tr, ok := rt.base.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func isRetryableErr(err error) bool {
	if apierrors.IsForbidden(err) || apierrors.IsConflict(err) ||
		apierrors.IsInternalError(err) || apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) ||
		apierrors.IsUnauthorized(err) || utilnet.IsProbableEOF(err) ||
		utilnet.IsConnectionReset(err) {
		return true
	}
	if _, shouldRetry := apierrors.SuggestsClientDelay(err); shouldRetry {
		return true
	}
	if netErr, ok := err.(net.Error); ok {
		return netErr.Temporary() || netErr.Timeout()
	}
	return strings.Contains(err.Error(), "http2: client connection lost")
}

func retryWithExponentialBackOff(fn func() error) error {
	backoff := wait.Backoff{
		Duration: 100 * time.Millisecond,
		Factor:   3,
		Jitter:   0,
		Steps:    6,
	}
	return wait.ExponentialBackoff(backoff, func() (bool, error) {
		err := fn()
		if err == nil || apierrors.IsNotFound(err) {
			return true, nil
		}
		if isRetryableErr(err) {
			return false, nil
		}
		return false, err
	})
}

func (s *Simulator) withMutateSem(ctx context.Context, fn func() error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.podMutateSem <- struct{}{}:
		defer func() { <-s.podMutateSem }()
		return fn()
	}
}

func (s *Simulator) sleepAbortOnDelete(ctx context.Context, d time.Duration, uid types.UID) bool {
	if d > 0 {
		time.Sleep(d)
	}
	v, ok := s.seenUIDs.Load(uid)
	return ctx.Err() == nil && (!ok || v.(int) != 2)
}

func (s *Simulator) isFullWatchNodeIndex(idx int) bool {
	return s.cfg.FullWatchNodes <= 0 || idx < s.cfg.FullWatchNodes
}

func (s *Simulator) isFullWatchNode(n *SimNode) bool {
	return s.isFullWatchNodeIndex(n.Index)
}

func (s *Simulator) currentWatchMode() string {
	if v := s.watchMode.Load(); v != nil {
		if m, ok := v.(string); ok && m != "" {
			return m
		}
	}
	return "rv0"
}

func (s *Simulator) fetchLatestRV(ctx context.Context, client *http.Client, watchURL string) string {
	if s.cfg.SharedPodWatch || s.cfg.FullWatchNodes > 0 {
		return s.getLatestRV(ctx)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.Replace(watchURL, "watch=true", "limit=1&resourceVersion=0", 1), nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil || resp == nil {
		return ""
	}
	defer resp.Body.Close()
	var meta struct {
		Metadata metav1.ListMeta `json:"metadata"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&meta)
	return meta.Metadata.ResourceVersion
}

func (s *Simulator) getLatestRV(ctx context.Context) string {
	if v := s.latestRV.Load(); v != nil {
		if rv, ok := v.(string); ok && rv != "" {
			return rv
		}
	}
	if s.adminClient != nil {
		if ns, err := s.adminClient.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{ResourceVersion: "0"}); err == nil && ns != nil && ns.ResourceVersion != "" {
			s.latestRV.Store(ns.ResourceVersion)
			return ns.ResourceVersion
		}
	}
	return "0"
}

func shouldShardLoopbackIPs(isLoopback bool, goos string, numNodes int) bool {
	return isLoopback && goos == "linux" && numNodes > 1000
}

func (s *Simulator) newTransport(connIdx int) *http.Transport {
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	if shouldShardLoopbackIPs(s.isLoopback, runtime.GOOS, s.cfg.NumNodes) {
		b1 := byte(1 + (connIdx/40000)%250)
		b2 := byte(1 + (connIdx/200)%200)
		b3 := byte(1 + (connIdx % 200))
		dialer.LocalAddr = &net.TCPAddr{IP: net.IPv4(127, b1, b2, b3)}
	}
	return &http.Transport{
		DialContext: dialer.DialContext, TLSClientConfig: s.nodeTLSConfig.Clone(), ForceAttemptHTTP2: true,
		MaxIdleConnsPerHost: 2, IdleConnTimeout: 90 * time.Second, ReadBufferSize: 8192, WriteBufferSize: 8192,
	}
}

func (s *Simulator) newNodeHTTPClient(connIdx int, dedicatedTransport bool, user string, groups []string, bearer, ua string) *http.Client {
	var tr *http.Transport
	if dedicatedTransport || len(s.sparseTransports) == 0 {
		tr = s.newTransport(connIdx)
	} else {
		tr = s.sparseTransports[(connIdx/2)%len(s.sparseTransports)]
	}
	return &http.Client{Transport: &impersonatingRoundTripper{base: tr, bearer: bearer, user: user, groups: groups, ua: ua}}
}

func (s *Simulator) ensureNodeWatchRBAC(ctx context.Context) {
	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "kubelite-node-watches"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"csinodes"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps", "secrets"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{"create"}},
		},
	}
	if _, err := s.adminClient.RbacV1().ClusterRoles().Create(ctx, cr, metav1.CreateOptions{}); apierrors.IsAlreadyExists(err) {
		_, _ = s.adminClient.RbacV1().ClusterRoles().Update(ctx, cr, metav1.UpdateOptions{})
	}
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "kubelite-node-watches"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "kubelite-node-watches"},
		Subjects:   []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "Group", Name: "system:nodes"}},
	}
	if _, err := s.adminClient.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{}); apierrors.IsAlreadyExists(err) {
		_, _ = s.adminClient.RbacV1().ClusterRoleBindings().Update(ctx, crb, metav1.UpdateOptions{})
	}
}

func (s *Simulator) nodePodCIDR(nodeIndex int) string {
	base := s.cfg.PodCIDRBaseOctet
	if base <= 0 {
		base = 64
	}
	if s.cfg.NumNodes > 16000 || nodeIndex >= 16000 {
		return fmt.Sprintf("10.%d.%d.%d/26", base+(nodeIndex/1024), (nodeIndex/4)%256, (nodeIndex%4)*64)
	}
	return fmt.Sprintf("10.%d.%d.0/24", base+(nodeIndex/256), nodeIndex%256)
}

func (s *Simulator) nodePodIP(nodeIndex int, uid types.UID) string {
	base := s.cfg.PodCIDRBaseOctet
	if base <= 0 {
		base = 64
	}
	if s.cfg.NumNodes > 16000 || nodeIndex >= 16000 {
		hostOffset := 2
		if len(uid) > 0 {
			hostOffset += int(uid[0]) % 60
		}
		return fmt.Sprintf("10.%d.%d.%d", base+(nodeIndex/1024), (nodeIndex/4)%256, (nodeIndex%4)*64+hostOffset)
	}
	hostOffset := 2
	if len(uid) > 0 {
		hostOffset += int(uid[0]) % 250
	}
	return fmt.Sprintf("10.%d.%d.%d", base+(nodeIndex/256), nodeIndex%256, hostOffset)
}

func (s *Simulator) buildNode(n *SimNode, hb metav1.Time) *corev1.Node {
	cidr := s.nodePodCIDR(n.Index)
	var taints []corev1.Taint
	if s.cfg.TaintSimulated {
		taints = []corev1.Taint{{Key: taintSimulatedKey, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	}
	labels := map[string]string{
		"kubernetes.io/hostname":           n.Name,
		"kubernetes.io/os":                 "linux",
		"kubernetes.io/arch":               "amd64",
		"kubernetes.io/role":               "node",
		"node-role.kubernetes.io/node":     "",
		"topology.kubernetes.io/region":    "us-east1",
		"topology.kubernetes.io/zone":      n.Zone,
		"node.kubernetes.io/instance-type": "e2-medium",
		simulatedLabelKey:                  "true",
		taintSimulatedKey:                  "true",
	}
	if s.cfg.NodeAccelerators > 0 {
		labels["worker-node-pool"] = "true"
		labels["kubelite.k8s.io/nodepool"] = "default-pool"
		labels["kubelite.k8s.io/accelerator"] = "tpu-v5p-slice"
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   n.Name,
			UID:    n.UID,
			Labels: labels,
		},
		Spec: corev1.NodeSpec{PodCIDR: cidr, PodCIDRs: []string{cidr}, ProviderID: fmt.Sprintf("kubelite://%s/%s", n.Zone, n.Name), Taints: taints},
		Status: corev1.NodeStatus{
			Capacity: n.Capacity, Allocatable: n.Capacity, Phase: corev1.NodeRunning,
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: n.InternalIP}, {Type: corev1.NodeHostName, Address: n.Name}},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: hb, LastTransitionTime: n.TransitionTime, Reason: "KubeletReady"},
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: hb, LastTransitionTime: n.TransitionTime, Reason: "KubeletHasSufficientMemory"},
				{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: hb, LastTransitionTime: n.TransitionTime, Reason: "KubeletHasNoDiskPressure"},
				{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: hb, LastTransitionTime: n.TransitionTime, Reason: "KubeletHasSufficientPID"},
				{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse, LastHeartbeatTime: hb, LastTransitionTime: n.TransitionTime, Reason: "RouteCreated"},
			},
			NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.36.1", KubeProxyVersion: "v1.36.1", ContainerRuntimeVersion: "containerd://2.3.0", OperatingSystem: "linux", Architecture: "amd64"},
			Images:   defaultNodeImages,
		},
	}
}

func (s *Simulator) patchNodeStatus(n *SimNode, hb metav1.Time) error {
	newNode := s.buildNode(n, hb)
	baseNode := newNode.DeepCopy()
	baseNode.Status = corev1.NodeStatus{}
	_, _, err := nodeutil.PatchNodeStatus(n.KubeletClient.CoreV1(), types.NodeName(n.Name), baseNode, newNode)
	return err
}

func (s *Simulator) registerSingleNode(ctx context.Context, n *SimNode) error {
	now := metav1.Now()
	n.TransitionTime = now
	var created *corev1.Node
	err := retryWithExponentialBackOff(func() error {
		var cErr error
		created, cErr = s.adminClient.CoreV1().Nodes().Create(ctx, s.buildNode(n, now), metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(cErr) {
			created, cErr = s.adminClient.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
		}
		return cErr
	})
	if err != nil {
		return fmt.Errorf("creating node %s: %w", n.Name, err)
	}
	n.UID = created.UID
	return retryWithExponentialBackOff(func() error {
		return s.patchNodeStatus(n, now)
	})
}

func (s *Simulator) markAllNodesReadyStatus(ctx context.Context, status corev1.ConditionStatus, reason, message string) {
	sem := make(chan struct{}, max(1, s.cfg.RegisterConcurrency))
	var wg sync.WaitGroup
	now := metav1.Now()
	for _, n := range s.nodes {
		wg.Add(1)
		sem <- struct{}{}
		go func(node *SimNode) {
			defer wg.Done()
			defer func() { <-sem }()
			newNode := s.buildNode(node, now)
			newNode.Status.Conditions[0] = corev1.NodeCondition{
				Type:               corev1.NodeReady,
				Status:             status,
				LastHeartbeatTime:  now,
				LastTransitionTime: now,
				Reason:             reason,
				Message:            message,
			}
			baseNode := newNode.DeepCopy()
			baseNode.Status = corev1.NodeStatus{}
			_, _, _ = nodeutil.PatchNodeStatus(s.adminClient.CoreV1(), types.NodeName(node.Name), baseNode, newNode)
		}(n)
	}
	wg.Wait()
}

func (s *Simulator) runNodeLeaseController(ctx context.Context, n *SimNode, isReconnect bool, reconnectWindow time.Duration) {
	interval := s.cfg.LeaseInterval
	groupSize := s.cfg.NumNodes
	idxInGroup := n.Index
	if !s.isFullWatchNode(n) {
		interval = s.cfg.SparseLeaseInterval
		groupSize = max(1, s.cfg.NumNodes-s.cfg.FullWatchNodes)
		idxInGroup = n.Index - s.cfg.FullWatchNodes
	} else if s.cfg.FullWatchNodes > 0 && s.cfg.FullWatchNodes < s.cfg.NumNodes {
		groupSize = s.cfg.FullWatchNodes
	}
	if interval <= 0 {
		return
	}
	initialWindow := interval
	if isReconnect {
		initialWindow = reconnectWindow
	}
	delay := time.Duration(int64(idxInGroup) * int64(initialWindow) / int64(max(1, groupSize)))
	if !sleepWithContext(ctx, delay) {
		return
	}
	leaseDurationSec := max(int32(40), int32(interval.Seconds()*2))
	lease.NewController(clock.RealClock{}, n.KubeletClient, n.Name, leaseDurationSec, nil, interval, n.Name, corev1.NamespaceNodeLease,
		func(l *coordinationv1.Lease) error {
			if len(l.OwnerReferences) == 0 {
				l.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: n.Name, UID: n.UID}}
			}
			return nil
		}).Run(ctx)
}

func (s *Simulator) runNodeStatusLoop(ctx context.Context, n *SimNode, isReconnect bool, reconnectWindow time.Duration) {
	interval := s.cfg.NodeStatusInterval
	groupSize := s.cfg.NumNodes
	idxInGroup := n.Index
	if !s.isFullWatchNode(n) {
		interval = s.cfg.SparseNodeStatusInterval
		groupSize = max(1, s.cfg.NumNodes-s.cfg.FullWatchNodes)
		idxInGroup = n.Index - s.cfg.FullWatchNodes
	} else if s.cfg.FullWatchNodes > 0 && s.cfg.FullWatchNodes < s.cfg.NumNodes {
		groupSize = s.cfg.FullWatchNodes
	}
	if interval <= 0 && !isReconnect {
		return
	}
	initialWindow := interval
	if isReconnect {
		initialWindow = reconnectWindow
	}
	delay := time.Duration(int64(idxInGroup) * int64(initialWindow) / int64(max(1, groupSize)))
	if !sleepWithContext(ctx, delay) {
		return
	}
	if isReconnect {
		n.TransitionTime = metav1.Now()
		_ = retryWithExponentialBackOff(func() error {
			if ctx.Err() != nil {
				return nil
			}
			return s.patchNodeStatus(n, metav1.Now())
		})
		if interval <= 0 || !sleepWithContext(ctx, interval) {
			return
		}
	}
	wait.UntilWithContext(ctx, func(_ context.Context) { _ = s.patchNodeStatus(n, metav1.Now()) }, interval)
}

func extractPodVolumeWatchPaths(pod *corev1.Pod) []string {
	seen := make(map[string]struct{}, len(pod.Spec.Volumes)+2)
	add := func(res, name string) {
		if name != "" {
			seen[fmt.Sprintf("/api/v1/namespaces/%s/%s?watch=true&fieldSelector=metadata.name%%3D%s", pod.Namespace, res, name)] = struct{}{}
		}
	}
	for _, v := range pod.Spec.Volumes {
		if v.ConfigMap != nil {
			add("configmaps", v.ConfigMap.Name)
		}
		if v.Secret != nil {
			add("secrets", v.Secret.SecretName)
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.ConfigMap != nil {
					add("configmaps", src.ConfigMap.Name)
				}
				if src.Secret != nil {
					add("secrets", src.Secret.Name)
				}
			}
		}
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	return paths
}

func (s *Simulator) buildWatchRequestURL(watchURL, rv string, synced bool) string {
	timeoutSec := 7200 + rand.IntN(3600)
	if !synced {
		switch s.currentWatchMode() {
		case "watchlist":
			return fmt.Sprintf("%s&allowWatchBookmarks=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&timeoutSeconds=%d", watchURL, timeoutSec)
		case "rv0":
			return fmt.Sprintf("%s&allowWatchBookmarks=true&resourceVersion=0&timeoutSeconds=%d", watchURL, timeoutSec)
		}
	}
	if rv == "" {
		rv = s.getLatestRV(context.Background())
	}
	return fmt.Sprintf("%s&allowWatchBookmarks=true&resourceVersion=%s&timeoutSeconds=%d", watchURL, rv, timeoutSec)
}

func (s *Simulator) runWatchStream(ctx context.Context, client *http.Client, delay time.Duration, ua, watchURL string) {
	if !sleepWithContext(ctx, delay) {
		return
	}
	var rv string
	synced := false
	for ctx.Err() == nil {
		reqURL := s.buildWatchRequestURL(watchURL, rv, synced)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return
		}
		req.Header.Set("Accept", k8sruntime.ContentTypeProtobuf)
		req.Header.Set("User-Agent", ua)
		streamStart := time.Now()
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			s.activeWatches.Add(1)
			_, copyErr := io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			s.activeWatches.Add(-1)
			// Only transition out of initial-events mode if the stream stayed open
			// past initial sync (or cleanly hit server watch timeout).
			if copyErr == nil || time.Since(streamStart) >= watchListSteadyStateMin {
				synced = true
				if latest := s.fetchLatestRV(ctx, client, watchURL); latest != "" {
					rv = latest
				}
			}
		} else if resp != nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code == http.StatusForbidden || code == http.StatusNotFound {
				return
			}
			if synced {
				if latest := s.fetchLatestRV(ctx, client, watchURL); latest != "" {
					rv = latest
				}
			}
		}
		if !sleepWithContext(ctx, time.Duration(500+rand.IntN(1500))*time.Millisecond) {
			return
		}
	}
}

func (s *Simulator) runPrometheusNamespacePodWatch(ctx context.Context, n *SimNode, delay time.Duration, namespace string) {
	if !sleepWithContext(ctx, delay) {
		return
	}
	fieldSel := url.QueryEscape("spec.nodeName=" + n.Name + ",status.phase=Running")
	listURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?fieldSelector=%s&limit=500&resourceVersion=0", s.baseURL, namespace, fieldSel)
	watchURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?watch=true&fieldSelector=%s", s.baseURL, namespace, fieldSel)
	for ctx.Err() == nil {
		lreq, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
		if err != nil {
			return
		}
		lreq.Header.Set("Accept", k8sruntime.ContentTypeProtobuf)
		lreq.Header.Set("User-Agent", prometheusUserAgent)
		lresp, err := n.KubeletHTTP.Do(lreq)
		if err != nil || lresp == nil {
			if !sleepWithContext(ctx, time.Duration(500+rand.IntN(1500))*time.Millisecond) {
				return
			}
			continue
		}
		_, _ = io.Copy(io.Discard, lresp.Body)
		_ = lresp.Body.Close()
		if lresp.StatusCode != http.StatusOK {
			if !sleepWithContext(ctx, time.Duration(500+rand.IntN(1500))*time.Millisecond) {
				return
			}
			continue
		}
		rv := s.getLatestRV(ctx)
		wreq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s&allowWatchBookmarks=true&resourceVersion=%s&timeoutSeconds=%d", watchURL, rv, 7200+rand.IntN(3600)), nil)
		if err != nil {
			return
		}
		wreq.Header.Set("Accept", k8sruntime.ContentTypeProtobuf)
		wreq.Header.Set("User-Agent", prometheusUserAgent)
		wresp, err := n.KubeletHTTP.Do(wreq)
		if err == nil && wresp.StatusCode == http.StatusOK {
			s.activeWatches.Add(1)
			_, _ = io.Copy(io.Discard, wresp.Body)
			_ = wresp.Body.Close()
			s.activeWatches.Add(-1)
		} else if wresp != nil {
			_ = wresp.Body.Close()
		}
		if !sleepWithContext(ctx, time.Duration(500+rand.IntN(1500))*time.Millisecond) {
			return
		}
	}
}

func (s *Simulator) syncPodVolumeWatches(ctx context.Context, n *SimNode, pod *corev1.Pod, active bool) {
	if !s.isFullWatchNode(n) || s.disconnected.Load() {
		return
	}
	n.volMu.Lock()
	defer n.volMu.Unlock()
	if active {
		if _, exists := n.podVols[pod.UID]; exists {
			return
		}
		paths := extractPodVolumeWatchPaths(pod)
		n.podVols[pod.UID] = paths
		for _, p := range paths {
			n.volRefCnt[p]++
			if n.volRefCnt[p] == 1 {
				wctx, cancel := context.WithCancel(ctx)
				n.volCancels[p] = cancel
				go s.runWatchStream(wctx, n.KubeletHTTP, time.Duration(rand.IntN(100))*time.Millisecond, kubeletUserAgent, s.baseURL+p)
			}
		}
		return
	}
	paths, exists := n.podVols[pod.UID]
	if !exists {
		return
	}
	delete(n.podVols, pod.UID)
	for _, p := range paths {
		n.volRefCnt[p]--
		if n.volRefCnt[p] <= 0 {
			delete(n.volRefCnt, p)
			if cancel := n.volCancels[p]; cancel != nil {
				cancel()
			}
			delete(n.volCancels, p)
		}
	}
}

func trimPodForSimulator(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	pod.ManagedFields = nil
	pod.Annotations = nil
	pod.OwnerReferences = nil
	pod.Spec.Affinity = nil
	pod.Spec.TopologySpreadConstraints = nil
	pod.Spec.Tolerations = nil
	pod.Spec.SecurityContext = nil
	pod.Spec.ImagePullSecrets = nil
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		c.Env = nil
		c.Ports = nil
		c.SecurityContext = nil
		c.Command = nil
		c.Args = nil
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		c.Env = nil
		c.Ports = nil
		c.SecurityContext = nil
		c.Command = nil
		c.Args = nil
	}
}

func isPodConditionReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (s *Simulator) inspectPod(n *SimNode, pod *corev1.Pod) {
	if n == nil || pod.Spec.NodeName == "" || s.disconnected.Load() {
		return
	}
	if pod.DeletionTimestamp != nil {
		if prev, _ := s.seenUIDs.Swap(pod.UID, 2); prev != 2 {
			s.podWorkCh <- pod
		}
		return
	}
	if pod.Status.Phase == corev1.PodPending {
		if _, loaded := s.seenUIDs.LoadOrStore(pod.UID, 1); !loaded {
			s.podWorkCh <- pod
		}
		return
	}
	// When KCM's NodeLifecycleController marks a disconnected node Ready=Unknown,
	// controllerutil.MarkPodsNotReady patches running pods to PodReady=False while
	// keeping Phase=Running. Re-queue those pods once the node reconnects.
	if pod.Status.Phase == corev1.PodRunning && !isPodConditionReady(pod) {
		if v, loaded := s.seenUIDs.Load(pod.UID); !loaded || v.(int) == 3 {
			s.seenUIDs.Store(pod.UID, 1)
			s.podWorkCh <- pod
		}
	}
}

func buildVolumeMountStatuses(mounts []corev1.VolumeMount) []corev1.VolumeMountStatus {
	if len(mounts) == 0 {
		return nil
	}
	disabled := corev1.RecursiveReadOnlyDisabled
	out := make([]corev1.VolumeMountStatus, len(mounts))
	for i, vm := range mounts {
		var rro *corev1.RecursiveReadOnlyMode
		if vm.ReadOnly {
			rro = &disabled
		}
		out[i] = corev1.VolumeMountStatus{
			Name:              vm.Name,
			MountPath:         vm.MountPath,
			ReadOnly:          vm.ReadOnly,
			RecursiveReadOnly: rro,
		}
	}
	return out
}

func (s *Simulator) requestServiceAccountTokenAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) {
	if !s.cfg.RequestPodTokens {
		return
	}
	saName := pod.Spec.ServiceAccountName
	if saName == "" {
		saName = "default"
	}
	ns, name, uid := pod.Namespace, pod.Name, pod.UID
	go func() {
		select {
		case <-ctx.Done():
			return
		case s.eventSem <- struct{}{}:
			defer func() { <-s.eventSem }()
			tr := &authenticationv1.TokenRequest{
				Spec: authenticationv1.TokenRequestSpec{
					ExpirationSeconds: ptr.To(int64(3607)),
					BoundObjectRef:    &authenticationv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: name, UID: uid},
				},
			}
			_, _ = n.KubeletClient.CoreV1().ServiceAccounts(ns).CreateToken(ctx, saName, tr, metav1.CreateOptions{})
		}
	}()
}

func (s *Simulator) emitPodEvent(ctx context.Context, n *SimNode, pod *corev1.Pod, reason, msg string) {
	if !s.cfg.EmitPodEvents {
		return
	}
	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta:          metav1.ObjectMeta{Name: fmt.Sprintf("%s.%s.%x.%x", pod.Name, strings.ToLower(reason), now.UnixNano(), rand.Uint32()), Namespace: pod.Namespace},
		InvolvedObject:      corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID, APIVersion: "v1", ResourceVersion: pod.ResourceVersion},
		Reason:              reason,
		Message:             msg,
		Source:              corev1.EventSource{Component: "kubelet", Host: n.Name},
		FirstTimestamp:      now,
		LastTimestamp:       now,
		Count:               1,
		Type:                corev1.EventTypeNormal,
		ReportingController: "kubelet",
		ReportingInstance:   n.Name,
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case s.eventSem <- struct{}{}:
			defer func() { <-s.eventSem }()
			_, _ = n.KubeletClient.CoreV1().Events(ev.Namespace).Create(ctx, ev, metav1.CreateOptions{})
		}
	}()
}

func (s *Simulator) patchPodStatusStage(ctx context.Context, n *SimNode, pod *corev1.Pod, phase corev1.PodPhase, initRunningIdx, initDoneCount, readyCount int, started bool, waitingReason string, terminated, allowDeleting bool) error {
	attempt := 0
	skipFirstGet := s.cfg.SkipPrePatchGet || s.cfg.PodStatusStages == 1
	return retryWithExponentialBackOff(func() error {
		return s.withMutateSem(ctx, func() error {
			attempt++
			if !skipFirstGet || attempt > 1 {
				if latest, err := n.KubeletClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); err == nil && latest != nil {
					pod = latest
				} else if apierrors.IsNotFound(err) {
					return nil
				}
			}
			if !allowDeleting && pod.DeletionTimestamp != nil {
				return nil
			}
			now := metav1.Now()
			schedTime := now
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodScheduled && !c.LastTransitionTime.IsZero() {
					schedTime = c.LastTransitionTime
					break
				}
			}
			podIP := s.nodePodIP(n.Index, pod.UID)
			uidHex := strings.ReplaceAll(string(pod.UID), "-", "")
			containerUser := &corev1.ContainerUser{
				Linux: &corev1.LinuxContainerUser{UID: 65532, GID: 65532, SupplementalGroups: []int64{65532}},
			}

			var initStatuses []corev1.ContainerStatus
			if len(pod.Spec.InitContainers) > 0 {
				initStatuses = make([]corev1.ContainerStatus, len(pod.Spec.InitContainers))
				for i, ic := range pod.Spec.InitContainers {
					cid := fmt.Sprintf("containerd://%s%032x", uidHex, i+100)
					icRes := ic.Resources
					isDone := i < initDoneCount
					isRunning := i == initRunningIdx
					st := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
					if isDone {
						st = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0, Reason: "Completed", StartedAt: schedTime, FinishedAt: now, ContainerID: cid,
						}}
					} else if isRunning {
						st = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}
					}
					initStatuses[i] = corev1.ContainerStatus{
						Name: ic.Name, Image: ic.Image, ImageID: pauseImageID,
						ContainerID:        cid,
						Ready:              isDone,
						Started:            &isRunning,
						State:              st,
						Resources:          &icRes,
						AllocatedResources: ic.Resources.Requests,
						User:               containerUser,
						VolumeMounts:       buildVolumeMountStatuses(ic.VolumeMounts),
					}
				}
			}

			cStatuses := make([]corev1.ContainerStatus, len(pod.Spec.Containers))
			for i, c := range pod.Spec.Containers {
				cid := fmt.Sprintf("containerd://%s%032x", uidHex, i)
				cRes := c.Resources
				cReady := i < readyCount && !terminated && waitingReason == ""
				cStarted := started && !terminated && waitingReason == ""
				cs := corev1.ContainerStatus{
					Name: c.Name, Image: c.Image, ImageID: pauseImageID,
					ContainerID:        cid,
					Ready:              cReady,
					Started:            &cStarted,
					State:              corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
					Resources:          &cRes,
					AllocatedResources: c.Resources.Requests,
					User:               containerUser,
					VolumeMounts:       buildVolumeMountStatuses(c.VolumeMounts),
				}
				if waitingReason != "" {
					cs.ImageID, cs.ContainerID = "", ""
					cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: waitingReason}}
				} else if terminated {
					cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0, Reason: "Completed", StartedAt: schedTime, FinishedAt: now, ContainerID: cid,
					}}
				}
				cStatuses[i] = cs
			}

			allReady := readyCount >= len(pod.Spec.Containers) && len(pod.Spec.Containers) > 0 && !terminated && waitingReason == ""
			readyCond, readyReason := corev1.ConditionFalse, "ContainersNotReady"
			if allReady {
				readyCond, readyReason = corev1.ConditionTrue, ""
			} else if phase == corev1.PodSucceeded {
				readyReason = "PodCompleted"
			}
			initCond, initReason := corev1.ConditionTrue, ""
			if initDoneCount < len(pod.Spec.InitContainers) {
				initCond, initReason = corev1.ConditionFalse, "ContainersNotInitialized"
			}
			readyToStart := corev1.ConditionTrue
			if terminated {
				readyToStart = corev1.ConditionFalse
			}
			gen := pod.Generation
			if gen == 0 {
				gen = 1
			}

			raw, _ := json.Marshal(map[string]any{"status": corev1.PodStatus{
				Phase: phase, HostIP: n.InternalIP, HostIPs: []corev1.HostIP{{IP: n.InternalIP}},
				PodIP: podIP, PodIPs: []corev1.PodIP{{IP: podIP}}, StartTime: &schedTime,
				ObservedGeneration: gen,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReadyToStartContainers, Status: readyToStart, LastTransitionTime: now, ObservedGeneration: gen},
					{Type: corev1.PodInitialized, Status: initCond, Reason: initReason, LastTransitionTime: schedTime, ObservedGeneration: gen},
					{Type: corev1.PodReady, Status: readyCond, Reason: readyReason, LastTransitionTime: now, ObservedGeneration: gen},
					{Type: corev1.ContainersReady, Status: readyCond, Reason: readyReason, LastTransitionTime: now, ObservedGeneration: gen},
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: schedTime, ObservedGeneration: gen},
				},
				InitContainerStatuses: initStatuses,
				ContainerStatuses:     cStatuses,
			}})

			_, err := n.KubeletClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, raw, metav1.PatchOptions{}, "status")
			return err
		})
	})
}

func (s *Simulator) podActuatorWorker(ctx context.Context) {
	for {
	nextPod:
		select {
		case <-ctx.Done():
			return
		case pod := <-s.podWorkCh:
			if s.disconnected.Load() {
				s.seenUIDs.Delete(pod.UID)
				continue
			}
			n := s.nodesByName[pod.Spec.NodeName]
			if n == nil {
				continue
			}
			nInit := len(pod.Spec.InitContainers)
			nCont := len(pod.Spec.Containers)
			if pod.DeletionTimestamp != nil {
				s.syncPodVolumeWatches(ctx, n, pod, false)
				if s.cfg.PodStatusStages >= 3 {
					_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodRunning, -1, nInit, 0, true, "", false, true)
					if nCont > 1 && len(pod.UID) > 0 && pod.UID[0]%2 == 0 {
						s.emitPodEvent(ctx, n, pod, "Killing", "Stopping container "+pod.Spec.Containers[0].Name)
					}
				}
				_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodRunning, -1, nInit, 0, false, "", true, true)
				if err := s.withMutateSem(ctx, func() error {
					if !s.cfg.SkipPrePatchGet {
						_, _ = n.KubeletClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
					}
					return retryWithExponentialBackOff(func() error {
						return n.KubeletClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)), Preconditions: &metav1.Preconditions{UID: &pod.UID},
						})
					})
				}); err != nil {
					go func(p *corev1.Pod) {
						time.Sleep(time.Second)
						select {
						case s.podWorkCh <- p:
						default:
							s.seenUIDs.Delete(p.UID)
						}
					}(pod)
				}
				continue
			}

			s.syncPodVolumeWatches(ctx, n, pod, true)
			// Fast-path recovery for already-Running pods whose PodReady condition
			// was flipped to False by KCM during a node disconnect.
			if pod.Status.Phase == corev1.PodRunning {
				if err := s.patchPodStatusStage(ctx, n, pod, corev1.PodRunning, -1, nInit, nCont, true, "", false, false); err != nil {
					s.seenUIDs.Delete(pod.UID)
					continue
				}
				s.seenUIDs.Store(pod.UID, 3)
				continue
			}

			s.requestServiceAccountTokenAsNode(ctx, n, pod)
			if !s.cfg.SkipPrePatchGet && s.cfg.PodStatusStages >= 3 {
				_ = s.withMutateSem(ctx, func() error {
					_, err := n.KubeletClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
					return err
				})
			}

			totalDelay := s.cfg.PodStartupDelay + time.Duration(rand.Int64N(max(1, int64(s.cfg.Jitter))))
			if s.cfg.PodStatusStages >= 3 {
				numSleeps := 3 + 2*nInit
				step := max(20*time.Millisecond, totalDelay/time.Duration(numSleeps))
				for idx, ic := range pod.Spec.InitContainers {
					if !s.sleepAbortOnDelete(ctx, step, pod.UID) {
						goto nextPod
					}
					_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodPending, idx, idx, 0, false, "PodInitializing", false, false)
					s.emitPodEvent(ctx, n, pod, "Pulled", "Container image already present on machine ("+ic.Name+")")
					s.emitPodEvent(ctx, n, pod, "Created", "Created container "+ic.Name)
					s.emitPodEvent(ctx, n, pod, "Started", "Started container "+ic.Name)
					if !s.sleepAbortOnDelete(ctx, step, pod.UID) {
						goto nextPod
					}
					_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodPending, -1, idx+1, 0, false, "PodInitializing", false, false)
				}

				if !s.sleepAbortOnDelete(ctx, step, pod.UID) {
					continue
				}
				_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodPending, -1, nInit, 0, false, "ContainerCreating", false, false)
				s.emitPodEvent(ctx, n, pod, "Pulled", "Container image already present on machine")
				if nCont > 1 {
					s.emitPodEvent(ctx, n, pod, "Created", "Created container "+pod.Spec.Containers[0].Name)
				}

				if !s.sleepAbortOnDelete(ctx, step, pod.UID) {
					continue
				}
				_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodRunning, -1, nInit, 0, true, "", false, false)
				if nCont > 1 {
					s.emitPodEvent(ctx, n, pod, "Started", "Started container "+pod.Spec.Containers[0].Name)
					for ci := 1; ci < min(3, nCont); ci++ {
						s.emitPodEvent(ctx, n, pod, "Created", "Created container "+pod.Spec.Containers[ci].Name)
						s.emitPodEvent(ctx, n, pod, "Started", "Started container "+pod.Spec.Containers[ci].Name)
					}
					if len(pod.UID) > 0 && pod.UID[0]%2 == 0 {
						_ = s.patchPodStatusStage(ctx, n, pod, corev1.PodRunning, -1, nInit, nCont/2, true, "", false, false)
					}
				} else {
					s.emitPodEvent(ctx, n, pod, "Created", "Created container "+pod.Name)
				}

				if !s.sleepAbortOnDelete(ctx, step, pod.UID) {
					continue
				}
			} else if !s.sleepAbortOnDelete(ctx, totalDelay, pod.UID) {
				continue
			}
			if err := s.patchPodStatusStage(ctx, n, pod, corev1.PodRunning, -1, nInit, nCont, true, "", false, false); err != nil {
				go func(p *corev1.Pod) {
					time.Sleep(500 * time.Millisecond)
					s.seenUIDs.Delete(p.UID)
					s.inspectPod(n, p)
				}(pod)
				continue
			}
			s.seenUIDs.Store(pod.UID, 3)
			if nCont <= 1 {
				s.emitPodEvent(ctx, n, pod, "Started", "Started container "+pod.Name)
			}
		}
	}
}

func (s *Simulator) runNodePodWatch(ctx context.Context, n *SimNode, delay time.Duration) {
	if s.cfg.SharedPodWatch {
		s.runWatchStream(ctx, n.KubeletHTTP, delay, kubeletUserAgent, s.baseURL+"/api/v1/pods?watch=true&fieldSelector=spec.nodeName%3D"+n.Name)
		return
	}
	if !sleepWithContext(ctx, delay) {
		return
	}
	fieldSel := "spec.nodeName=" + n.Name
	for ctx.Err() == nil {
		if s.currentWatchMode() == "watchlist" {
			sendInit := true
			w, err := n.KubeletClient.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{
				FieldSelector:        fieldSel,
				AllowWatchBookmarks:  true,
				SendInitialEvents:    &sendInit,
				ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			})
			if err == nil {
				s.activeWatches.Add(1)
				for ev := range w.ResultChan() {
					if pod, ok := ev.Object.(*corev1.Pod); ok {
						if pod.ResourceVersion != "" {
							s.latestRV.Store(pod.ResourceVersion)
						}
						trimPodForSimulator(pod)
						if ev.Type == watch.Deleted {
							s.syncPodVolumeWatches(ctx, n, pod, false)
							s.seenUIDs.Delete(pod.UID)
						} else {
							s.inspectPod(n, pod)
						}
					}
				}
				w.Stop()
				s.activeWatches.Add(-1)
			}
			if !sleepWithContext(ctx, time.Duration(500+rand.IntN(1500))*time.Millisecond) {
				return
			}
			continue
		}

		lw := &cache.ListWatch{
			WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
				opts.FieldSelector, opts.AllowWatchBookmarks = fieldSel, true
				return n.KubeletClient.CoreV1().Pods("").Watch(ctx, opts)
			},
		}
		list, err := n.KubeletClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: fieldSel, ResourceVersion: "0"})
		if err != nil || list.ResourceVersion == "" {
			if !sleepWithContext(ctx, time.Second) {
				return
			}
			continue
		}
		rv := list.ResourceVersion
		s.latestRV.Store(rv)
		for i := range list.Items {
			trimPodForSimulator(&list.Items[i])
			pCopy := list.Items[i].DeepCopy()
			s.inspectPod(n, pCopy)
		}
		list.Items = nil
		if rw, err := retrywatcher.NewRetryWatcherWithContext(ctx, rv, lw); err == nil {
			s.activeWatches.Add(1)
			for ev := range rw.ResultChan() {
				if pod, ok := ev.Object.(*corev1.Pod); ok {
					if pod.ResourceVersion != "" {
						s.latestRV.Store(pod.ResourceVersion)
					}
					trimPodForSimulator(pod)
					if ev.Type == watch.Deleted {
						s.syncPodVolumeWatches(ctx, n, pod, false)
						s.seenUIDs.Delete(pod.UID)
					} else {
						s.inspectPod(n, pod)
					}
				}
			}
			s.activeWatches.Add(-1)
		}
		if !sleepWithContext(ctx, time.Second) {
			return
		}
	}
}

func (s *Simulator) runSharedPodWatch(ctx context.Context) {
	lw := &cache.ListWatch{
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			opts.AllowWatchBookmarks = true
			return s.adminClient.CoreV1().Pods("").Watch(ctx, opts)
		},
	}
	for ctx.Err() == nil {
		list, err := s.adminClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{ResourceVersion: "0"})
		if err != nil || list.ResourceVersion == "" {
			time.Sleep(time.Second)
			continue
		}
		rv := list.ResourceVersion
		s.latestRV.Store(rv)
		for i := range list.Items {
			pod := &list.Items[i]
			if pod.Spec.NodeName != "" {
				if n := s.nodesByName[pod.Spec.NodeName]; n != nil {
					trimPodForSimulator(pod)
					pCopy := pod.DeepCopy()
					s.inspectPod(n, pCopy)
				}
			}
			list.Items[i] = corev1.Pod{}
		}
		list.Items = nil
		list = nil
		if rw, err := retrywatcher.NewRetryWatcherWithContext(ctx, rv, lw); err == nil {
			for ev := range rw.ResultChan() {
				if pod, ok := ev.Object.(*corev1.Pod); ok {
					if pod.ResourceVersion != "" {
						s.latestRV.Store(pod.ResourceVersion)
					}
					if pod.Spec.NodeName != "" {
						trimPodForSimulator(pod)
						if n := s.nodesByName[pod.Spec.NodeName]; n != nil {
							if ev.Type == watch.Deleted {
								s.syncPodVolumeWatches(ctx, n, pod, false)
								s.seenUIDs.Delete(pod.UID)
							} else {
								s.inspectPod(n, pod)
							}
						}
					}
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func (s *Simulator) reconcilePodsOnReconnect(ctx context.Context) {
	list, err := s.adminClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{ResourceVersion: "0"})
	if err != nil || list == nil {
		return
	}
	for i := range list.Items {
		pod := &list.Items[i]
		if pod.Spec.NodeName != "" {
			if n := s.nodesByName[pod.Spec.NodeName]; n != nil {
				trimPodForSimulator(pod)
				pCopy := pod.DeepCopy()
				s.inspectPod(n, pCopy)
			}
		}
		list.Items[i] = corev1.Pod{}
	}
}

func (s *Simulator) startNodeWatches(ctx context.Context, n *SimNode, isReconnect bool, reconnectWindow time.Duration) {
	watchNodes := s.cfg.NumNodes
	if s.cfg.FullWatchNodes > 0 && s.cfg.FullWatchNodes < watchNodes {
		watchNodes = s.cfg.FullWatchNodes
	}
	window := 6 * time.Second
	if isReconnect {
		window = reconnectWindow
	}
	stagger := time.Duration(int64(n.Index) * int64(window) / int64(max(1, watchNodes)))
	if !s.cfg.SharedPodWatch || s.cfg.FullWatchNodes > 0 {
		go s.runNodePodWatch(ctx, n, stagger)
	}

	type watchSpec struct {
		client   *http.Client
		ua, path string
	}
	specs := []watchSpec{
		{n.KubeletHTTP, kubeletUserAgent, "/api/v1/nodes?watch=true&fieldSelector=metadata.name%3D" + n.Name},
		{n.KubeProxyHTTP, kubeProxyUserAgent, "/api/v1/services?watch=true&labelSelector=%21service.kubernetes.io%2Fservice-proxy-name"},
		{n.KubeProxyHTTP, kubeProxyUserAgent, "/apis/discovery.k8s.io/v1/endpointslices?watch=true&labelSelector=%21service.kubernetes.io%2Fheadless"},
		{n.KubeletHTTP, kubeletUserAgent, "/api/v1/services?watch=true"},
		{n.KubeProxyHTTP, kubeProxyUserAgent, "/api/v1/nodes?watch=true&fieldSelector=metadata.name%3D" + n.Name},
		{n.KubeletHTTP, kubeletUserAgent, "/apis/storage.k8s.io/v1/csidrivers?watch=true"},
		{n.KubeletHTTP, kubeletUserAgent, "/apis/storage.k8s.io/v1/csinodes?watch=true&fieldSelector=metadata.name%3D" + n.Name},
		{n.KubeletHTTP, kubeletUserAgent, "/apis/node.k8s.io/v1/runtimeclasses?watch=true"},
		{n.KubeProxyHTTP, kubeProxyUserAgent, "/apis/networking.k8s.io/v1/servicecidrs?watch=true"},
		{n.KubeletHTTP, kubeletUserAgent, "/api/v1/namespaces/kube-system/configmaps?watch=true&fieldSelector=metadata.name%3Dkube-root-ca.crt"},
	}
	if s.nodeAgents.Load() {
		specs = append(specs,
			watchSpec{n.KubeletHTTP, ciliumUserAgent, "/api/v1/pods?watch=true&fieldSelector=spec.nodeName%3D" + n.Name},
			watchSpec{n.KubeProxyHTTP, netdUserAgent, "/api/v1/pods?watch=true&fieldSelector=spec.nodeName%3D" + n.Name},
			watchSpec{n.KubeletHTTP, autoscalingUserAgent, "/api/v1/pods?watch=true&fieldSelector=spec.nodeName%3D" + n.Name},
		)
		for j, ns := range []string{"kube-system", "monitoring", "metrics-system"} {
			go s.runPrometheusNamespacePodWatch(ctx, n, stagger+time.Duration(len(specs)+j+1)*20*time.Millisecond, ns)
		}
	}
	stepGap := 40 * time.Millisecond
	if isReconnect {
		stepGap = 5 * time.Millisecond
	}
	for i, sp := range specs {
		go s.runWatchStream(ctx, sp.client, stagger+time.Duration(i+1)*stepGap, sp.ua, s.baseURL+sp.path)
	}
}

func (s *Simulator) startNodeSession(parentCtx context.Context, isReconnect bool) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	if s.sessionCancel != nil {
		s.sessionCancel()
	}
	sctx, cancel := context.WithCancel(parentCtx)
	s.sessionCancel = cancel
	s.disconnected.Store(false)

	reconnectWindow := time.Duration(s.reconnectStagger.Load())
	for _, n := range s.nodes {
		go s.runNodeLeaseController(sctx, n, isReconnect, reconnectWindow)
		go s.runNodeStatusLoop(sctx, n, isReconnect, reconnectWindow)
		if s.isFullWatchNode(n) {
			s.startNodeWatches(sctx, n, isReconnect, reconnectWindow)
		}
	}
	if isReconnect && s.cfg.SharedPodWatch {
		go s.reconcilePodsOnReconnect(sctx)
	}
}

func (s *Simulator) stopNodeSession() {
	s.sessionMu.Lock()
	cancel := s.sessionCancel
	s.sessionCancel = nil
	s.disconnected.Store(true)
	s.sessionMu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, n := range s.nodes {
		n.volMu.Lock()
		for _, c := range n.volCancels {
			if c != nil {
				c()
			}
		}
		clear(n.volRefCnt)
		clear(n.volCancels)
		clear(n.podVols)
		n.volMu.Unlock()
		closeClientConnections(n.KubeletHTTP)
		if n.KubeProxyHTTP != n.KubeletHTTP {
			closeClientConnections(n.KubeProxyHTTP)
		}
	}
	for _, tr := range s.sparseTransports {
		tr.CloseIdleConnections()
	}
}

// Disconnect cancels all per-node Lease, NodeStatus, and Watch loops and closes
// their underlying HTTP/2 connections. When fastUnready is true, it also patches
// NodeReady=Unknown via adminClient so KCM sees nodes NotReady immediately.
func (s *Simulator) Disconnect(ctx context.Context, fastUnready bool) time.Duration {
	start := time.Now()
	s.disconnectCount.Add(1)
	s.stopNodeSession()
	if fastUnready {
		s.markAllNodesReadyStatus(ctx, corev1.ConditionUnknown, "NodeStatusUnknown", "Kubelet stopped posting node status.")
	}
	d := time.Since(start).Round(time.Millisecond)
	klog.Infof("Disconnected all %d simulated nodes in %v (fast-unready=%v, active-watches=%d)",
		s.cfg.NumNodes, d, fastUnready, s.activeWatches.Load())
	return d
}

// Reconnect simultaneously reconnects all simulated nodes to the control plane,
// starting per-node Lease renewals, NodeStatus Ready=True updates, and
// WatchList/watch streams across all full-watch nodes.
func (s *Simulator) Reconnect(parentCtx context.Context) time.Duration {
	start := time.Now()
	s.reconnectCount.Add(1)
	s.startNodeSession(parentCtx, true)
	d := time.Since(start).Round(time.Millisecond)
	klog.Infof("Triggered reconnect for all %d simulated nodes in %v (watch-mode=%s, node-agents=%v, stagger=%v)",
		s.cfg.NumNodes, d, s.currentWatchMode(), s.nodeAgents.Load(), time.Duration(s.reconnectStagger.Load()))
	return d
}

func (s *Simulator) startControlServer(ctx context.Context) {
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sigCh:
				if sig == syscall.SIGUSR1 {
					s.Disconnect(ctx, false)
				} else if sig == syscall.SIGUSR2 {
					s.Reconnect(ctx)
				}
			}
		}
	}()

	if s.cfg.ControlAddr == "" {
		return
	}
	mux := http.NewServeMux()
	writeStatus := func(w http.ResponseWriter, action string, elapsed time.Duration) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"action":           action,
			"elapsed":          elapsed.String(),
			"nodes":            s.cfg.NumNodes,
			"fullWatchNodes":   s.cfg.FullWatchNodes,
			"disconnected":     s.disconnected.Load(),
			"activeWatches":    s.activeWatches.Load(),
			"watchMode":        s.currentWatchMode(),
			"nodeAgents":       s.nodeAgents.Load(),
			"reconnectStagger": time.Duration(s.reconnectStagger.Load()).String(),
			"disconnectCount":  s.disconnectCount.Load(),
			"reconnectCount":   s.reconnectCount.Load(),
		})
	}
	applyQueryOverrides := func(r *http.Request) {
		q := r.URL.Query()
		if wm := strings.ToLower(q.Get("watch_mode")); wm == "watchlist" || wm == "rv0" || wm == "latest" {
			s.watchMode.Store(wm)
		}
		if na := q.Get("node_agents"); na != "" {
			s.nodeAgents.Store(na == "1" || strings.EqualFold(na, "true"))
		}
		if st := q.Get("stagger"); st != "" {
			if d, err := time.ParseDuration(st); err == nil && d >= 0 {
				s.reconnectStagger.Store(int64(d))
			}
		}
	}
	mux.HandleFunc("/disconnect", func(w http.ResponseWriter, r *http.Request) {
		applyQueryOverrides(r)
		q := r.URL.Query()
		fast := q.Get("fast_unready") == "1" || strings.EqualFold(q.Get("fast_unready"), "true")
		d := s.Disconnect(r.Context(), fast)
		writeStatus(w, "disconnect", d)
	})
	mux.HandleFunc("/reconnect", func(w http.ResponseWriter, r *http.Request) {
		applyQueryOverrides(r)
		d := s.Reconnect(ctx)
		writeStatus(w, "reconnect", d)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, "status", 0)
	})

	srv := &http.Server{Addr: s.cfg.ControlAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		klog.Infof("kubelite control server listening on http://%s (/disconnect, /reconnect, /status; SIGUSR1=disconnect, SIGUSR2=reconnect)", s.cfg.ControlAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Warningf("control server %s exited: %v", s.cfg.ControlAddr, err)
		}
	}()
}

func (s *Simulator) maybeBindPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) {
	if pvc.Status.Phase != corev1.ClaimPending || pvc.DeletionTimestamp != nil || !strings.HasPrefix(pvc.Namespace, s.cfg.PVCNamespacePrefix) {
		return
	}
	if _, loaded := s.seenUIDs.LoadOrStore(pvc.UID, 1); loaded {
		return
	}
	pvName := fmt.Sprintf("kubelite-pv-%s-%s", pvc.Namespace, pvc.Name)
	qty := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if qty.IsZero() {
		qty = resource.MustParse("1Gi")
	}
	var scName string
	if pvc.Spec.StorageClassName != nil {
		scName = *pvc.Spec.StorageClassName
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: qty}, AccessModes: pvc.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete, StorageClassName: scName,
			PersistentVolumeSource: corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/" + pvName}},
			ClaimRef:               &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID},
		},
	}
	_, _ = s.adminClient.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
}

func (s *Simulator) watchAndBindPVCs(ctx context.Context) {
	lw := &cache.ListWatch{
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return s.adminClient.CoreV1().PersistentVolumeClaims("").Watch(ctx, opts)
		},
	}
	for ctx.Err() == nil {
		list, err := s.adminClient.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{ResourceVersion: "0"})
		if err != nil || list.ResourceVersion == "" {
			time.Sleep(time.Second)
			continue
		}
		for i := range list.Items {
			s.maybeBindPVC(ctx, &list.Items[i])
		}
		if rw, err := retrywatcher.NewRetryWatcherWithContext(ctx, list.ResourceVersion, lw); err == nil {
			for ev := range rw.ResultChan() {
				if pvc, ok := ev.Object.(*corev1.PersistentVolumeClaim); ok {
					s.maybeBindPVC(ctx, pvc)
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func (s *Simulator) Run(ctx context.Context) error {
	start := time.Now()
	s.ensureNodeWatchRBAC(ctx)
	existingReady := make(map[string]types.UID, s.cfg.NumNodes)
	if nodeList, err := s.adminClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{ResourceVersion: "0"}); err == nil && nodeList != nil {
		if nodeList.ResourceVersion != "" {
			s.latestRV.Store(nodeList.ResourceVersion)
		}
		for i := range nodeList.Items {
			nd := &nodeList.Items[i]
			for _, cond := range nd.Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
					existingReady[nd.Name] = nd.UID
					break
				}
			}
		}
	}
	sem := make(chan struct{}, max(1, s.cfg.RegisterConcurrency))
	var wg sync.WaitGroup
	var firstErr atomic.Value
	var registered atomic.Int64
	now := metav1.Now()
	for _, n := range s.nodes {
		if uid, ok := existingReady[n.Name]; ok {
			n.UID = uid
			n.TransitionTime = now
			registered.Add(1)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(node *SimNode) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.registerSingleNode(ctx, node); err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			if cur := registered.Add(1); cur%5000 == 0 || cur == int64(s.cfg.NumNodes) {
				klog.Infof("Registered %d/%d simulated nodes (%v elapsed)", cur, s.cfg.NumNodes, time.Since(start).Round(time.Millisecond))
			}
		}(n)
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return v.(error)
	}
	klog.Infof("Registered %d simulated nodes in %v (full-watch-nodes=%d, shared-pod-watch=%v, watch-mode=%s, node-agents=%v)",
		s.cfg.NumNodes, time.Since(start).Round(time.Millisecond), s.cfg.FullWatchNodes, s.cfg.SharedPodWatch, s.currentWatchMode(), s.nodeAgents.Load())

	for w := 0; w < s.cfg.PodWorkers; w++ {
		go s.podActuatorWorker(ctx)
	}
	go s.watchAndBindPVCs(ctx)
	if s.cfg.SharedPodWatch {
		go s.runSharedPodWatch(ctx)
	}

	s.startControlServer(ctx)
	s.startNodeSession(ctx, false)

	<-ctx.Done()
	s.stopNodeSession()
	if s.cfg.CleanupOnExit {
		cctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		_ = s.adminClient.CoreV1().Nodes().DeleteCollection(cctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: simulatedLabelKey + "=true"})
	}
	return nil
}

func NewSimulator(cfg Config) (*Simulator, error) {
	cfg.Kubeconfig = resolveKubeconfigPath(cfg.Kubeconfig)
	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig %q: %w", cfg.Kubeconfig, err)
	}
	if cfg.APIServerURL != "" {
		restCfg.Host = cfg.APIServerURL
	}
	restCfg.QPS, restCfg.Burst = 10000, 20000
	restCfg.ContentType = k8sruntime.ContentTypeProtobuf
	restCfg.AcceptContentTypes = k8sruntime.ContentTypeProtobuf + "," + k8sruntime.ContentTypeJSON

	adminClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(restCfg.Host)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := rest.TLSConfigFor(restCfg)
	if err != nil {
		return nil, err
	}
	tlsCfg.NextProtos = []string{"h2", "http/1.1"}
	tlsCfg.ClientSessionCache = tls.NewLRUClientSessionCache(512)

	var tokensByUser map[string]string
	nodeTLSConfig := tlsCfg
	if cfg.TokensFile != "" {
		tokensByUser, err = loadNodeTokens(cfg.TokensFile)
		if err != nil {
			return nil, fmt.Errorf("loading tokens file %q: %w", cfg.TokensFile, err)
		}
		// Strip client certificate so kube-apiserver authenticates via Bearer token
		// (hitting NodeAuthorizer) instead of X509 kubernetes-admin.
		nodeTLSConfig = tlsCfg.Clone()
		nodeTLSConfig.Certificates = nil
		nodeTLSConfig.GetClientCertificate = nil
	}

	sim := &Simulator{
		cfg:           cfg,
		baseURL:       strings.TrimRight(restCfg.Host, "/"),
		isLoopback:    u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost",
		adminClient:   adminClient,
		tlsConfig:     tlsCfg,
		nodeTLSConfig: nodeTLSConfig,
		nodesByName:   make(map[string]*SimNode, cfg.NumNodes),
		nodes:         make([]*SimNode, cfg.NumNodes),
		podWorkCh:     make(chan *corev1.Pod, max(131072, cfg.NumNodes*8)),
		podMutateSem:  make(chan struct{}, max(1, cfg.MaxInflightMut)),
		eventSem:      make(chan struct{}, max(128, cfg.MaxInflightMut)),
	}
	sim.watchMode.Store(strings.ToLower(cfg.WatchMode))
	sim.nodeAgents.Store(cfg.SimulateNodeAgents)
	sim.reconnectStagger.Store(int64(cfg.ReconnectStagger))

	if cfg.FullWatchNodes > 0 && cfg.FullWatchNodes < cfg.NumNodes {
		sim.sparseTransports = make([]*http.Transport, sparseTransportCnt)
		for i := range sim.sparseTransports {
			sim.sparseTransports[i] = sim.newTransport(cfg.FullWatchNodes*2 + i)
		}
	}

	capList := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("4"),
		corev1.ResourceMemory:           resource.MustParse("16Gi"),
		corev1.ResourcePods:             resource.MustParse("110"),
		corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
	}
	if cfg.NodeAccelerators > 0 {
		capList[corev1.ResourceName("kubelite.k8s.io/tpu")] = resource.MustParse(fmt.Sprintf("%d", cfg.NodeAccelerators))
	}

	nodeRestCfg := rest.CopyConfig(restCfg)
	nodeRestCfg.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()
	if cfg.TokensFile != "" {
		nodeRestCfg.CertData, nodeRestCfg.KeyData = nil, nil
		nodeRestCfg.CertFile, nodeRestCfg.KeyFile = "", ""
		nodeRestCfg.BearerToken, nodeRestCfg.BearerTokenFile = "", ""
	}
	zones := []string{"us-east1-b", "us-east1-c", "us-east1-d"}

	for i := 0; i < cfg.NumNodes; i++ {
		name := formatNodeName(cfg.NodePrefix, i, cfg.NumNodes)
		var kubeletHTTP, kubeProxyHTTP *http.Client
		isFull := sim.isFullWatchNodeIndex(i)
		if cfg.TokensFile != "" {
			tok := tokensByUser["system:node:"+name]
			if tok == "" {
				tok = "token-" + name
			}
			kubeletHTTP = sim.newNodeHTTPClient(i*2, isFull, "", nil, tok, kubeletUserAgent)
			if isFull {
				kubeProxyHTTP = sim.newNodeHTTPClient(i*2+1, true, "", nil, tok, kubeProxyUserAgent)
			} else {
				kubeProxyHTTP = kubeletHTTP
			}
		} else {
			kubeletHTTP = sim.newNodeHTTPClient(i*2, isFull, "system:node:"+name, []string{"system:nodes", "system:authenticated"}, restCfg.BearerToken, kubeletUserAgent)
			if isFull {
				kubeProxyHTTP = sim.newNodeHTTPClient(i*2+1, true, "system:kube-proxy", []string{"system:authenticated"}, restCfg.BearerToken, kubeProxyUserAgent)
			} else {
				kubeProxyHTTP = kubeletHTTP
			}
		}
		kubeletClient, err := kubernetes.NewForConfigAndClient(nodeRestCfg, kubeletHTTP)
		if err != nil {
			return nil, err
		}
		internalIP := fmt.Sprintf("10.%d.%d.%d", 128+(i/(250*250)), (i/250)%250+1, (i%250)+2)
		n := &SimNode{
			Index: i, Name: name, InternalIP: internalIP,
			Zone: zones[i%len(zones)], KubeletClient: kubeletClient, KubeletHTTP: kubeletHTTP, KubeProxyHTTP: kubeProxyHTTP, Capacity: capList,
			volRefCnt: make(map[string]int), volCancels: make(map[string]context.CancelFunc), podVols: make(map[types.UID][]string),
		}
		sim.nodes[i], sim.nodesByName[name] = n, n
	}
	return sim, nil
}

func runEmbeddedScript(scriptName string, scriptBytes []byte, extraEnv []string) error {
	dir, err := os.MkdirTemp("", "kubelite-cmd-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	assets := map[string][]byte{
		"kind-5k-scale.yaml":     embeddedKind5kYAML,
		"kind-65k-scale.yaml":    embeddedKind65kYAML,
		"up-5k-control-plane.sh": embeddedUpScript,
		"hotswap-apiserver.sh":   embeddedHotswapScript,
	}
	for name, data := range assets {
		mode := os.FileMode(0644)
		if strings.HasSuffix(name, ".sh") {
			mode = 0755
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, mode); err != nil {
			return err
		}
	}
	cmd := exec.Command("bash", filepath.Join(dir, scriptName))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = append(os.Environ(), extraEnv...)
	return cmd.Run()
}

func runUpSubcommand(args []string) error {
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	scale := fs.String("scale", "5k", "Control-plane scale preset: 5k or 65k")
	clusterName := fs.String("cluster-name", "", "Optional kind cluster name override")
	numNodes := fs.Int("nodes", 0, "Optional static token count override")
	dataDir := fs.String("data-dir", "", "Optional host directory for etcd and kubeconfig (default /var/lib/kubelite)")
	apiserverBin := fs.String("apiserver-bin", "", "Optional custom kube-apiserver binary to hotswap after cluster creation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	env := []string{"SCALE=" + *scale}
	if *clusterName != "" {
		env = append(env, "CLUSTER_NAME="+*clusterName)
	}
	if *numNodes > 0 {
		env = append(env, fmt.Sprintf("NUM_NODES=%d", *numNodes))
	}
	if *dataDir != "" {
		env = append(env, "DATA_DIR="+*dataDir)
	}
	if *apiserverBin != "" {
		env = append(env, "CUSTOM_APISERVER_BIN="+*apiserverBin)
	}
	return runEmbeddedScript("up-5k-control-plane.sh", embeddedUpScript, env)
}

func runHotswapSubcommand(args []string) error {
	fs := flag.NewFlagSet("hotswap", flag.ContinueOnError)
	clusterName := fs.String("cluster-name", "kubelite-5k", "Target kind cluster name")
	apiserverBin := fs.String("apiserver-bin", "", "Path to static kube-apiserver binary")
	worktree := fs.String("k8s-worktree", "", "Path to kubernetes source checkout if building kube-apiserver")
	rebuild := fs.Bool("rebuild", false, "Rebuild kube-apiserver with CGO_ENABLED=0 before swapping")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *apiserverBin == "" && fs.NArg() > 0 {
		*apiserverBin = fs.Arg(0)
	}
	env := []string{"CLUSTER_NAME=" + *clusterName}
	if *apiserverBin != "" {
		env = append(env, "APISERVER_BIN="+*apiserverBin)
	}
	if *worktree != "" {
		env = append(env, "K8S_WORKTREE="+*worktree)
	}
	if *rebuild {
		env = append(env, "REBUILD_APISERVER=1")
	}
	return runEmbeddedScript("hotswap-apiserver.sh", embeddedHotswapScript, env)
}

func runControlSubcommand(action string, args []string) (string, error) {
	fs := flag.NewFlagSet(action, flag.ContinueOnError)
	addr := fs.String("control-addr", "127.0.0.1:18080", "kubelite HTTP control server address")
	fastUnready := fs.Bool("fast-unready", false, "Immediately patch NodeReady=Unknown on disconnect")
	watchMode := fs.String("watch-mode", "", "Override watch mode on reconnect: watchlist, rv0, or latest")
	nodeAgents := fs.String("node-agents", "", "Override simulated node agents on reconnect: true or false")
	stagger := fs.String("stagger", "", "Override reconnect stagger window (e.g. 500ms)")
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	q := url.Values{}
	if *fastUnready {
		q.Set("fast_unready", "1")
	}
	if *watchMode != "" {
		q.Set("watch_mode", *watchMode)
	}
	if *nodeAgents != "" {
		q.Set("node_agents", *nodeAgents)
	}
	if *stagger != "" {
		q.Set("stagger", *stagger)
	}
	reqURL := fmt.Sprintf("http://%s/%s", strings.TrimPrefix(strings.TrimPrefix(*addr, "http://"), "https://"), action)
	if enc := q.Encode(); enc != "" {
		reqURL += "?" + enc
	}
	resp, err := http.Get(reqURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return strings.TrimSpace(string(body)), nil
}

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "up":
			if err := runUpSubcommand(os.Args[2:]); err != nil {
				klog.Fatalf("kubelite up failed: %v", err)
			}
			return
		case "hotswap":
			if err := runHotswapSubcommand(os.Args[2:]); err != nil {
				klog.Fatalf("kubelite hotswap failed: %v", err)
			}
			return
		case "disconnect", "reconnect", "status":
			out, err := runControlSubcommand(os.Args[1], os.Args[2:])
			if err != nil {
				klog.Fatalf("kubelite %s failed: %v", os.Args[1], err)
			}
			fmt.Println(out)
			return
		}
	}

	var cfg Config
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", defaultKubeconfigPath(), "Path to kubeconfig (defaults to /var/lib/kubelite/kubeconfig with fallback to ~/.kube/kubelite-config and ~/.kube/config)")
	flag.StringVar(&cfg.APIServerURL, "apiserver-url", "", "Optional apiserver URL override (e.g. https://172.18.0.2:6443)")
	flag.StringVar(&cfg.TokensFile, "tokens-file", "", "Optional known_tokens.csv path for per-node Bearer token authentication")
	flag.StringVar(&cfg.ControlAddr, "control-addr", "127.0.0.1:18080", "HTTP control address for /disconnect, /reconnect, and /status (empty to disable)")
	flag.StringVar(&cfg.WatchMode, "watch-mode", "rv0", "Watch initialization mode on boot and reconnect: rv0 (resourceVersion=0, 5k CI default), watchlist (sendInitialEvents=true), or latest")
	flag.IntVar(&cfg.NumNodes, "nodes", 5000, "Number of simulated nodes")
	flag.StringVar(&cfg.NodePrefix, "node-prefix", "kubelite-", "Simulated node name prefix")
	flag.StringVar(&cfg.PVCNamespacePrefix, "pvc-namespace-prefix", "test-", "Namespace prefix for auto-binding Pending PVCs")
	flag.IntVar(&cfg.PodCIDRBaseOctet, "pod-cidr-base-octet", 64, "Second IPv4 octet base for /26 node PodCIDRs (10.<base>.0.0/10)")
	flag.IntVar(&cfg.RegisterConcurrency, "register-concurrency", 128, "Concurrency for initial node registration")
	flag.IntVar(&cfg.PodWorkers, "pod-workers", 512, "Concurrent workers actuating pod lifecycle transitions")
	flag.IntVar(&cfg.MaxInflightMut, "max-inflight-pod-mutations", 96, "Max concurrent in-flight pod status/event/delete mutations")
	flag.IntVar(&cfg.PodStatusStages, "pod-status-stages", 3, "Number of PodStatus PATCH stages per pod startup (1 or 3)")
	flag.IntVar(&cfg.FullWatchNodes, "full-watch-nodes", 0, "Number of nodes opening full per-node watch streams and 10s lease loops (0 = all nodes)")
	flag.IntVar(&cfg.NodeAccelerators, "node-accelerators", 0, "Number of kubelite.k8s.io/tpu resources per node")
	flag.DurationVar(&cfg.LeaseInterval, "lease-interval", 10*time.Second, "Per-node Lease heartbeat interval for full-watch nodes")
	flag.DurationVar(&cfg.SparseLeaseInterval, "sparse-lease-interval", 180*time.Second, "Per-node Lease heartbeat interval for sparse nodes (>= full-watch-nodes)")
	flag.DurationVar(&cfg.NodeStatusInterval, "node-status-interval", 240*time.Second, "Per-node NodeStatus PATCH interval for full-watch nodes")
	flag.DurationVar(&cfg.SparseNodeStatusInterval, "sparse-node-status-interval", 180*time.Second, "Per-node NodeStatus PATCH interval for sparse nodes (0 = only at registration)")
	flag.DurationVar(&cfg.PodStartupDelay, "pod-startup-delay", 150*time.Millisecond, "Minimum delay before patching PodStatus")
	flag.DurationVar(&cfg.Jitter, "pod-startup-jitter", 350*time.Millisecond, "Random startup jitter added to pod-startup-delay")
	flag.DurationVar(&cfg.ReconnectStagger, "reconnect-stagger", 500*time.Millisecond, "Stagger window across nodes when reconnecting after /disconnect")
	flag.BoolVar(&cfg.TaintSimulated, "taint-simulated", true, "Taint simulated nodes with kubelite.io/simulated=true:NoSchedule")
	flag.BoolVar(&cfg.CleanupOnExit, "cleanup-on-exit", false, "Delete simulated nodes on SIGINT/SIGTERM")
	flag.BoolVar(&cfg.SharedPodWatch, "shared-pod-watch", false, "Use a single cluster-wide Pod watch for pod actuation (auto-enabled when --full-watch-nodes > 0)")
	flag.BoolVar(&cfg.SkipPrePatchGet, "skip-pre-patch-get", false, "Skip GET /pods/{name} before first PodStatus PATCH attempt")
	flag.BoolVar(&cfg.EmitPodEvents, "emit-pod-events", true, "Emit Pod Pulled/Created/Started Events")
	flag.BoolVar(&cfg.RequestPodTokens, "request-pod-tokens", true, "Issue POST /serviceaccounts/{sa}/token per pod startup")
	flag.BoolVar(&cfg.SimulateNodeAgents, "simulate-node-agents", false, "Also open cilium-agent, netd, autoscaling-metrics-pipeline Pod WatchLists and 3 Prometheus/ namespace LIST+WATCH streams per full-watch node")
	klog.InitFlags(nil)
	flag.Parse()

	if cfg.FullWatchNodes > 0 {
		cfg.SharedPodWatch = true
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sim, err := NewSimulator(cfg)
	if err != nil {
		klog.Fatalf("Failed to initialize kubelite: %v", err)
	}
	if err := sim.Run(ctx); err != nil && ctx.Err() == nil {
		klog.Fatalf("kubelite failed: %v", err)
	}
}
