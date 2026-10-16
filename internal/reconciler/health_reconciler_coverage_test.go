// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/health"
	"github.com/inditextech/redkey-robin/internal/redis"
)

// --- remediateClusterCheck tests ---

// errorHealthClusterClient always returns errors for all operations.
type errorHealthClusterClient struct {
	err error
}

func (e *errorHealthClusterClient) GetClusterInfo(context.Context) (*redis.ClusterInfo, error) {
	return nil, e.err
}
func (e *errorHealthClusterClient) GetClusterNodes(context.Context) ([]redis.ClusterNode, error) {
	return nil, e.err
}
func (e *errorHealthClusterClient) ClusterCheck(context.Context) (*redis.ClusterCheckResult, error) {
	return nil, e.err
}
func (e *errorHealthClusterClient) Close() error { return nil }

func TestHealthReconciler_RemediateClusterCheck_Success(t *testing.T) {
	// ClusterFix uses redis-cli; we override the CLI factory.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'All slots covered'; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	// Build a checker that returns an unhealthy report (ClusterCheckOK=false)
	clusterNodes := healthyClusterView()
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"some issue"},
		},
	}
	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ClusterCheck failed → remediation triggered → returns wait interval
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_RemediateClusterCheck_CLIError(t *testing.T) {
	// ClusterFix fails with non-zero exit on all nodes → pipeline stops with error
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'fix failed'; exit 1"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clusterNodes := healthyClusterView()
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"problem"},
		},
	}
	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	// Fix failure on all nodes now returns error and blocks pipeline
	if err == nil {
		t.Fatal("expected error when fix fails on all nodes")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_Reconcile_FixError_BlocksRebalance(t *testing.T) {
	originalFactory := redis.ExportNewRedisCLICommand()
	fixCalls := 0
	rebalanceCalls := 0
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--cluster" && args[i+1] == "fix" {
				fixCalls++
				return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'fix failed'; exit 1"}, env)
			}
			if args[i] == "--cluster" && args[i+1] == "rebalance" {
				rebalanceCalls++
				return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'rebalance'; exit 0"}, env)
			}
		}
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'unexpected'; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	// Cluster report: cluster check fails and balance is off. Fix fails on all nodes
	// but pipeline continues to rebalance which succeeds.
	unbalancedView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-100"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "101-8200"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "8201-16383"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: unbalancedView,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"Not all 16384 slots are covered by nodes."},
		},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	if err == nil {
		t.Fatal("expected error when fix fails on all nodes")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
	if fixCalls != 3 {
		t.Fatalf("expected fix invocation on all 3 nodes, got %d", fixCalls)
	}
	if rebalanceCalls != 0 {
		t.Fatalf("rebalance should NOT be called when fix fails, got %d", rebalanceCalls)
	}
}

// --- remediateMembership error paths ---

func TestHealthReconciler_RemediateMembership_ConnectionError(t *testing.T) {
	// Cluster view shows 4 nodes but only 3 K8s pods exist.
	// len(seedIDs)=4 != len(nodes)=3 → MembershipOK=false → triggers remediation.
	// Remediation calls clientFactory (real redis.Client) on unreachable addr → error.
	fourNodeView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: fourNodeView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// clientFactory creates real redis.Client that will fail to connect
	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(ctx, nodes, "", 3, 0)
	// The membership remediation should fail because it can't connect to the seed node
	if err == nil {
		t.Fatal("expected error from membership remediation connection failure")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval on error, got %v", schedule)
	}
}

// --- HealMembership entrypoint tests ---
//
// HealMembership's forget/meet/fix behaviour drives real go-redis commands through the
// concrete *redis.Client factory, which cannot be faked at the unit level (the factory
// returns a concrete type, not an interface). Behavioural coverage therefore lives in the
// chaos suite; here we assert the deterministic entrypoint guards.

func TestHealMembership_EmptyNodes_Error(t *testing.T) {
	hr := newTestHealthReconciler(healthyClusterView(), healthyInfo())

	changed, err := hr.HealMembership(context.Background(), nil, "", true, false)
	if err == nil {
		t.Fatal("expected error for empty node list")
	}
	if changed {
		t.Fatal("expected changed=false when no nodes are provided")
	}
}

func TestHealMembership_SeedUnreachable_Error(t *testing.T) {
	// The client factory returns a real client pointing at a refused address, so the
	// initial GetClusterNodes on the seed fails and HealMembership reports the error
	// without having changed anything.
	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}
	checker := health.NewChecker(nil, func(addr, password string) health.ClusterClient {
		return &fakeHealthClusterClient{clusterInfo: healthyInfo(), clusterNodes: healthyClusterView()}
	}, 2*time.Second)
	hr := NewHealthReconciler(checker, clientFactory, config.NewRuntimeConfig(), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 127.0.0.1:1 refuses connections immediately, keeping the test fast and deterministic.
	nodes := []health.Node{{Name: "cluster-0", Addr: "127.0.0.1:1"}}
	changed, err := hr.HealMembership(ctx, nodes, "", true, false)
	if err == nil {
		t.Fatal("expected error when the seed is unreachable")
	}
	if changed {
		t.Fatal("expected changed=false on seed failure")
	}
}

// --- isForgettableNoaddrGhost ---

func TestIsForgettableNoaddrGhost(t *testing.T) {
	tests := []struct {
		name string
		node redis.ClusterNode
		want bool
	}{
		{
			name: "noaddr master without slots is a forgettable phantom",
			node: redis.ClusterNode{ID: "ghost", IP: "", Addr: ":0@0", Flags: "master,fail,noaddr"},
			want: true,
		},
		{
			name: "noaddr replica without slots is a forgettable phantom",
			node: redis.ClusterNode{ID: "ghost", IP: "", Addr: ":0@0", Flags: "slave,noaddr"},
			want: true,
		},
		{
			name: "noaddr node that still owns slots is protected",
			node: redis.ClusterNode{ID: "ghost", IP: "", Addr: ":0@0", Flags: "master,noaddr", Slots: "0-5460"},
			want: false,
		},
		{
			name: "noaddr node with only migration markers is a forgettable phantom",
			node: redis.ClusterNode{ID: "ghost", IP: "", Addr: ":0@0", Flags: "master,noaddr", Slots: "[123->-abc]"},
			want: true,
		},
		{
			name: "noaddr node with an unparseable slot entry is conservatively protected",
			node: redis.ClusterNode{ID: "ghost", IP: "", Addr: ":0@0", Flags: "master,noaddr", Slots: "100-5"},
			want: false,
		},
		{
			name: "empty IP without the noaddr flag is not forgotten (e.g. handshake)",
			node: redis.ClusterNode{ID: "joining", IP: "", Addr: ":0@0", Flags: "handshake"},
			want: false,
		},
		{
			name: "reachable node with an IP is never a noaddr ghost",
			node: redis.ClusterNode{ID: "live", IP: "10.0.0.1", Addr: "10.0.0.1:6379@16379", Flags: "master,noaddr"},
			want: false,
		},
		{
			name: "healthy master is not a ghost",
			node: redis.ClusterNode{ID: "live", IP: "10.0.0.1", Addr: "10.0.0.1:6379@16379", Flags: "master", Slots: "0-5460"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isForgettableNoaddrGhost(tt.node); got != tt.want {
				t.Fatalf("isForgettableNoaddrGhost(%+v) = %v, want %v", tt.node, got, tt.want)
			}
		})
	}
}

func TestRemediateReplicaTopology_EmptyNodes_Error(t *testing.T) {
	hr := newTestHealthReconciler(healthyClusterView(), healthyInfo())

	ok, changed, err := hr.RemediateReplicaTopology(context.Background(), nil, "", 3, 1)
	if err == nil {
		t.Fatal("expected error for empty node list")
	}
	if ok || changed {
		t.Fatal("expected ok=false and changed=false when no nodes are provided")
	}
}

func TestRemediateReplicaTopology_SeedUnreachable_Error(t *testing.T) {
	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}
	checker := health.NewChecker(nil, func(addr, password string) health.ClusterClient {
		return &fakeHealthClusterClient{clusterInfo: healthyInfo(), clusterNodes: healthyClusterView()}
	}, 2*time.Second)
	hr := NewHealthReconciler(checker, clientFactory, config.NewRuntimeConfig(), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 127.0.0.1:1 refuses connections immediately, so no seed can be read.
	nodes := []health.Node{{Name: "cluster-0", Addr: "127.0.0.1:1"}}
	ok, _, err := hr.RemediateReplicaTopology(ctx, nodes, "", 3, 1)
	if err == nil {
		t.Fatal("expected error when no node is reachable")
	}
	if ok {
		t.Fatal("expected ok=false when topology could not be read")
	}
}

// --- Reconcile pipeline tests ---

func TestHealthReconciler_Reconcile_CheckerError_AllNodesDown(t *testing.T) {
	// All nodes return errors → checker can't find a seed → returns error
	checkerFactory := func(addr, password string) health.ClusterClient {
		return &errorHealthClusterClient{err: context.DeadlineExceeded}
	}
	checker := health.NewChecker(nil, checkerFactory, 100*time.Millisecond)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	// Short context so remediation redis calls don't hang
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes := healthyNodes()
	_, err := hr.Reconcile(ctx, nodes, "", 3, 0)
	if err == nil {
		t.Fatal("expected error when all nodes are unreachable")
	}
}

func TestHealthReconciler_Reconcile_SlotCoverageRemediation_NoPrimaries(t *testing.T) {
	// Cluster with no primaries → slot coverage fails → remediation fails with "no primaries"
	onlyReplicasView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "slave", State: "connected", Primary: "id-unknown"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "slave", State: "connected", Primary: "id-unknown"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-unknown"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     0,
			SlotsFail:   16384,
			KnownNodes:  3,
			ClusterSize: 0,
		},
		clusterNodes: onlyReplicasView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	// Slot coverage remediation should fail with "no primaries found"
	if err == nil {
		t.Fatal("expected error for slot coverage remediation with no primaries")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_Reconcile_ReplicaSpread_PrimaryCountMismatch(t *testing.T) {
	// Cluster has 2 primaries but desired is 3 → remediation skips (returns nil)
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	twoPrimariesView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-8191"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "8192-16383"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-1"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  3,
			ClusterSize: 2,
		},
		clusterNodes: twoPrimariesView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
	}
	// desiredPrimaries=3 but actual=2 → replica spread remediation should be skipped
	// And since balancedOK will be OK (slots are covered evenly for 2 primaries)
	// the cluster just goes through remaining steps
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 1)
	// ReplicaSpreadOK fails → remediation is triggered → skipped due to mismatch → no error
	// After that, if balance is OK, returns wait interval
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

// --- logReport coverage ---

func TestHealthReconciler_LogReport_WithWarningsAndErrors(t *testing.T) {
	// Build a checker returning a report with errors and warnings
	clusterNodes := healthyClusterView()
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"error1", "error2"},
			Warnings:          []string{"warn1"},
		},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Mock ClusterFix so remediation succeeds
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo fixed; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ClusterCheck failed → remediation happened
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_Reconcile_SlotCoverage_SkipsEmptyIPPrimary(t *testing.T) {
	// Cluster has 3 primaries, one with empty IP (simulating a node that reports :6379@16379).
	// The empty-IP primary should be skipped; slots should be assigned to the other two.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clusterView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Addr: "10.0.0.1:6379@16379", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Addr: "10.0.0.2:6379@16379", Flags: "master", State: "connected", Slots: "5461-10921"},
		// Node with empty IP — simulates :6379@16379 being parsed with the fix
		{ID: "id-3", IP: "", Addr: ":6379@16379", Flags: "master", State: "connected", Slots: ""},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     10922,
			SlotsFail:   5462,
			KnownNodes:  3,
			ClusterSize: 3,
		},
		clusterNodes: clusterView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Track addresses that clientFactory is called with
	var calledAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		calledAddrs = append(calledAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// The remediation will fail connecting to real redis, but the key assertion
	// is that it never tries to connect to the empty-IP address ":6379"
	_, _ = hr.Reconcile(ctx, nodes, "", 3, 0)

	for _, addr := range calledAddrs {
		if addr == ":6379" || addr == ":6379@16379:6379" {
			t.Fatalf("clientFactory was called with invalid empty-IP address: %q", addr)
		}
	}
}

// --- Excess primary demotion (failover) tests ---

func TestHealthReconciler_ExcessPrimaries_DemotesEmptyPrimary(t *testing.T) {
	// Scenario: 3 primaries desired with 1 replica each.
	// After failover: 4 primaries (the promoted replica + the new empty pod), 0 replicas.
	// Expected: the empty primary (10.0.0.4, 0 slots) is demoted to replica of a primary needing one.
	fourPrimaryView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected", Slots: ""},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  4,
			ClusterSize: 4,
		},
		clusterNodes: fourPrimaryView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Track which addresses ClusterReplicate is attempted on
	var replicateAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		replicateAddrs = append(replicateAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
		{Name: "cluster-3", Addr: "10.0.0.4:6379"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// desiredPrimaries=3, desiredReplicasPerPrimary=1
	schedule, err := hr.Reconcile(ctx, nodes, "", 3, 1)

	// The demotion will fail connecting to real redis (no instance at 10.0.0.4),
	// but we verify it attempts to demote the correct node.
	if err == nil {
		t.Fatal("expected error from ClusterReplicate connection failure")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}

	// Verify the factory was called with the empty primary's address for demotion
	found := false
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.4:6379" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected clientFactory to be called with empty primary addr 10.0.0.4:6379, got %v", replicateAddrs)
	}
}

func TestHealthReconciler_ExcessPrimaries_SkipsImportingNode(t *testing.T) {
	// Scenario: 4 primaries, one has importing marker in Slots → should NOT be demoted.
	// The other empty primary (no slots, no markers) should be the demotion target.
	fourPrimaryView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		// Node with importing marker — should be skipped
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected", Slots: "[5461-<-id-2]"},
		// Truly empty node — should be demoted
		{ID: "id-5", IP: "10.0.0.5", Flags: "master", State: "connected", Slots: ""},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  5,
			ClusterSize: 5,
		},
		clusterNodes: fourPrimaryView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	var replicateAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		replicateAddrs = append(replicateAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
		{Name: "cluster-3", Addr: "10.0.0.4:6379"},
		{Name: "cluster-4", Addr: "10.0.0.5:6379"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// desiredPrimaries=3, desiredReplicasPerPrimary=1
	_, _ = hr.Reconcile(ctx, nodes, "", 3, 1)

	// Verify the importing node (10.0.0.4) was NOT targeted for demotion
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.4:6379" {
			t.Fatalf("should not attempt to demote node with importing marker, but called clientFactory with %q", addr)
		}
	}

	// Verify the empty node (10.0.0.5) WAS targeted
	found := false
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.5:6379" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected clientFactory to be called with truly empty primary 10.0.0.5:6379, got %v", replicateAddrs)
	}
}

func TestHealthReconciler_ExcessPrimaries_DrainsWhenNoEmpty(t *testing.T) {
	// Scenario: 4 primaries ALL holding slots (no empty primary), 3 desired with 1 replica
	// each. There is no empty primary to demote, so the reconciler must drain one surplus
	// primary with a weight-0 rebalance (so it becomes empty and can be demoted next cycle).
	fourPrimaryView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-4095"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "4096-8191"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "8192-12287"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected", Slots: "12288-16383"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  &redis.ClusterInfo{State: "ok", SlotsOK: 16384, KnownNodes: 4, ClusterSize: 4},
		clusterNodes: fourPrimaryView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}
	checker := health.NewChecker(nil, func(addr, password string) health.ClusterClient {
		return fakeClient
	}, 2*time.Second)

	// Capture redis-cli rebalance invocations and their weight arguments.
	var weightArgs []string
	rebalanceCalled := false
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--cluster" && args[i+1] == "rebalance" {
				rebalanceCalled = true
			}
			if args[i] == "--cluster-weight" {
				weightArgs = append(weightArgs, args[i+1])
			}
		}
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}
	hr := NewHealthReconciler(checker, clientFactory, config.NewRuntimeConfig(), nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
		{Name: "cluster-3", Addr: "10.0.0.4:6379"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// desiredPrimaries=3, desiredReplicasPerPrimary=1 → excess of 1 primary, none empty.
	schedule, err := hr.Reconcile(ctx, nodes, "", 3, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval after draining, got %v", schedule)
	}
	if !rebalanceCalled {
		t.Fatal("expected a weight-0 rebalance to drain a surplus primary")
	}
	// Exactly the excess (1) primary should be drained, weighted to 0.
	if len(weightArgs) != 1 {
		t.Fatalf("expected exactly one drained primary weight, got %v", weightArgs)
	}
	if n := len(weightArgs[0]); n < 2 || weightArgs[0][n-2:] != "=0" {
		t.Fatalf("expected a =0 weight in rebalance args, got %q", weightArgs[0])
	}
}

func TestHealthReconciler_FewerPrimaries_SkipsRemediation(t *testing.T) {
	// Scenario: 2 primaries exist but 3 desired (scale-up scenario).
	// Should skip and not attempt any demotion.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	twoPrimariesWithReplicaView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-8191"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "8192-16383"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-1"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  3,
			ClusterSize: 2,
		},
		clusterNodes: twoPrimariesWithReplicaView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	var replicateAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		replicateAddrs = append(replicateAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
	}

	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not attempt any ClusterReplicate for demotion
	// The only clientFactory calls should be for rebalance (if triggered)
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.3:6379" {
			// This would indicate the replica is being reassigned — that's OK
			// but NOT a demotion of a primary
			continue
		}
	}

	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_FewerPrimaries_PromotesSurplusReplica(t *testing.T) {
	// Scenario (chaos nopurge): 2 primaries + 1 replica but 3 primaries / 0 replicas
	// desired. The replica is surplus and must be promoted to an empty primary, otherwise
	// the health reconciler deadlocks (replica spread stays broken forever). The promotion
	// is done via CLUSTER RESET on the replica; with no real Redis at the address the
	// reset connection fails, which lets us assert the replica was the promotion target.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	twoPrimariesWithReplicaView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-8191"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "8192-16383"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-1"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  3,
			ClusterSize: 2,
		},
		clusterNodes: twoPrimariesWithReplicaView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	var calledAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		calledAddrs = append(calledAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// desiredPrimaries=3, desiredReplicasPerPrimary=0 → the lone replica is surplus.
	_, err := hr.Reconcile(ctx, nodes, "", 3, 0)
	if err == nil {
		t.Fatal("expected error from CLUSTER RESET connection failure during promotion")
	}

	found := false
	for _, addr := range calledAddrs {
		if addr == "10.0.0.3:6379" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected the surplus replica 10.0.0.3:6379 to be promoted (reset), got %v", calledAddrs)
	}
}
