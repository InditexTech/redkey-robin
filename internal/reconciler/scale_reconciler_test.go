// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"testing"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/kubernetes"
	"github.com/inditextech/redkey-robin/internal/redis"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func scalingTestConfig(status string, primaries, replicasPerPrimary int32) *redisv1.RedkeyConfig {
	cfg := testClusterConfig(status)
	cfg.Spec.Primaries = primaries
	cfg.Spec.ReplicasPerPrimary = replicasPerPrimary
	return cfg
}

// makeStatefulSet returns a StatefulSet of the given size with no ready pods.
func makeStatefulSet(replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
}

// --- Pure helpers ---

func TestFastScalingEligible(t *testing.T) {
	cases := []struct {
		name      string
		ephemeral bool
		replicas  int32
		purge     *bool
		want      bool
	}{
		{"ephemeral no-replicas purge", true, 0, boolPtr(true), true},
		{"not ephemeral", false, 0, boolPtr(true), false},
		{"has replicas", true, 1, boolPtr(true), false},
		{"purge nil", true, 0, nil, false},
		{"purge false", true, 0, boolPtr(false), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := scalingTestConfig("", 3, tc.replicas)
			cfg.Spec.Ephemeral = tc.ephemeral
			cfg.Spec.PurgeKeysOnRebalance = tc.purge
			if got := fastScalingEligible(cfg); got != tc.want {
				t.Fatalf("fastScalingEligible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTotalNodes(t *testing.T) {
	cases := []struct {
		primaries, replicas, want int32
	}{
		{3, 0, 3},
		{3, 1, 6},
		{5, 2, 15},
	}
	for _, tc := range cases {
		cfg := scalingTestConfig("", tc.primaries, tc.replicas)
		if got := totalNodes(cfg); got != int(tc.want) {
			t.Fatalf("totalNodes(%d,%d) = %d, want %d", tc.primaries, tc.replicas, got, tc.want)
		}
	}
}

func TestOrdinalOf(t *testing.T) {
	cr := NewClusterReconciler(nil, "test-cluster", "default", config.NewRuntimeConfig())
	cases := []struct {
		name string
		want int
	}{
		{"test-cluster-0", 0},
		{"test-cluster-3", 3},
		{"test-cluster-12", 12},
		{"noordinal", 1 << 30},
		{"trailing-", 1 << 30},
	}
	for _, tc := range cases {
		if got := cr.ordinalOf(tc.name); got != tc.want {
			t.Fatalf("ordinalOf(%q) = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// --- handleScalingUp ---

func TestHandleScalingUp_ScalesStatefulSetAndWaitsForPods(t *testing.T) {
	cfg := scalingTestConfig(redisv1.ClusterStatusScalingUp, 5, 0)
	owner := testOwnerCluster()
	sts := makeStatefulSet(3) // old size, no pods ready

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleScalingUp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected wait interval while pods not ready, got %v", schedule)
	}

	replicas, err := kubernetes.GetStatefulSetReplicas(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error reading replicas: %v", err)
	}
	if replicas != 5 {
		t.Fatalf("expected StatefulSet scaled to 5, got %d", replicas)
	}
}

func TestHandleScalingUp_RoutedViaReconcile(t *testing.T) {
	cfg := scalingTestConfig(redisv1.ClusterStatusScalingUp, 5, 0)
	owner := testOwnerCluster()
	sts := makeStatefulSet(3)

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	if _, err := cr.ReconcileCluster(context.Background(), cfg, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replicas, err := kubernetes.GetStatefulSetReplicas(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error reading replicas: %v", err)
	}
	if replicas != 5 {
		t.Fatalf("expected ScalingUp status to scale StatefulSet to 5, got %d", replicas)
	}
}

// --- handleScalingDown ---

func TestHandleScalingDown_ShrinksStatefulSetWhenNodesAlreadyGone(t *testing.T) {
	cfg := scalingTestConfig(redisv1.ClusterStatusScalingDown, 3, 0)
	owner := testOwnerCluster()
	sts := makeStatefulSet(5) // larger than target, no pods

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleScalingDown(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected wait interval, got %v", schedule)
	}

	replicas, err := kubernetes.GetStatefulSetReplicas(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error reading replicas: %v", err)
	}
	if replicas != 3 {
		t.Fatalf("expected StatefulSet scaled down to 3, got %d", replicas)
	}
}

// --- handleFastScaling ---

func TestHandleFastScaling_DeletesStatefulSetAtWrongSize(t *testing.T) {
	cfg := scalingTestConfig(redisv1.ClusterStatusScalingUp, 5, 0)
	cfg.Spec.Ephemeral = true
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(true)
	owner := testOwnerCluster()
	sts := makeStatefulSet(3)

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleFastScaling(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected wait interval, got %v", schedule)
	}

	exists, err := kubernetes.StatefulSetExists(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("expected StatefulSet to be deleted for fast scaling recreate")
	}
}

func TestHandleFastScaling_RecreatesObjectsWhenMissing(t *testing.T) {
	cfg := scalingTestConfig(redisv1.ClusterStatusScalingUp, 5, 0)
	cfg.Spec.Ephemeral = true
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(true)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleFastScaling(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected wait interval, got %v", schedule)
	}

	exists, err := kubernetes.StatefulSetExists(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected StatefulSet to be recreated for fast scaling")
	}

	replicas, err := kubernetes.GetStatefulSetReplicas(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replicas != 5 {
		t.Fatalf("expected recreated StatefulSet sized to 5, got %d", replicas)
	}
}

// --- hasReplicaNodes ---

func TestHasReplicaNodes(t *testing.T) {
	cases := []struct {
		name  string
		flags []string
		want  bool
	}{
		{"no nodes", nil, false},
		{"only masters", []string{"master", "master"}, false},
		{"one replica", []string{"master", "slave"}, true},
		{"all replicas", []string{"slave", "slave"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nodes []*redis.Node
			for i, f := range tc.flags {
				n := redis.NewNode(fmt.Sprintf("node-%d", i), fmt.Sprintf("10.0.0.%d:6379", i), "")
				n.Flags = f
				nodes = append(nodes, n)
			}
			if got := hasReplicaNodes(nodes); got != tc.want {
				t.Fatalf("hasReplicaNodes = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- Scale down does NOT use fast scaling when current cluster has replicas ---
// This behavior is tested via hasReplicaNodes (unit) and the integration test
// "does not use fast scaling when transitioning from replicas>0 to replicas=0".
// A full handler-level unit test is not feasible without mocking Redis connections.
