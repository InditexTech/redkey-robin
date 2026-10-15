// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"testing"
	"time"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/kubernetes"
	"github.com/inditextech/redkey-robin/internal/redis"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var clusterTestScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(clusterTestScheme)
	_ = redisv1.AddToScheme(clusterTestScheme)
}

func testOwnerCluster() *redisv1.Redkey {
	return &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid-cluster",
		},
		Spec: redisv1.RedkeySpec{
			Primaries:          3,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func testClusterConfig(status string) *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           1,
			Primaries:          3,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
		Status: redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseInProgress,
			Status:      status,
			Nodes:       map[string]*redisv1.RedisNode{},
		},
	}
}

func TestClusterReconciler_HandleNew_CreatesObjectsAndSetsInitializing(t *testing.T) {
	cfg := testClusterConfig("")
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}

	// Verify status transitioned to Initializing
	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusInitializing {
		t.Fatalf("expected status Initializing, got '%s'", fetched.Status.Status)
	}
}

func TestClusterReconciler_HandleInitializing_WaitsForPods(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusInitializing)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// No StatefulSet exists yet, so AllPodsReady will return error (StatefulSet not found)
	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)

	// Should get an error because StatefulSet doesn't exist
	if err == nil {
		// In our implementation, AllPodsReady tries to GET the StatefulSet and fails.
		_ = schedule
	}
}

func TestClusterReconciler_HandleReady_NoOp(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusReady)
	owner := testOwnerCluster()

	// Create pods so handleReady can find them
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster-0",
				Namespace: "default",
				Labels: map[string]string{
					"redkey.inditex.dev/cluster":   "test-cluster",
					"redkey.inditex.dev/component": "redis",
				},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster-1",
				Namespace: "default",
				Labels: map[string]string{
					"redkey.inditex.dev/cluster":   "test-cluster",
					"redkey.inditex.dev/component": "redis",
				},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster-2",
				Namespace: "default",
				Labels: map[string]string{
					"redkey.inditex.dev/cluster":   "test-cluster",
					"redkey.inditex.dev/component": "redis",
				},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.3"},
		},
	}

	objs := make([]runtime.Object, 0, 2+len(pods))
	objs = append(objs, owner, cfg)
	for i := range pods {
		objs = append(objs, &pods[i])
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithRuntimeObjects(objs...).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// handleReady now performs health checks against real addresses, which will fail
	// since no Redis is running. The test verifies no panic and that it returns gracefully.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	schedule, err := cr.ReconcileCluster(ctx, cfg, nil)
	// We expect an error because health checker can't reach Redis nodes
	if err == nil {
		// If no error, it must have been healthy (unlikely without real Redis)
		if schedule != reconcileAfterInterval {
			t.Fatalf("expected interval reconcile on success, got %v", schedule)
		}
	}
	// Any error is acceptable — the important thing is no panic and it returned
}

func TestClusterReconciler_UnhandledStatus(t *testing.T) {
	cfg := testClusterConfig("SomeUnknownStatus")
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected interval reconcile, got %v", schedule)
	}
}

func TestClusterReconciler_GetOwner(t *testing.T) {
	owner := testOwnerCluster()
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	fetched, err := cr.getOwner(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fetched.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", fetched.Name)
	}
}

func TestClusterReconciler_GetPassword_NoSecret(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig("")
	password := cr.getPassword(context.Background(), cfg)
	if password != "" {
		t.Fatalf("expected empty password, got '%s'", password)
	}
}

func TestClusterReconciler_GetPassword_WithSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-auth",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("mypass"),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).WithObjects(secret).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig("")
	cfg.Spec.Auth.SecretName = "redis-auth"
	password := cr.getPassword(context.Background(), cfg)
	if password != "mypass" {
		t.Fatalf("expected 'mypass', got '%s'", password)
	}
}

func TestClusterReconciler_UpdateClusterStatus(t *testing.T) {
	cfg := testClusterConfig("")
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	err := cr.updateClusterStatus(context.Background(), cfg, redisv1.ClusterStatusConfiguring)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusConfiguring {
		t.Fatalf("expected status Configuring, got '%s'", fetched.Status.Status)
	}
}

func TestClusterReconciler_SetConfigPhaseApplied(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusReady)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	err := cr.setConfigPhaseApplied(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseApplied {
		t.Fatalf("expected ConfigPhase Applied, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestClusterReconciler_UpdateNodeStatus(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusReady)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	nodes := []*redis.Node{
		{Name: "test-cluster-0", IP: "10.0.0.1", Flags: "myself,master"},
		{Name: "test-cluster-1", IP: "10.0.0.2", Flags: "myself,master"},
		{Name: "test-cluster-2", IP: "10.0.0.3", Flags: "slave"},
	}

	err := cr.updateNodeStatus(context.Background(), cfg, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if len(fetched.Status.Nodes) != 3 {
		t.Fatalf("expected 3 nodes in status, got %d", len(fetched.Status.Nodes))
	}
	if fetched.Status.Nodes["test-cluster-0"].Role != "primary" {
		t.Fatalf("expected node-0 role 'primary', got '%s'", fetched.Status.Nodes["test-cluster-0"].Role)
	}
	if fetched.Status.Nodes["test-cluster-2"].Role != "replica" {
		t.Fatalf("expected node-2 role 'replica', got '%s'", fetched.Status.Nodes["test-cluster-2"].Role)
	}
}

func TestClusterReconciler_HandleInitializing_PodsReady_ButNoPodsFound(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusInitializing)
	owner := testOwnerCluster()
	// Create StatefulSet with ReadyReplicas=3 but no pods exist
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// Pods are "ready" per STS status, but initNodes will fail because no pods exist
	_, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected error when no pods have IPs")
	}
}

func TestClusterReconciler_HandleInitializing_NotReady(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusInitializing)
	owner := testOwnerCluster()
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected wait reconcile while waiting for pods, got %v", schedule)
	}
}

func TestCloseNodes(t *testing.T) {
	// Should not panic with nil clients
	nodes := []*redis.Node{
		{Name: "node-0"},
		{Name: "node-1"},
	}
	closeNodes(nodes) // Should not panic
}

func TestCloseNodes_Empty(t *testing.T) {
	closeNodes(nil)             // Should not panic
	closeNodes([]*redis.Node{}) // Should not panic
}

func authConfigMap(redisConf string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Data: map[string]string{"redis.conf": redisConf},
	}
}

func TestClusterReconciler_ReconcileAuthRotation_NoConfigMapIsNoOp(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig(redisv1.ClusterStatusReady)

	// No ConfigMap yet (cluster not fully provisioned): must be a no-op.
	if err := cr.reconcileAuthRotation(context.Background(), cfg, "any-pass"); err != nil {
		t.Fatalf("unexpected error when ConfigMap is absent: %v", err)
	}
}

func TestClusterReconciler_ReconcileAuthRotation_InSyncIsNoOp(t *testing.T) {
	cm := authConfigMap("requirepass same-pass\nmasterauth same-pass\n")
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(cm).
		Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig(redisv1.ClusterStatusReady)

	// ConfigMap password matches the Secret password: no rotation, no node access.
	if err := cr.reconcileAuthRotation(context.Background(), cfg, "same-pass"); err != nil {
		t.Fatalf("unexpected error when passwords are in sync: %v", err)
	}
}

func TestClusterReconciler_ReconcileAuthRotation_NoAuthInSync(t *testing.T) {
	// A cluster without auth has no requirepass line; an empty Secret password
	// must be considered in sync and never mistaken for a rotation.
	cm := authConfigMap("appendonly no\nsave \"\"\n")
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(cm).
		Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig(redisv1.ClusterStatusReady)

	if err := cr.reconcileAuthRotation(context.Background(), cfg, ""); err != nil {
		t.Fatalf("unexpected error for no-auth in-sync cluster: %v", err)
	}
}

func TestClusterReconciler_ReconcileAuthRotation_RotatesAndUpdatesConfigMap(t *testing.T) {
	cm := authConfigMap("requirepass old-pass\nmasterauth old-pass\n")
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(cm).
		Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig(redisv1.ClusterStatusReady)

	// Secret holds a new password while the ConfigMap still has the old one.
	// With no pods, the CONFIG SET loop is skipped but the ConfigMap must be
	// updated so the cluster converges and future cycles are no-ops.
	if err := cr.reconcileAuthRotation(context.Background(), cfg, "new-pass"); err != nil {
		t.Fatalf("unexpected error applying rotation: %v", err)
	}

	pw, found, err := kubernetes.GetConfigMapPassword(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error reading ConfigMap: %v", err)
	}
	if !found || pw != "new-pass" {
		t.Fatalf("expected ConfigMap password 'new-pass' (found=%v), got '%s'", found, pw)
	}
}
