// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var testScheme = runtime.NewScheme()

func intPtr(v int) *int { return &v }

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = redisv1.AddToScheme(testScheme)
}

func newTestReconciler(objs ...client.Object) *Reconciler {
	// Always include the owner Redkey so cluster reconciler can find it.
	hasCluster := false
	for _, obj := range objs {
		if _, ok := obj.(*redisv1.Redkey); ok {
			hasCluster = true
			break
		}
	}
	if !hasCluster {
		objs = append(objs, testRedkey())
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(objs...).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()
	return NewReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfigWithReconcilerIntervals(5*time.Second, 2*time.Second, 10*time.Second))
}

func testRedkey() *redisv1.Redkey {
	return &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: redisv1.RedkeySpec{
			Primaries:          3,
			ReplicasPerPrimary: 1,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func makeConfigWithLabels(name string, seq int, phase string, primaries, replicas int32) *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{ClusterLabel: "test-cluster"},
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           seq,
			Primaries:          primaries,
			ReplicasPerPrimary: replicas,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
		},
		Status: redisv1.RedkeyConfigStatus{
			ConfigPhase: phase,
			Nodes:       map[string]*redisv1.RedisNode{},
		},
	}
}

// --- NewReconciler tests ---

func TestNewReconciler(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()
	r := NewReconciler(fakeClient, "my-cluster", "ns-1", config.NewRuntimeConfigWithReconcilerIntervals(10*time.Second, 3*time.Second, 7*time.Second))

	if r.clusterName != "my-cluster" {
		t.Fatalf("expected clusterName 'my-cluster', got '%s'", r.clusterName)
	}
	if r.namespace != "ns-1" {
		t.Fatalf("expected namespace 'ns-1', got '%s'", r.namespace)
	}
	if r.interval != 10*time.Second {
		t.Fatalf("expected interval 10s, got %v", r.interval)
	}
	if r.intervalOnError != 3*time.Second {
		t.Fatalf("expected intervalOnError 3s, got %v", r.intervalOnError)
	}
	if r.intervalOnWait != 7*time.Second {
		t.Fatalf("expected intervalOnWait 7s, got %v", r.intervalOnWait)
	}
	if r.client == nil {
		t.Fatal("expected non-nil client")
	}
	if r.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewReconciler_CreatesDefaultRuntimeConfigWhenNil(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()
	r := NewReconciler(fakeClient, "my-cluster", "ns-1", nil)

	if r.runtimeConfig == nil {
		t.Fatal("expected runtimeConfig to be initialized")
	}
	if r.interval != 30*time.Second {
		t.Fatalf("expected default interval 30s, got %v", r.interval)
	}
	if r.intervalOnError != 10*time.Second {
		t.Fatalf("expected default intervalOnError 10s, got %v", r.intervalOnError)
	}
	if r.intervalOnWait != 10*time.Second {
		t.Fatalf("expected default intervalOnWait 10s, got %v", r.intervalOnWait)
	}
}

func TestNextWaitDuration(t *testing.T) {
	r := newTestReconciler()

	if got := r.nextWaitDuration(reconcileImmediately, false); got != 0 {
		t.Fatalf("expected immediate wait duration 0, got %v", got)
	}
	if got := r.nextWaitDuration(reconcileAfterInterval, false); got != 5*time.Second {
		t.Fatalf("expected idle wait duration 5s, got %v", got)
	}
	if got := r.nextWaitDuration(reconcileAfterWaitInterval, false); got != 10*time.Second {
		t.Fatalf("expected wait-state duration 10s, got %v", got)
	}
	if got := r.nextWaitDuration(reconcileAfterInterval, true); got != 2*time.Second {
		t.Fatalf("expected error wait duration 2s, got %v", got)
	}
}

// --- listConfigs tests ---

func TestListConfigs_Empty(t *testing.T) {
	r := newTestReconciler()
	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(configs))
	}
}

func TestListConfigs_FiltersNamespace(t *testing.T) {
	// Config in the right namespace and cluster
	cfg1 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)
	// Config in a different namespace (should not appear)
	cfg2 := makeConfigWithLabels("cfg-other-ns", 2, redisv1.ConfigPhasePending, 3, 1)
	cfg2.Namespace = "other-namespace"

	r := newTestReconciler(cfg1, cfg2)
	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(configs))
	}
	if configs[0].Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %s", configs[0].Name)
	}
}

func TestListConfigs_FiltersClusterLabel(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-other-cluster", 2, redisv1.ConfigPhasePending, 3, 1)
	cfg2.Labels[ClusterLabel] = "other-cluster"

	r := newTestReconciler(cfg1, cfg2)
	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(configs))
	}
	if configs[0].Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %s", configs[0].Name)
	}
}

func TestListConfigs_Sorted(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1)
	cfg2 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)
	cfg3 := makeConfigWithLabels("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 3 {
		t.Fatalf("expected 3 configs, got %d", len(configs))
	}
	if configs[0].Spec.Sequence != 1 || configs[1].Spec.Sequence != 2 || configs[2].Spec.Sequence != 3 {
		t.Fatalf("expected sorted by sequence [1,2,3], got [%d,%d,%d]",
			configs[0].Spec.Sequence, configs[1].Spec.Sequence, configs[2].Spec.Sequence)
	}
}

func TestListConfigs_Unsorted(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1)
	cfg2 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg1, cfg2)

	configs, err := r.listConfigs(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(configs))
	}
	// Not asserting order — just that both are returned.
}

// --- initConfigPhase tests ---

func TestInitConfigPhase(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-init", 1, "", 3, 1)

	r := newTestReconciler(cfg)

	// Re-fetch the object so we have the right resource version
	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if err := r.initConfigPhase(context.Background(), &fetched); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the status was updated
	var updated redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
		t.Fatalf("failed to get updated config: %v", err)
	}
	if updated.Status.ConfigPhase != redisv1.ConfigPhasePending {
		t.Fatalf("expected ConfigPhase Pending, got '%s'", updated.Status.ConfigPhase)
	}
}

// --- setConfigPhaseInProgress tests ---

func TestSetConfigPhaseInProgress(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-ip", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg)

	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if err := r.setConfigPhaseInProgress(context.Background(), &fetched); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
		t.Fatalf("failed to get updated config: %v", err)
	}
	if updated.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected ConfigPhase InProgress, got '%s'", updated.Status.ConfigPhase)
	}
}

// --- selectConfig tests (full method with real fake client) ---

func TestSelectConfig_NoConfigs(t *testing.T) {
	r := newTestReconciler()

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected != nil {
		t.Fatalf("expected nil, got %s", selected.Name)
	}
	if prev != nil {
		t.Fatalf("expected nil prev, got %s", prev.Name)
	}
}

func TestSelectConfig_InitialisesEmptyPhase(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-nophase", 1, "", 3, 1)

	r := newTestReconciler(cfg)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil {
		t.Fatal("expected non-nil selected config")
	}
	if prev != nil {
		t.Fatalf("expected nil prev for single config, got %s", prev.Name)
	}

	// Verify the phase was initialised to Pending
	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhasePending {
		t.Fatalf("expected ConfigPhase Pending after init, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestSelectConfig_ReturnsFirstPending(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-pending", 2, redisv1.ConfigPhasePending, 5, 1)

	r := newTestReconciler(cfg1, cfg2)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil || selected.Name != "cfg-pending" {
		t.Fatalf("expected cfg-pending, got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}
}

func TestSelectConfig_WithSuperseding(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-skip", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg2.Spec.SkipIfSuperseded = true
	cfg3 := makeConfigWithLabels("cfg-target", 3, redisv1.ConfigPhasePending, 7, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil || selected.Name != "cfg-target" {
		t.Fatalf("expected cfg-target (superseding), got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}

	// Verify cfg-skip is now Superseded in the cluster
	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg2), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseSuperseded {
		t.Fatalf("expected cfg-skip to be Superseded, got '%s'", fetched.Status.ConfigPhase)
	}
}

// --- reconcile tests ---

func TestReconcile_NoConfigs(t *testing.T) {
	r := newTestReconciler()

	schedule, onError := r.reconcile(context.Background())
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected interval reconcile, got %v", schedule)
	}
	if onError {
		t.Fatal("expected no error")
	}
}

func TestReconcile_PendingConfig_TransitionsToInProgress(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-pending", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg)

	schedule, onError := r.reconcile(context.Background())
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}
	if onError {
		t.Fatal("expected no error")
	}

	// Verify it transitioned to InProgress
	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestReconcile_InProgressConfig_Resumes(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-ip", 1, redisv1.ConfigPhaseInProgress, 3, 1)

	r := newTestReconciler(cfg)

	schedule, onError := r.reconcile(context.Background())
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}
	if onError {
		t.Fatal("expected no error")
	}

	// Should still be InProgress (it resumes, doesn't re-transition)
	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestReconcile_AllApplied(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)

	r := newTestReconciler(cfg)

	schedule, onError := r.reconcile(context.Background())
	// When all configs are applied, SelectConfig returns the last one (which is Applied),
	// so it will try to re-process it (transition to InProgress).
	// This is expected behavior for the "re-apply last config" case.
	if onError {
		t.Fatal("expected no error")
	}
	_ = schedule // The exact value depends on the code path; we just ensure no error.
}

func TestCopyPreviousStatus_DeepCopiesReferenceFieldsPreservesTargetConfigPhaseAndPersists(t *testing.T) {
	target := makeConfigWithLabels("cfg-target", 2, redisv1.ConfigPhasePending, 3, 1)
	previous := makeConfigWithLabels("cfg-previous", 1, redisv1.ConfigPhaseApplied, 3, 1)
	lastUpdatedAt := metav1.Now()

	previous.Status = redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhaseApplied,
		Status:      redisv1.ClusterStatusReady,
		Nodes: map[string]*redisv1.RedisNode{
			"node-0": {Role: "primary", IP: "10.0.0.1"},
		},
		Conditions: []metav1.Condition{{
			Type:   "Ready",
			Status: metav1.ConditionTrue,
			Reason: "Stable",
		}},
		LastUpdatedAt: &lastUpdatedAt,
	}
	r := newTestReconciler(target, previous)

	var fetchedTarget redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(target), &fetchedTarget); err != nil {
		t.Fatalf("failed to get target config: %v", err)
	}

	var fetchedPrevious redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(previous), &fetchedPrevious); err != nil {
		t.Fatalf("failed to get previous config: %v", err)
	}

	if err := r.copyPreviousClusterStatus(context.Background(), &fetchedTarget, &fetchedPrevious); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fetchedTarget.Status.Status != previous.Status.Status {
		t.Fatalf("expected status %q, got %q", previous.Status.Status, fetchedTarget.Status.Status)
	}
	if fetchedTarget.Status.ConfigPhase != redisv1.ConfigPhasePending {
		t.Fatalf("expected target ConfigPhase to remain %q, got %q", redisv1.ConfigPhasePending, fetchedTarget.Status.ConfigPhase)
	}

	var persisted redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(target), &persisted); err != nil {
		t.Fatalf("failed to get persisted target config: %v", err)
	}
	if persisted.Status.Status != previous.Status.Status {
		t.Fatalf("expected persisted status %q, got %q", previous.Status.Status, persisted.Status.Status)
	}
	if persisted.Status.ConfigPhase != redisv1.ConfigPhasePending {
		t.Fatalf("expected persisted ConfigPhase %q, got %q", redisv1.ConfigPhasePending, persisted.Status.ConfigPhase)
	}

	fetchedTarget.Status.Nodes["node-0"].Role = "replica"
	fetchedTarget.Status.Conditions[0].Reason = "Changed"
	fetchedTarget.Status.LastUpdatedAt.Time = fetchedTarget.Status.LastUpdatedAt.Add(time.Second)

	if fetchedPrevious.Status.Nodes["node-0"].Role != "primary" {
		t.Fatalf("expected previous node role to remain 'primary', got %q", fetchedPrevious.Status.Nodes["node-0"].Role)
	}
	if fetchedPrevious.Status.Conditions[0].Reason != "Stable" {
		t.Fatalf("expected previous condition reason to remain 'Stable', got %q", fetchedPrevious.Status.Conditions[0].Reason)
	}
	if fetchedPrevious.Status.LastUpdatedAt.Time.Equal(fetchedTarget.Status.LastUpdatedAt.Time) {
		t.Fatal("expected LastUpdatedAt to be copied independently")
	}
}

// --- Start tests ---

func TestStart_CancelledContextStops(t *testing.T) {
	r := newTestReconciler()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := r.Start(ctx)
	if err != nil {
		t.Fatalf("expected nil error from cancelled Start, got %v", err)
	}
}

func TestStart_RunsReconciliationThenStops(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-start", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg)
	r.interval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := r.Start(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// After running, the config should have been transitioned to InProgress
	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress after Start, got '%s'", fetched.Status.ConfigPhase)
	}
}

// --- applySupersedingStatus tests ---

func TestApplySupersedingStatus_Success(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-skip", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg3 := makeConfigWithLabels("cfg-target", 3, redisv1.ConfigPhasePending, 7, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	// Fetch cfg2 to have the right resource version
	var fetchedCfg2 redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg2), &fetchedCfg2); err != nil {
		t.Fatalf("failed to get cfg2: %v", err)
	}

	sr := SupersedingResult{
		Selected:   cfg3,
		Superseded: []*redisv1.RedkeyConfig{&fetchedCfg2},
	}

	result := r.applySupersedingStatus(context.Background(), sr)
	if result.Name != "cfg-target" {
		t.Fatalf("expected cfg-target, got %s", result.Name)
	}

	// Verify cfg-skip was marked as Superseded
	var updated redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg2), &updated); err != nil {
		t.Fatalf("failed to get updated cfg2: %v", err)
	}
	if updated.Status.ConfigPhase != redisv1.ConfigPhaseSuperseded {
		t.Fatalf("expected Superseded, got '%s'", updated.Status.ConfigPhase)
	}
}

// --- applyRobinConfig tests ---

func TestApplyRobinConfig_UpdatesReconcilerIntervals(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-interval", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg.Spec.RobinConfig = &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds:        intPtr(20),
			IntervalOnErrorSeconds: intPtr(3),
			IntervalOnWaitSeconds:  intPtr(9),
		},
	}

	r := newTestReconciler(cfg)
	// Initial interval is 5s (from newTestReconciler).
	if r.interval != 5*time.Second {
		t.Fatalf("expected initial interval 5s, got %v", r.interval)
	}

	r.applyRobinConfig(cfg, nil)

	// RuntimeConfig should be updated.
	if r.runtimeConfig.ReconcilerInterval() != 20*time.Second {
		t.Fatalf("expected runtimeConfig interval 20s, got %v", r.runtimeConfig.ReconcilerInterval())
	}
	if r.runtimeConfig.ReconcilerIntervalOnError() != 3*time.Second {
		t.Fatalf("expected runtimeConfig intervalOnError 3s, got %v", r.runtimeConfig.ReconcilerIntervalOnError())
	}
	if r.runtimeConfig.ReconcilerIntervalOnWait() != 9*time.Second {
		t.Fatalf("expected runtimeConfig intervalOnWait 9s, got %v", r.runtimeConfig.ReconcilerIntervalOnWait())
	}
	// Reconciler's own interval field should be updated.
	if r.interval != 20*time.Second {
		t.Fatalf("expected r.interval 20s, got %v", r.interval)
	}
	if r.intervalOnError != 3*time.Second {
		t.Fatalf("expected r.intervalOnError 3s, got %v", r.intervalOnError)
	}
	if r.intervalOnWait != 9*time.Second {
		t.Fatalf("expected r.intervalOnWait 9s, got %v", r.intervalOnWait)
	}
}

func TestApplyRobinConfig_SetsAuthSecret(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-auth", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg.Spec.Auth = redisv1.RedisAuth{SecretName: "my-secret"}
	cfg.Spec.RobinConfig = &redisv1.RobinConfig{}

	r := newTestReconciler(cfg)
	r.applyRobinConfig(cfg, nil)

	if r.runtimeConfig.AuthSecret() != "my-secret" {
		t.Fatalf("expected auth secret 'my-secret', got %q", r.runtimeConfig.AuthSecret())
	}
}

func TestApplyRobinConfig_SetsAuthSecretEmpty(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-noauth", 1, redisv1.ConfigPhaseApplied, 3, 1)
	// No auth set — Auth.SecretName is "".
	cfg.Spec.RobinConfig = &redisv1.RobinConfig{}

	r := newTestReconciler(cfg)
	// Pre-set a secret to verify it gets cleared.
	r.runtimeConfig.SetAuthSecret("old-secret")

	r.applyRobinConfig(cfg, nil)

	if r.runtimeConfig.AuthSecret() != "" {
		t.Fatalf("expected empty auth secret, got %q", r.runtimeConfig.AuthSecret())
	}
}

func TestApplyRobinConfig_UsesPreviousWhenTargetIsPending(t *testing.T) {
	// Previous is Applied with specific robin config.
	prev := makeConfigWithLabels("cfg-prev", 1, redisv1.ConfigPhaseApplied, 3, 1)
	prev.Spec.RobinConfig = &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds:        intPtr(15),
			IntervalOnErrorSeconds: intPtr(4),
			IntervalOnWaitSeconds:  intPtr(6),
		},
	}
	prev.Spec.Auth = redisv1.RedisAuth{SecretName: "prev-secret"}

	// Target is Pending with different robin config (should NOT be used).
	target := makeConfigWithLabels("cfg-target", 2, redisv1.ConfigPhasePending, 5, 2)
	target.Spec.RobinConfig = &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds:        intPtr(99),
			IntervalOnErrorSeconds: intPtr(8),
			IntervalOnWaitSeconds:  intPtr(12),
		},
	}
	target.Spec.Auth = redisv1.RedisAuth{SecretName: "target-secret"}

	r := newTestReconciler(prev, target)
	r.applyRobinConfig(target, prev)

	// Should use previous config (15s, prev-secret).
	if r.runtimeConfig.ReconcilerInterval() != 15*time.Second {
		t.Fatalf("expected interval from previous (15s), got %v", r.runtimeConfig.ReconcilerInterval())
	}
	if r.runtimeConfig.ReconcilerIntervalOnError() != 4*time.Second {
		t.Fatalf("expected error interval from previous (4s), got %v", r.runtimeConfig.ReconcilerIntervalOnError())
	}
	if r.runtimeConfig.ReconcilerIntervalOnWait() != 6*time.Second {
		t.Fatalf("expected wait interval from previous (6s), got %v", r.runtimeConfig.ReconcilerIntervalOnWait())
	}
	if r.runtimeConfig.AuthSecret() != "prev-secret" {
		t.Fatalf("expected auth secret from previous, got %q", r.runtimeConfig.AuthSecret())
	}
}

func TestApplyRobinConfig_NilRobinConfigResetsIntervalsToBootstrap(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-norobin", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfgWithIntervals := makeConfigWithLabels("cfg-with-intervals", 2, redisv1.ConfigPhaseApplied, 3, 1)
	cfgWithIntervals.Spec.RobinConfig = &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds:        intPtr(21),
			IntervalOnErrorSeconds: intPtr(8),
			IntervalOnWaitSeconds:  intPtr(13),
		},
	}

	r := newTestReconciler(cfg, cfgWithIntervals)
	r.applyRobinConfig(cfgWithIntervals, nil)
	r.applyRobinConfig(cfg, nil)

	if r.interval != 5*time.Second {
		t.Fatalf("expected bootstrap interval 5s, got %v", r.interval)
	}
	if r.intervalOnError != 2*time.Second {
		t.Fatalf("expected bootstrap intervalOnError 2s, got %v", r.intervalOnError)
	}
	if r.intervalOnWait != 10*time.Second {
		t.Fatalf("expected bootstrap intervalOnWait 10s, got %v", r.intervalOnWait)
	}

	topo := r.runtimeConfig.AppliedTopology()
	if topo.Primaries != 3 || topo.ReplicasPerPrimary != 1 {
		t.Fatalf("expected topology 3/1, got %d/%d", topo.Primaries, topo.ReplicasPerPrimary)
	}
}

func TestApplyRobinConfig_UpdatesMetricsLabels(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-labels", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg.Spec.RobinConfig = &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			MetricsLabels: map[string]string{"env": "staging", "team": "platform"},
		},
	}

	r := newTestReconciler(cfg)
	r.applyRobinConfig(cfg, nil)

	got := r.runtimeConfig.MetricsLabels()
	expected := map[string]string{"env": "staging", "team": "platform"}
	if len(got) != len(expected) {
		t.Fatalf("expected %d labels, got %d: %v", len(expected), len(got), got)
	}
	for k, v := range expected {
		if got[k] != v {
			t.Fatalf("expected label %q=%q, got %q", k, v, got[k])
		}
	}
}

func TestApplyRobinConfig_MetricsLabelsUpdateOnConfigChange(t *testing.T) {
	// First config with initial labels.
	cfg1 := makeConfigWithLabels("cfg-labels-v1", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg1.Spec.RobinConfig = &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			MetricsLabels: map[string]string{"env": "staging"},
		},
	}

	r := newTestReconciler(cfg1)
	r.applyRobinConfig(cfg1, nil)

	got := r.runtimeConfig.MetricsLabels()
	if got["env"] != "staging" {
		t.Fatalf("expected env=staging, got %q", got["env"])
	}

	// Second config with updated labels.
	cfg2 := makeConfigWithLabels("cfg-labels-v2", 2, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2.Spec.RobinConfig = &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			MetricsLabels: map[string]string{"env": "prod", "region": "eu"},
		},
	}

	r.applyRobinConfig(cfg2, cfg1)

	got = r.runtimeConfig.MetricsLabels()
	if got["env"] != "prod" {
		t.Fatalf("expected env=prod after update, got %q", got["env"])
	}
	if got["region"] != "eu" {
		t.Fatalf("expected region=eu after update, got %q", got["region"])
	}
	// "team" should no longer be present (was never set in cfg2).
	if _, exists := got["team"]; exists {
		t.Fatalf("expected 'team' label to be absent, but it exists")
	}
}

func TestApplyRobinConfig_LogsMetricsAndClusterConfigChanges(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-runtime-v1", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg1.Spec.Auth = redisv1.RedisAuth{SecretName: "secret-v1"}
	cfg1.Spec.RobinConfig = &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			CollectionIntervalSeconds: intPtr(30),
			RedisInfoKeys:             []string{"keyspace_hits"},
			MetricsLabels:             map[string]string{"env": "staging"},
		},
		Cluster: &redisv1.RobinConfigCluster{
			ConnectionMaxRetries:     intPtr(3),
			ConnectionBackOffSeconds: intPtr(5),
		},
	}

	cfg2 := makeConfigWithLabels("cfg-runtime-v2", 2, redisv1.ConfigPhaseApplied, 5, 2)
	cfg2.Spec.Auth = redisv1.RedisAuth{SecretName: "secret-v2"}
	cfg2.Spec.RobinConfig = &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			CollectionIntervalSeconds: intPtr(45),
			RedisInfoKeys:             []string{"connected_clients", "used_memory_rss"},
			MetricsLabels:             map[string]string{"env": "prod", "region": "eu"},
		},
		Cluster: &redisv1.RobinConfigCluster{
			ConnectionMaxRetries:     intPtr(7),
			ConnectionBackOffSeconds: intPtr(9),
		},
	}

	r := newTestReconciler(cfg1, cfg2)
	r.applyRobinConfig(cfg1, nil)

	var logBuffer bytes.Buffer
	r.logger = slog.New(slog.NewTextHandler(&logBuffer, nil))

	r.applyRobinConfig(cfg2, cfg1)

	logOutput := logBuffer.String()
	if !strings.Contains(logOutput, "Metrics configuration changed") {
		t.Fatalf("expected metrics change log, got %q", logOutput)
	}
	if !strings.Contains(logOutput, "Cluster configuration changed") {
		t.Fatalf("expected cluster change log, got %q", logOutput)
	}
	if !strings.Contains(logOutput, "config=cfg-runtime-v2") {
		t.Fatalf("expected effective config name in log, got %q", logOutput)
	}
}

// --- selectConfig with multiple chained superseded configs ---

func TestSelectConfig_MultipleChainedSuperseded(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-skip-1", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg2.Spec.SkipIfSuperseded = true
	cfg3 := makeConfigWithLabels("cfg-skip-2", 3, redisv1.ConfigPhasePending, 6, 1)
	cfg3.Spec.SkipIfSuperseded = true
	cfg4 := makeConfigWithLabels("cfg-skip-3", 4, redisv1.ConfigPhasePending, 7, 1)
	cfg4.Spec.SkipIfSuperseded = true
	cfg5 := makeConfigWithLabels("cfg-final", 5, redisv1.ConfigPhasePending, 9, 2)

	r := newTestReconciler(cfg1, cfg2, cfg3, cfg4, cfg5)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil || selected.Name != "cfg-final" {
		t.Fatalf("expected cfg-final, got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}

	// All intermediate configs should be Superseded.
	for _, name := range []string{"cfg-skip-1", "cfg-skip-2", "cfg-skip-3"} {
		var fetched redisv1.RedkeyConfig
		key := client.ObjectKey{Name: name, Namespace: "default"}
		if err := r.client.Get(context.Background(), key, &fetched); err != nil {
			t.Fatalf("failed to get %s: %v", name, err)
		}
		if fetched.Status.ConfigPhase != redisv1.ConfigPhaseSuperseded {
			t.Fatalf("expected %s to be Superseded, got '%s'", name, fetched.Status.ConfigPhase)
		}
	}
}

func TestSelectConfig_SkipIfSupersededButIsLastPending(t *testing.T) {
	// When a config has skipIfSuperseded=true but it's the last pending, it should NOT be skipped.
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-last-skip", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg2.Spec.SkipIfSuperseded = true

	r := newTestReconciler(cfg1, cfg2)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Since cfg-last-skip is the only pending and there's nothing superseding it, it should be selected.
	if selected == nil || selected.Name != "cfg-last-skip" {
		t.Fatalf("expected cfg-last-skip (only pending), got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}
}
