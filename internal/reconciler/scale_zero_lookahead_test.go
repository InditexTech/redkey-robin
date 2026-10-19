// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// --- ApplyScaleToZeroLookahead (pure) ---

func TestApplyScaleToZeroLookahead(t *testing.T) {
	const (
		applied    = redisv1.ConfigPhaseApplied
		pending    = redisv1.ConfigPhasePending
		inProgress = redisv1.ConfigPhaseInProgress
		superseded = redisv1.ConfigPhaseSuperseded
	)

	tests := []struct {
		name           string
		configs        []redisv1.RedkeyConfig
		selectedIdx    int
		wantSelected   string
		wantSuperseded []string
	}{
		{
			name: "no pending scale-to-zero config",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 5, 1, false),
				makeFullConfig("cfg-3", 3, pending, 7, 1, false),
			},
			selectedIdx:  1,
			wantSelected: "cfg-2",
		},
		{
			name: "pending selected followed by scale-to-zero",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 5, 1, false),
				makeFullConfig("cfg-3", 3, pending, 0, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-3",
			wantSuperseded: []string{"cfg-2"},
		},
		{
			name: "wedged in-progress selected followed by scale-to-zero",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, inProgress, 3, 1, false),
				makeFullConfig("cfg-3", 3, pending, 0, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-3",
			wantSuperseded: []string{"cfg-2"},
		},
		{
			name: "multiple intermediates are all superseded",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, inProgress, 3, 1, false),
				makeFullConfig("cfg-3", 3, pending, 5, 1, false),
				makeFullConfig("cfg-4", 4, "", 5, 2, false),
				makeFullConfig("cfg-5", 5, pending, 0, 2, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-5",
			wantSuperseded: []string{"cfg-2", "cfg-3", "cfg-4"},
		},
		{
			name: "multiple scale-to-zero configs: the last one wins",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 5, 1, false),
				makeFullConfig("cfg-3", 3, pending, 0, 1, false),
				makeFullConfig("cfg-4", 4, pending, 3, 1, false),
				makeFullConfig("cfg-5", 5, pending, 0, 1, false),
				makeFullConfig("cfg-6", 6, pending, 5, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-5",
			wantSuperseded: []string{"cfg-2", "cfg-3", "cfg-4"},
		},
		{
			name: "configs after the scale-to-zero target are left untouched",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 5, 1, false),
				makeFullConfig("cfg-3", 3, pending, 0, 1, false),
				makeFullConfig("cfg-4", 4, pending, 3, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-3",
			wantSuperseded: []string{"cfg-2"},
		},
		{
			name: "scale-to-zero target with empty phase",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 5, 1, false),
				makeFullConfig("cfg-3", 3, "", 0, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-3",
			wantSuperseded: []string{"cfg-2"},
		},
		{
			name: "in-progress scale-to-zero selected is not superseded",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, inProgress, 0, 1, false),
				makeFullConfig("cfg-3", 3, pending, 5, 1, false),
				makeFullConfig("cfg-4", 4, pending, 0, 1, false),
			},
			selectedIdx:  1,
			wantSelected: "cfg-2",
		},
		{
			name: "pending scale-to-zero selected jumps to a later scale-to-zero",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 0, 1, false),
				makeFullConfig("cfg-3", 3, pending, 5, 1, false),
				makeFullConfig("cfg-4", 4, pending, 0, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-4",
			wantSuperseded: []string{"cfg-2", "cfg-3"},
		},
		{
			name: "applied selected (nothing pending) is a no-op",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
			},
			selectedIdx:  0,
			wantSelected: "cfg-1",
		},
		{
			name: "already superseded configs in between are not re-added",
			configs: []redisv1.RedkeyConfig{
				makeFullConfig("cfg-1", 1, applied, 3, 1, false),
				makeFullConfig("cfg-2", 2, pending, 5, 1, false),
				makeFullConfig("cfg-3", 3, superseded, 7, 1, false),
				makeFullConfig("cfg-4", 4, pending, 0, 1, false),
			},
			selectedIdx:    1,
			wantSelected:   "cfg-4",
			wantSuperseded: []string{"cfg-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyScaleToZeroLookahead(tt.configs, &tt.configs[tt.selectedIdx])
			if result.Selected == nil || result.Selected.Name != tt.wantSelected {
				t.Fatalf("expected selected %s, got %v", tt.wantSelected, result.Selected)
			}
			got := namesOf(result.Superseded)
			if len(got) == 0 && len(tt.wantSuperseded) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.wantSuperseded) {
				t.Fatalf("expected superseded %v, got %v", tt.wantSuperseded, got)
			}
		})
	}
}

func TestApplyScaleToZeroLookahead_IgnoresSkipIfSupersededAndNonTopologyChanges(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 3, 1, false),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 0, 1, false),
	}
	configs[1].Spec.Image = "redis:does-not-exist"
	configs[2].Spec.Version = "8.0"

	result := ApplyScaleToZeroLookahead(configs, &configs[1])
	if result.Selected.Name != "cfg-3" {
		t.Fatalf("expected cfg-3, got %s", result.Selected.Name)
	}
	if !reflect.DeepEqual(namesOf(result.Superseded), []string{"cfg-2"}) {
		t.Fatalf("expected [cfg-2] superseded, got %v", namesOf(result.Superseded))
	}
}

func TestApplyScaleToZeroLookahead_NilSelected(t *testing.T) {
	result := ApplyScaleToZeroLookahead(nil, nil)
	if result.Selected != nil || len(result.Superseded) != 0 {
		t.Fatalf("expected empty result, got %+v", result)
	}
}

// --- selectConfig with scale-to-zero lookahead ---

func TestSelectConfig_ScaleToZeroLookahead_SupersedesInProgressAndPending(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-wedged", 2, redisv1.ConfigPhaseInProgress, 3, 1)
	cfg2.Spec.Image = "redis:does-not-exist"
	cfg3 := makeConfigWithLabels("cfg-pending", 3, redisv1.ConfigPhasePending, 5, 1)
	cfg4 := makeConfigWithLabels("cfg-zero", 4, redisv1.ConfigPhasePending, 0, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3, cfg4)

	prev, selected, force, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !force {
		t.Fatal("expected forceScaleToZero to be true")
	}
	if selected == nil || selected.Name != "cfg-zero" {
		t.Fatalf("expected cfg-zero, got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}

	for _, name := range []string{"cfg-wedged", "cfg-pending"} {
		assertConfigPhase(t, r.client, name, redisv1.ConfigPhaseSuperseded)
	}
	assertConfigPhase(t, r.client, "cfg-zero", redisv1.ConfigPhasePending)
}

func TestSelectConfig_ScaleToZeroLookahead_SkipsApplySuperseding(t *testing.T) {
	// cfg-zero has skipIfSuperseded=true and a later topology-only config exists:
	// after the jump, ApplySuperseding must not supersede the scale-to-zero config.
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-pending", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg3 := makeConfigWithLabels("cfg-zero", 3, redisv1.ConfigPhasePending, 0, 1)
	cfg3.Spec.SkipIfSuperseded = true
	cfg4 := makeConfigWithLabels("cfg-after", 4, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3, cfg4)

	_, selected, force, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !force || selected == nil || selected.Name != "cfg-zero" {
		t.Fatalf("expected forced cfg-zero, got %v (force=%v)", selected, force)
	}
	assertConfigPhase(t, r.client, "cfg-zero", redisv1.ConfigPhasePending)
	assertConfigPhase(t, r.client, "cfg-after", redisv1.ConfigPhasePending)
}

func TestSelectConfig_NoScaleToZero_DoesNotForce(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-ip", 2, redisv1.ConfigPhaseInProgress, 5, 1)
	cfg3 := makeConfigWithLabels("cfg-pending", 3, redisv1.ConfigPhasePending, 7, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	_, selected, force, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if force {
		t.Fatal("expected forceScaleToZero to be false")
	}
	if selected == nil || selected.Name != "cfg-ip" {
		t.Fatalf("expected cfg-ip, got %v", selected)
	}
	assertConfigPhase(t, r.client, "cfg-ip", redisv1.ConfigPhaseInProgress)
}

func TestSelectConfig_ScaleToZeroLookahead_WriteFailureAborts(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-wedged", 2, redisv1.ConfigPhaseInProgress, 3, 1)
	cfg3 := makeConfigWithLabels("cfg-pending", 3, redisv1.ConfigPhasePending, 5, 1)
	cfg4 := makeConfigWithLabels("cfg-zero", 4, redisv1.ConfigPhasePending, 0, 1)

	// Fail the status update for the second config to supersede.
	failOn := "cfg-pending"
	r := newTestReconcilerWithInterceptor(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if obj.GetName() == failOn {
				return errors.New("injected status update failure")
			}
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}, cfg1, cfg2, cfg3, cfg4)

	prev, selected, force, err := r.selectConfig(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if prev != nil || selected != nil || force {
		t.Fatalf("expected no selection on error, got prev=%v selected=%v force=%v", prev, selected, force)
	}
	// Partial progress is persisted; intermediate config is never started.
	assertConfigPhase(t, r.client, "cfg-wedged", redisv1.ConfigPhaseSuperseded)
	assertConfigPhase(t, r.client, "cfg-pending", redisv1.ConfigPhasePending)
	assertConfigPhase(t, r.client, "cfg-zero", redisv1.ConfigPhasePending)

	// Next cycle (write works again) resumes the lookahead from the pending config.
	failOn = ""
	_, selected, force, err = r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !force || selected == nil || selected.Name != "cfg-zero" {
		t.Fatalf("expected forced cfg-zero, got %v (force=%v)", selected, force)
	}
	assertConfigPhase(t, r.client, "cfg-pending", redisv1.ConfigPhaseSuperseded)
}

// --- reconcile with scale-to-zero lookahead ---

func TestReconcile_ScaleToZeroLookahead(t *testing.T) {
	tests := []struct {
		name      string
		configs   func() []client.Object
		zeroName  string
		superseds []string
	}{
		{
			name: "wedged upgrade with previous applied config",
			configs: func() []client.Object {
				cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
				cfg1.Status.Status = redisv1.ClusterStatusReady
				cfg2 := makeConfigWithLabels("cfg-wedged", 2, redisv1.ConfigPhaseInProgress, 3, 1)
				cfg2.Spec.Image = "redis:does-not-exist"
				cfg2.Status.Status = redisv1.ClusterStatusUpgrading
				cfg3 := makeConfigWithLabels("cfg-zero", 3, redisv1.ConfigPhasePending, 0, 1)
				return []client.Object{cfg1, cfg2, cfg3}
			},
			zeroName:  "cfg-zero",
			superseds: []string{"cfg-wedged"},
		},
		{
			name: "wedged first config without previous applied config",
			configs: func() []client.Object {
				cfg1 := makeConfigWithLabels("cfg-wedged", 1, redisv1.ConfigPhaseInProgress, 3, 1)
				cfg1.Spec.Image = "redis:does-not-exist"
				cfg1.Status.Status = redisv1.ClusterStatusInitializing
				cfg2 := makeConfigWithLabels("cfg-zero", 2, redisv1.ConfigPhasePending, 0, 1)
				return []client.Object{cfg1, cfg2}
			},
			zeroName:  "cfg-zero",
			superseds: []string{"cfg-wedged"},
		},
		{
			name: "previous applied config already at zero primaries",
			configs: func() []client.Object {
				cfg1 := makeConfigWithLabels("cfg-applied-zero", 1, redisv1.ConfigPhaseApplied, 0, 1)
				cfg1.Status.Status = redisv1.ClusterStatusReady
				cfg2 := makeConfigWithLabels("cfg-wedged", 2, redisv1.ConfigPhaseInProgress, 3, 1)
				cfg2.Spec.Image = "redis:does-not-exist"
				cfg2.Status.Status = redisv1.ClusterStatusInitializing
				cfg3 := makeConfigWithLabels("cfg-zero", 3, redisv1.ConfigPhasePending, 0, 1)
				return []client.Object{cfg1, cfg2, cfg3}
			},
			zeroName:  "cfg-zero",
			superseds: []string{"cfg-wedged"},
		},
		{
			name: "standalone wedged upgrade",
			configs: func() []client.Object {
				cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 1, 0)
				cfg1.Spec.Mode = redisv1.ModeStandalone
				cfg1.Status.Status = redisv1.ClusterStatusReady
				cfg2 := makeConfigWithLabels("cfg-wedged", 2, redisv1.ConfigPhaseInProgress, 1, 0)
				cfg2.Spec.Mode = redisv1.ModeStandalone
				cfg2.Spec.Image = "redis:does-not-exist"
				cfg2.Status.Status = redisv1.ClusterStatusUpgrading
				cfg3 := makeConfigWithLabels("cfg-zero", 3, redisv1.ConfigPhasePending, 0, 0)
				cfg3.Spec.Mode = redisv1.ModeStandalone
				return []client.Object{cfg1, cfg2, cfg3}
			},
			zeroName:  "cfg-zero",
			superseds: []string{"cfg-wedged"},
		},
		{
			name: "pending chain with non-topology changes",
			configs: func() []client.Object {
				cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
				cfg1.Status.Status = redisv1.ClusterStatusReady
				cfg2 := makeConfigWithLabels("cfg-scale", 2, redisv1.ConfigPhasePending, 5, 1)
				cfg3 := makeConfigWithLabels("cfg-image", 3, redisv1.ConfigPhasePending, 5, 1)
				cfg3.Spec.Image = "redis:8"
				cfg4 := makeConfigWithLabels("cfg-zero", 4, redisv1.ConfigPhasePending, 0, 1)
				return []client.Object{cfg1, cfg2, cfg3, cfg4}
			},
			zeroName:  "cfg-zero",
			superseds: []string{"cfg-scale", "cfg-image"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append(tt.configs(), clusterObjects()...)
			r := newTestReconciler(objs...)

			_, onError := r.reconcile(context.Background())
			if onError {
				t.Fatal("expected no error")
			}

			for _, name := range tt.superseds {
				assertConfigPhase(t, r.client, name, redisv1.ConfigPhaseSuperseded)
			}

			var zero redisv1.RedkeyConfig
			if err := r.client.Get(context.Background(), types.NamespacedName{Name: tt.zeroName, Namespace: "default"}, &zero); err != nil {
				t.Fatalf("failed to get %s: %v", tt.zeroName, err)
			}
			if zero.Status.ConfigPhase != redisv1.ConfigPhaseApplied {
				t.Fatalf("expected %s Applied, got %q", tt.zeroName, zero.Status.ConfigPhase)
			}
			if zero.Status.Status != redisv1.ClusterStatusReady {
				t.Fatalf("expected %s status Ready, got %q", tt.zeroName, zero.Status.Status)
			}
			if len(zero.Status.Nodes) != 0 {
				t.Fatalf("expected no nodes, got %v", zero.Status.Nodes)
			}

			assertClusterObjectsDeleted(t, r.client)
		})
	}
}

func TestStartScaleToZero_WritesInProgressAndScalingToZeroAtomically(t *testing.T) {
	previous := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	previous.Status.Status = redisv1.ClusterStatusReady
	previous.Status.Substatus = redisv1.RedkeySubstatus{Status: redisv1.SubstatusDeletingResources}
	previous.Status.Nodes = map[string]*redisv1.RedisNode{"node-0": {Role: "primary"}}
	target := makeConfigWithLabels("cfg-zero", 2, redisv1.ConfigPhasePending, 0, 1)

	updates := 0
	r := newTestReconcilerWithInterceptor(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			updates++
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}, previous, target)

	var fetched redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(target), &fetched); err != nil {
		t.Fatalf("failed to get target: %v", err)
	}
	if err := r.startScaleToZero(context.Background(), &fetched, previous); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updates != 1 {
		t.Fatalf("expected a single status update, got %d", updates)
	}

	var persisted redisv1.RedkeyConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(target), &persisted); err != nil {
		t.Fatalf("failed to get persisted target: %v", err)
	}
	if persisted.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress, got %q", persisted.Status.ConfigPhase)
	}
	if persisted.Status.Status != redisv1.ClusterStatusScalingToZero {
		t.Fatalf("expected ScalingToZero, got %q", persisted.Status.Status)
	}
	if persisted.Status.Substatus.Status != "" {
		t.Fatalf("expected empty substatus, got %q", persisted.Status.Substatus.Status)
	}
	if _, ok := persisted.Status.Nodes["node-0"]; !ok {
		t.Fatalf("expected nodes copied from previous config, got %v", persisted.Status.Nodes)
	}
	// The previous config must not be mutated.
	if previous.Status.ConfigPhase != redisv1.ConfigPhaseApplied || previous.Status.Status != redisv1.ClusterStatusReady {
		t.Fatalf("previous config mutated: %+v", previous.Status)
	}
}

func TestReconcile_ScaleToZeroLookahead_ResumesInProgressScalingToZero(t *testing.T) {
	// A crash after startScaleToZero leaves the zero config InProgress/ScalingToZero:
	// the next cycle must resume the deletion, not supersede it.
	cfg1 := makeConfigWithLabels("cfg-wedged", 1, redisv1.ConfigPhaseSuperseded, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-zero", 2, redisv1.ConfigPhaseInProgress, 0, 1)
	cfg2.Status.Status = redisv1.ClusterStatusScalingToZero

	objs := append([]client.Object{cfg1, cfg2}, clusterObjects()...)
	r := newTestReconciler(objs...)

	if _, onError := r.reconcile(context.Background()); onError {
		t.Fatal("expected no error")
	}
	assertConfigPhase(t, r.client, "cfg-zero", redisv1.ConfigPhaseApplied)
	assertClusterObjectsDeleted(t, r.client)
}

// --- helpers ---

func newTestReconcilerWithInterceptor(funcs interceptor.Funcs, objs ...client.Object) *Reconciler {
	objs = append(objs, testRedkey())
	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(objs...).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		WithInterceptorFuncs(funcs).
		Build()
	return NewReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfigWithReconcilerIntervals(5*time.Second, 2*time.Second, 10*time.Second))
}

func clusterObjects() []client.Object {
	return []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-pdb", Namespace: "default"}},
	}
}

func assertClusterObjectsDeleted(t *testing.T, c client.Client) {
	t.Helper()
	objs := map[string]client.Object{
		"StatefulSet": &appsv1.StatefulSet{},
		"Service":     &corev1.Service{},
		"ConfigMap":   &corev1.ConfigMap{},
	}
	for kind, obj := range objs {
		if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, obj); err == nil {
			t.Fatalf("expected %s to be deleted", kind)
		}
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-pdb", Namespace: "default"}, &policyv1.PodDisruptionBudget{}); err == nil {
		t.Fatal("expected PDB to be deleted")
	}
}

func assertConfigPhase(t *testing.T, c client.Client, name, want string) {
	t.Helper()
	var fetched redisv1.RedkeyConfig
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get %s: %v", name, err)
	}
	if fetched.Status.ConfigPhase != want {
		t.Fatalf("expected %s ConfigPhase %q, got %q", name, want, fetched.Status.ConfigPhase)
	}
}
