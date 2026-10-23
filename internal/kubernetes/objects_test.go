// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

var testScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = redisv1.AddToScheme(testScheme)
}

func testOwner() *redisv1.Redkey {
	return &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid-1234",
		},
		Spec: redisv1.RedkeySpec{
			Primaries:          3,
			ReplicasPerPrimary: 1,
			Ephemeral:          true,
			Image:              "redis:7",
			AccessModes:        []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
}

func testConfig() *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config-1",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           1,
			Primaries:          3,
			ReplicasPerPrimary: 1,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func TestBuildConfigMap_Basic(t *testing.T) {
	config := testConfig()
	cm := buildConfigMap("test-cluster", "default", config, "")

	if cm.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", cm.Name)
	}
	if cm.Namespace != "default" {
		t.Fatalf("expected namespace 'default', got '%s'", cm.Namespace)
	}
	if _, ok := cm.Data["redis.conf"]; !ok {
		t.Fatal("expected redis.conf key in ConfigMap data")
	}
}

func TestBuildConfigMap_WithPassword(t *testing.T) {
	config := testConfig()
	cm := buildConfigMap("test-cluster", "default", config, "secret123")

	conf := cm.Data["redis.conf"]
	if conf == "" {
		t.Fatal("expected non-empty redis.conf")
	}
	// Should contain requirepass and masterauth
	if !contains(conf, "requirepass secret123") {
		t.Error("expected requirepass in redis.conf")
	}
	if !contains(conf, "masterauth secret123") {
		t.Error("expected masterauth in redis.conf")
	}
}

func TestBuildConfigMap_Ephemeral(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = true
	cm := buildConfigMap("test-cluster", "default", config, "")

	conf := cm.Data["redis.conf"]
	if got := countDirective(conf, "appendonly"); got != 1 || !contains(conf, "appendonly no\n") {
		t.Errorf("expected exactly one 'appendonly no' for ephemeral cluster, got %d in:\n%s", got, conf)
	}
	if got := countDirective(conf, "save"); got != 1 || !contains(conf, "save \"\"\n") {
		t.Errorf("expected exactly one 'save \"\"' for ephemeral cluster, got %d in:\n%s", got, conf)
	}
}

func TestBuildConfigMap_NonEphemeral(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = false
	cm := buildConfigMap("test-cluster", "default", config, "")

	conf := cm.Data["redis.conf"]
	if got := countDirective(conf, "appendonly"); got != 0 {
		t.Errorf("expected no 'appendonly' directive for non-ephemeral cluster, got %d in:\n%s", got, conf)
	}
	if got := countDirective(conf, "save"); got != 0 {
		t.Errorf("expected no 'save' directive for non-ephemeral cluster, got %d in:\n%s", got, conf)
	}
	if !contains(conf, "dir /data\n") || !contains(conf, "cluster-config-file /data/nodes.conf\n") {
		t.Errorf("expected /data as data dir for non-ephemeral cluster, got:\n%s", conf)
	}
}

func TestBuildService(t *testing.T) {
	svc := buildService("test-cluster", "default", testConfig())

	if svc.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", svc.Name)
	}
	if svc.Spec.ClusterIP != "None" {
		t.Fatalf("expected headless service (ClusterIP=None), got '%s'", svc.Spec.ClusterIP)
	}
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("expected 2 ports, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != RedisPort {
		t.Errorf("expected port %d, got %d", RedisPort, svc.Spec.Ports[0].Port)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("expected PublishNotReadyAddresses=true")
	}
}

func TestBuildStatefulSet_Ephemeral(t *testing.T) {
	config := testConfig()
	config.Spec.Primaries = 3
	config.Spec.ReplicasPerPrimary = 1
	config.Spec.Image = "redis:7.2"
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	expectedReplicas := int32(6) // 3 + 3*1
	if *sts.Spec.Replicas != expectedReplicas {
		t.Fatalf("expected %d replicas, got %d", expectedReplicas, *sts.Spec.Replicas)
	}
	if sts.Spec.Template.Spec.Containers[0].Image != "redis:7.2" {
		t.Fatalf("expected image 'redis:7.2', got '%s'", sts.Spec.Template.Spec.Containers[0].Image)
	}
	if sts.Spec.ServiceName != "test-cluster" {
		t.Fatalf("expected serviceName 'test-cluster', got '%s'", sts.Spec.ServiceName)
	}
	// Ephemeral: no VolumeClaimTemplates
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("expected no VolumeClaimTemplates for ephemeral, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		t.Fatal("expected no retention policy for ephemeral cluster")
	}
}

func TestBuildStatefulSet_WithStorage(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = false
	config.Spec.Storage = "10Gi"
	config.Spec.StorageClassName = "fast-ssd"
	deletePVC := true
	config.Spec.DeletePVC = &deletePVC

	owner := testOwner()
	owner.Spec.Ephemeral = false
	owner.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected 1 VolumeClaimTemplate, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	pvc := sts.Spec.VolumeClaimTemplates[0]
	if pvc.Name != "data" {
		t.Fatalf("expected PVC name 'data', got '%s'", pvc.Name)
	}
	expectedStorage := resource.MustParse("10Gi")
	actualStorage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !actualStorage.Equal(expectedStorage) {
		t.Fatalf("expected storage '%s', got '%s'", expectedStorage.String(), actualStorage.String())
	}
	if *pvc.Spec.StorageClassName != "fast-ssd" {
		t.Fatalf("expected storageClassName 'fast-ssd', got '%s'", *pvc.Spec.StorageClassName)
	}
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatalf("expected AccessModes [ReadWriteOnce], got %v", pvc.Spec.AccessModes)
	}

	// Check retention policy
	if sts.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		t.Fatal("expected retention policy")
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenDeleted=Delete, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenScaled=Delete, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	}

	// Check data volume mount
	found := false
	for _, vm := range sts.Spec.Template.Spec.Containers[0].VolumeMounts {
		if vm.Name == "data" && vm.MountPath == "/data" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected data volume mount at /data")
	}
}

func TestBuildStatefulSet_RetainPVC(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = false
	config.Spec.Storage = "5Gi"
	deletePVC := false
	config.Spec.DeletePVC = &deletePVC

	owner := testOwner()
	owner.Spec.Ephemeral = false

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenDeleted=Retain, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenScaled=Retain, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	}
}

func TestBuildStatefulSet_DefaultImage(t *testing.T) {
	config := testConfig()
	config.Spec.Image = ""
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if sts.Spec.Template.Spec.Containers[0].Image != defaultRedisImage {
		t.Fatalf("expected default image '%s', got '%s'", defaultRedisImage, sts.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestBuildStatefulSet_WithLabels(t *testing.T) {
	config := testConfig()
	customLabels := map[string]string{
		"app.kubernetes.io/team": "platform",
		"environment":            "test",
	}
	config.Spec.Labels = &customLabels
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Verify custom labels on StatefulSet ObjectMeta
	if sts.Labels["app.kubernetes.io/team"] != "platform" {
		t.Errorf("expected StatefulSet label 'app.kubernetes.io/team'='platform', got '%s'", sts.Labels["app.kubernetes.io/team"])
	}
	if sts.Labels["environment"] != "test" {
		t.Errorf("expected StatefulSet label 'environment'='test', got '%s'", sts.Labels["environment"])
	}

	// Verify custom labels on pod template
	podLabels := sts.Spec.Template.Labels
	if podLabels["app.kubernetes.io/team"] != "platform" {
		t.Errorf("expected pod template label 'app.kubernetes.io/team'='platform', got '%s'", podLabels["app.kubernetes.io/team"])
	}
	if podLabels["environment"] != "test" {
		t.Errorf("expected pod template label 'environment'='test', got '%s'", podLabels["environment"])
	}

	// Verify base labels are still present
	if podLabels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base ClusterLabel, got '%s'", podLabels[ClusterLabel])
	}
	if podLabels[ComponentLabel] != ComponentRedis {
		t.Errorf("expected base ComponentLabel, got '%s'", podLabels[ComponentLabel])
	}

	// Verify selector does NOT include custom labels (immutable after creation)
	selectorLabels := sts.Spec.Selector.MatchLabels
	if _, exists := selectorLabels["app.kubernetes.io/team"]; exists {
		t.Error("selector should not include custom labels")
	}
}

func TestBuildStatefulSet_WithAnnotations(t *testing.T) {
	config := testConfig()
	customAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "9121",
	}
	config.Spec.Annotations = &customAnnotations
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Verify custom annotations on StatefulSet ObjectMeta
	if sts.Annotations["prometheus.io/scrape"] != "true" {
		t.Errorf("expected StatefulSet annotation 'prometheus.io/scrape'='true', got '%s'", sts.Annotations["prometheus.io/scrape"])
	}
	if sts.Annotations["prometheus.io/port"] != "9121" {
		t.Errorf("expected StatefulSet annotation 'prometheus.io/port'='9121', got '%s'", sts.Annotations["prometheus.io/port"])
	}

	// Verify custom annotations on pod template
	podAnnotations := sts.Spec.Template.Annotations
	if podAnnotations["prometheus.io/scrape"] != "true" {
		t.Errorf("expected pod template annotation 'prometheus.io/scrape'='true', got '%s'", podAnnotations["prometheus.io/scrape"])
	}
	if podAnnotations["prometheus.io/port"] != "9121" {
		t.Errorf("expected pod template annotation 'prometheus.io/port'='9121', got '%s'", podAnnotations["prometheus.io/port"])
	}
}

func TestBuildStatefulSet_WithLabelsAndAnnotations(t *testing.T) {
	config := testConfig()
	labels := map[string]string{"team": "infra"}
	annotations := map[string]string{"note": "test"}
	config.Spec.Labels = &labels
	config.Spec.Annotations = &annotations
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Labels
	if sts.Spec.Template.Labels["team"] != "infra" {
		t.Errorf("expected pod template label 'team'='infra', got '%s'", sts.Spec.Template.Labels["team"])
	}
	// Annotations
	if sts.Spec.Template.Annotations["note"] != "test" {
		t.Errorf("expected pod template annotation 'note'='test', got '%s'", sts.Spec.Template.Annotations["note"])
	}
}

// TestBuildStatefulSet_BaseLabelsWin verifies that internal base labels always
// win over colliding spec.labels on both the StatefulSet metadata and the pods.
func TestBuildStatefulSet_BaseLabelsWin(t *testing.T) {
	config := testConfig()
	labels := map[string]string{
		ClusterLabel:   "hijacked",
		ComponentLabel: "hijacked",
		"team":         "infra",
	}
	config.Spec.Labels = &labels
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Base labels win on collision at the pod level.
	if sts.Spec.Template.Labels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base ClusterLabel to win, got '%s'", sts.Spec.Template.Labels[ClusterLabel])
	}
	if sts.Spec.Template.Labels[ComponentLabel] != ComponentRedis {
		t.Errorf("expected base ComponentLabel to win, got '%s'", sts.Spec.Template.Labels[ComponentLabel])
	}
	// Non-colliding spec label is still applied.
	if sts.Spec.Template.Labels["team"] != "infra" {
		t.Errorf("expected spec label 'team'='infra', got '%s'", sts.Spec.Template.Labels["team"])
	}
}

// TestBuildConfigMap_PropagatesLabelsAnnotations verifies spec.labels /
// spec.annotations are applied to the ConfigMap with base labels winning.
func TestBuildConfigMap_PropagatesLabelsAnnotations(t *testing.T) {
	config := testConfig()
	labels := map[string]string{"team": "infra", ClusterLabel: "hijacked"}
	annotations := map[string]string{"note": "test"}
	config.Spec.Labels = &labels
	config.Spec.Annotations = &annotations

	cm := buildConfigMap("test-cluster", "default", config, "")

	if cm.Labels["team"] != "infra" {
		t.Errorf("expected ConfigMap label 'team'='infra', got '%s'", cm.Labels["team"])
	}
	if cm.Labels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base ClusterLabel to win on ConfigMap, got '%s'", cm.Labels[ClusterLabel])
	}
	if cm.Annotations["note"] != "test" {
		t.Errorf("expected ConfigMap annotation 'note'='test', got '%s'", cm.Annotations["note"])
	}
}

// TestBuildService_PropagatesLabelsAnnotations verifies spec.labels /
// spec.annotations are applied to the Service with base labels winning.
func TestBuildService_PropagatesLabelsAnnotations(t *testing.T) {
	config := testConfig()
	labels := map[string]string{"team": "infra", ClusterLabel: "hijacked"}
	annotations := map[string]string{"note": "test"}
	config.Spec.Labels = &labels
	config.Spec.Annotations = &annotations

	svc := buildService("test-cluster", "default", config)

	if svc.Labels["team"] != "infra" {
		t.Errorf("expected Service label 'team'='infra', got '%s'", svc.Labels["team"])
	}
	if svc.Labels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base ClusterLabel to win on Service, got '%s'", svc.Labels[ClusterLabel])
	}
	if svc.Annotations["note"] != "test" {
		t.Errorf("expected Service annotation 'note'='test', got '%s'", svc.Annotations["note"])
	}
}

// TestBuildPDB_PropagatesLabelsAnnotations verifies spec.labels /
// spec.annotations are applied to the PDB with base labels winning.
func TestBuildPDB_PropagatesLabelsAnnotations(t *testing.T) {
	config := testConfig()
	labels := map[string]string{"team": "infra", ClusterLabel: "hijacked"}
	annotations := map[string]string{"note": "test"}
	config.Spec.Labels = &labels
	config.Spec.Annotations = &annotations

	pdb := buildPDB("test-cluster", "test-cluster", "default", config)

	if pdb.Labels["team"] != "infra" {
		t.Errorf("expected PDB label 'team'='infra', got '%s'", pdb.Labels["team"])
	}
	if pdb.Labels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base ClusterLabel to win on PDB, got '%s'", pdb.Labels[ClusterLabel])
	}
	if pdb.Annotations["note"] != "test" {
		t.Errorf("expected PDB annotation 'note'='test', got '%s'", pdb.Annotations["note"])
	}
}

func TestBuildStatefulSet_NoAnnotations(t *testing.T) {
	config := testConfig()
	// No annotations set
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if sts.Annotations != nil {
		t.Errorf("expected nil annotations on StatefulSet, got %v", sts.Annotations)
	}
	if sts.Spec.Template.Annotations != nil {
		t.Errorf("expected nil annotations on pod template, got %v", sts.Spec.Template.Annotations)
	}
}

func TestEnsureClusterObjects_CreatesAll(t *testing.T) {
	owner := testOwner()
	config := testConfig()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	err := EnsureClusterObjects(context.Background(), fakeClient, config, owner, "")
	if err != nil {
		t.Fatalf("EnsureClusterObjects failed: %v", err)
	}

	// Verify ConfigMap exists
	cm := &corev1.ConfigMap{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, cm); err != nil {
		t.Fatalf("ConfigMap not created: %v", err)
	}

	// Verify Service exists
	svc := &corev1.Service{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, svc); err != nil {
		t.Fatalf("Service not created: %v", err)
	}

	// Verify StatefulSet exists
	sts := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, sts); err != nil {
		t.Fatalf("StatefulSet not created: %v", err)
	}
}

func TestGetRedisPassword_NoSecret(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	password, err := GetRedisPassword(context.Background(), fakeClient, "", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if password != "" {
		t.Fatalf("expected empty password, got '%s'", password)
	}
}

func TestGetRedisPassword_WithSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-auth",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("mysecret"),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build()

	password, err := GetRedisPassword(context.Background(), fakeClient, "redis-auth", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if password != "mysecret" {
		t.Fatalf("expected 'mysecret', got '%s'", password)
	}
}

func TestClusterLabels(t *testing.T) {
	labels := clusterLabels("my-cluster")
	if labels[ClusterLabel] != "my-cluster" {
		t.Fatalf("expected ClusterLabel='my-cluster', got '%s'", labels[ClusterLabel])
	}
	if labels[ComponentLabel] != ComponentRedis {
		t.Fatalf("expected ComponentLabel='%s', got '%s'", ComponentRedis, labels[ComponentLabel])
	}
}

func TestAllPodsReady_StatefulSetNotFound(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	_, err := AllPodsReady(context.Background(), fakeClient, "missing", "default", 3)
	if err == nil {
		t.Fatal("expected error for missing StatefulSet")
	}
}

func TestAllPodsReady_NotEnoughReplicas(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	ready, err := AllPodsReady(context.Background(), fakeClient, "test-cluster", "default", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ready {
		t.Fatal("expected not ready with 1/3 replicas")
	}
}

func TestAllPodsReady_AllReady(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	ready, err := AllPodsReady(context.Background(), fakeClient, "test-cluster", "default", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ready {
		t.Fatal("expected ready with 3/3 replicas")
	}
}

func TestGetPodAddresses_NoPods(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	addrs, err := GetPodAddresses(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses, got %d", len(addrs))
	}
}

func TestGetPodAddresses_WithPods(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cluster-0", Namespace: "default",
				Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cluster-1", Namespace: "default",
				Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cluster-2", Namespace: "default",
				Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
			},
			Status: corev1.PodStatus{PodIP: ""}, // No IP yet
		},
	}
	objs := make([]runtime.Object, len(pods))
	for i := range pods {
		objs[i] = &pods[i]
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithRuntimeObjects(objs...).Build()

	addrs, err := GetPodAddresses(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only 2 pods have IPs
	if len(addrs) != 2 {
		t.Fatalf("expected 2 addresses, got %d", len(addrs))
	}
	if addrs["test-cluster-0"] != "10.0.0.1:6379" {
		t.Fatalf("expected '10.0.0.1:6379', got '%s'", addrs["test-cluster-0"])
	}
	if addrs["test-cluster-1"] != "10.0.0.2:6379" {
		t.Fatalf("expected '10.0.0.2:6379', got '%s'", addrs["test-cluster-1"])
	}
}

func TestGetPodAddresses_IgnoresOtherClusters(t *testing.T) {
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster-0", Namespace: "default",
			Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.1"},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-cluster-0", Namespace: "default",
			Labels: map[string]string{ClusterLabel: "other-cluster", ComponentLabel: ComponentRedis},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.99"},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithRuntimeObjects(pod1, pod2).Build()

	addrs, err := GetPodAddresses(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("expected 1 address, got %d", len(addrs))
	}
}

func TestBuildPDB(t *testing.T) {
	cfg := testConfig()
	cfg.Spec.Pdb.PdbSizeUnavailable = intstr.FromInt(1)

	pdb := buildPDB("test-cluster", "test-cluster", "default", cfg)

	if pdb.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", pdb.Name)
	}
	if pdb.Namespace != "default" {
		t.Fatalf("expected namespace 'default', got '%s'", pdb.Namespace)
	}
	if pdb.Spec.MaxUnavailable == nil {
		t.Fatal("expected MaxUnavailable to be set")
	}
	if pdb.Spec.MaxUnavailable.String() != "1" {
		t.Fatalf("expected MaxUnavailable='1', got '%s'", pdb.Spec.MaxUnavailable.String())
	}
}

func TestBuildPDB_MinAvailable(t *testing.T) {
	cfg := testConfig()
	cfg.Spec.Pdb.PdbSizeAvailable = intstr.FromInt(2)

	pdb := buildPDB("test-cluster", "test-cluster", "default", cfg)

	if pdb.Spec.MinAvailable == nil {
		t.Fatal("expected MinAvailable to be set")
	}
	if pdb.Spec.MinAvailable.String() != "2" {
		t.Fatalf("expected MinAvailable='2', got '%s'", pdb.Spec.MinAvailable.String())
	}
}

// --- Scaling helpers ---

func TestScaleStatefulSet_UpdatesReplicas(t *testing.T) {
	old := int32(6)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &old},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	if err := ScaleStatefulSet(context.Background(), fakeClient, "test-cluster", "default", 9); err != nil {
		t.Fatalf("ScaleStatefulSet failed: %v", err)
	}

	got := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, got); err != nil {
		t.Fatalf("getting StatefulSet: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 9 {
		t.Fatalf("expected 9 replicas, got %v", got.Spec.Replicas)
	}
}

func TestScaleStatefulSet_NoOpWhenUnchanged(t *testing.T) {
	current := int32(6)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", ResourceVersion: "1"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &current},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	if err := ScaleStatefulSet(context.Background(), fakeClient, "test-cluster", "default", 6); err != nil {
		t.Fatalf("ScaleStatefulSet failed: %v", err)
	}

	got := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, got); err != nil {
		t.Fatalf("getting StatefulSet: %v", err)
	}
	// ResourceVersion unchanged means no update was issued.
	if got.ResourceVersion != "1" {
		t.Fatalf("expected no update (resourceVersion 1), got %s", got.ResourceVersion)
	}
}

func TestScaleStatefulSet_NotFound(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	if err := ScaleStatefulSet(context.Background(), fakeClient, "missing", "default", 3); err == nil {
		t.Fatal("expected error for missing StatefulSet")
	}
}

func TestGetStatefulSetReplicas(t *testing.T) {
	current := int32(4)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &current},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	got, err := GetStatefulSetReplicas(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 4 {
		t.Fatalf("expected 4 replicas, got %d", got)
	}
}

func TestStatefulSetExists(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	exists, err := StatefulSetExists(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected StatefulSet to exist")
	}

	missing, err := StatefulSetExists(context.Background(), fakeClient, "missing", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if missing {
		t.Fatal("expected missing StatefulSet to not exist")
	}
}

func TestDeleteStatefulSet(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	if err := DeleteStatefulSet(context.Background(), fakeClient, "test-cluster", "default"); err != nil {
		t.Fatalf("DeleteStatefulSet failed: %v", err)
	}
	exists, err := StatefulSetExists(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("expected StatefulSet to be deleted")
	}

	// Deleting a missing StatefulSet is a no-op.
	if err := DeleteStatefulSet(context.Background(), fakeClient, "test-cluster", "default"); err != nil {
		t.Fatalf("expected no error deleting missing StatefulSet, got: %v", err)
	}
}

// --- Helpers ---

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- Default cluster configuration tests ---

func TestBuildRedisConf_ClusterDefaultsApplied(t *testing.T) {
	// Every default in clusterDefaults must appear in the generated config,
	// regardless of storage mode.
	topologies := []struct {
		name      string
		ephemeral bool
	}{
		{"ephemeral", true},
		{"persistent", false},
	}

	for _, tc := range topologies {
		t.Run(tc.name, func(t *testing.T) {
			conf := buildRedisConf("", "", tc.ephemeral, false)
			for _, param := range clusterDefaults {
				if !contains(conf, param) {
					t.Errorf("expected default param %q in redis.conf for topology %s", param, tc.name)
				}
			}
		})
	}
}

func TestBuildRedisConf_DataDirByStorage(t *testing.T) {
	// Ephemeral clusters have no mounted data volume, so Redis must write nodes.conf to
	// /tmp (writable by the container's runtime UID). Persistent clusters use the PVC at /data.
	cases := []struct {
		name      string
		ephemeral bool
		wantDir   string
	}{
		{"ephemeral", true, "/tmp"},
		{"persistent", false, "/data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := buildRedisConf("", "", tc.ephemeral, false)
			if !contains(conf, "dir "+tc.wantDir+"\n") {
				t.Errorf("expected 'dir %s' in redis.conf, got:\n%s", tc.wantDir, conf)
			}
			if !contains(conf, "cluster-config-file "+tc.wantDir+"/nodes.conf") {
				t.Errorf("expected 'cluster-config-file %s/nodes.conf' in redis.conf, got:\n%s", tc.wantDir, conf)
			}
		})
	}
}

func TestBuildRedisConf_RequireFullCoverageNo(t *testing.T) {
	conf := buildRedisConf("", "", true, false)
	if !contains(conf, "cluster-require-full-coverage no") {
		t.Error("expected 'cluster-require-full-coverage no' in redis.conf")
	}
}

func TestBuildRedisConf_AllowReadsWhenDownAllTopologies(t *testing.T) {
	// cluster-allow-reads-when-down must be present even without replicas
	conf := buildRedisConf("", "", true, false)
	if !contains(conf, "cluster-allow-reads-when-down yes") {
		t.Error("expected 'cluster-allow-reads-when-down yes' for ephemeral no-replicas")
	}

	conf = buildRedisConf("", "", false, false)
	if !contains(conf, "cluster-allow-reads-when-down yes") {
		t.Error("expected 'cluster-allow-reads-when-down yes' for persistent no-replicas")
	}
}

func TestBuildRedisConf_AllowReplicaMigrationDisabled(t *testing.T) {
	// cluster-allow-replica-migration must be disabled across every topology so that
	// primaries drained to zero slots during scale-down/upgrade are removed rather than
	// auto-converted into replicas (which would generate needless sync traffic and leave
	// replicas in a cluster configured without them).
	topologies := []struct {
		name      string
		ephemeral bool
	}{
		{"ephemeral", true},
		{"persistent", false},
	}

	for _, tc := range topologies {
		t.Run(tc.name, func(t *testing.T) {
			conf := buildRedisConf("", "", tc.ephemeral, false)
			if !contains(conf, "cluster-allow-replica-migration no") {
				t.Errorf("expected 'cluster-allow-replica-migration no' in redis.conf for topology %s", tc.name)
			}
		})
	}
}

func TestBuildRedisConf_UserConfigOverridesDefaults(t *testing.T) {
	// User sets cluster-require-full-coverage yes — it should appear AFTER the default 'no'
	userConf := "cluster-require-full-coverage yes"
	conf := buildRedisConf(userConf, "", true, false)

	// Both should be present (last-write-wins in Redis)
	if !contains(conf, "cluster-require-full-coverage no") {
		t.Error("expected default 'cluster-require-full-coverage no' to still be in config")
	}
	if !contains(conf, "cluster-require-full-coverage yes") {
		t.Error("expected user override 'cluster-require-full-coverage yes' to be in config")
	}

	// User override must come after the default
	defaultIdx := indexOf(conf, "cluster-require-full-coverage no")
	userIdx := indexOf(conf, "cluster-require-full-coverage yes")
	if userIdx <= defaultIdx {
		t.Error("expected user config to appear after defaults (last-write-wins)")
	}
}

// TestBuildRedisConf_PersistenceOwnership pins down who writes the persistence directives:
// Robin for ephemeral clusters (AOF and RDB off, emitted once, before the user config), the user
// (spec.config) for persistent clusters, where Robin must not add any appendonly/save line.
func TestBuildRedisConf_PersistenceOwnership(t *testing.T) {
	userConfigs := []struct {
		name           string
		config         string
		wantAppendonly int
		wantSave       int
	}{
		{"no user persistence", "maxmemory 90mb", 0, 0},
		{"user enables AOF and RDB", "appendonly yes\nappendfsync everysec\nsave 900 1", 1, 1},
		{"user disables AOF and RDB", "appendonly no\nsave \"\"", 1, 1},
	}

	for _, standalone := range []bool{false, true} {
		for _, uc := range userConfigs {
			name := fmt.Sprintf("standalone=%t/%s", standalone, uc.name)

			t.Run("ephemeral/"+name, func(t *testing.T) {
				conf := buildRedisConf(uc.config, "", true, standalone)
				if got, want := countDirective(conf, "appendonly"), 1+uc.wantAppendonly; got != want {
					t.Errorf("expected %d 'appendonly' lines, got %d in:\n%s", want, got, conf)
				}
				if got, want := countDirective(conf, "save"), 1+uc.wantSave; got != want {
					t.Errorf("expected %d 'save' lines, got %d in:\n%s", want, got, conf)
				}
				robinIdx := indexOf(conf, "appendonly no\nsave \"\"\n")
				if robinIdx < 0 {
					t.Fatalf("expected Robin's 'appendonly no' + 'save \"\"' block in:\n%s", conf)
				}
				if userIdx := strings.LastIndex(conf, uc.config); userIdx <= robinIdx {
					t.Errorf("expected user config after Robin's persistence defaults (last-write-wins), got:\n%s", conf)
				}
			})

			t.Run("persistent/"+name, func(t *testing.T) {
				conf := buildRedisConf(uc.config, "", false, standalone)
				if got := countDirective(conf, "appendonly"); got != uc.wantAppendonly {
					t.Errorf("expected %d 'appendonly' lines (user only), got %d in:\n%s", uc.wantAppendonly, got, conf)
				}
				if got := countDirective(conf, "save"); got != uc.wantSave {
					t.Errorf("expected %d 'save' lines (user only), got %d in:\n%s", uc.wantSave, got, conf)
				}
			})
		}
	}
}

// countDirective returns how many redis.conf lines set the given directive.
func countDirective(conf, directive string) int {
	n := 0
	for _, line := range strings.Split(conf, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == directive {
			n++
		}
	}
	return n
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func TestParseRequirePass(t *testing.T) {
	cases := []struct {
		name string
		conf string
		want string
	}{
		{"present", "appendonly no\nrequirepass secret123\nmasterauth secret123\n", "secret123"},
		{"absent", "appendonly no\nsave \"\"\n", ""},
		{"empty", "", ""},
		{"with surrounding whitespace", "  requirepass  spaced-pass  \n", "spaced-pass"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRequirePass(tc.conf); got != tc.want {
				t.Fatalf("parseRequirePass(%q) = %q, want %q", tc.conf, got, tc.want)
			}
		})
	}
}

func TestGetConfigMapPassword(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster", Namespace: "default"},
		Data:       map[string]string{"redis.conf": "requirepass cm-pass\nmasterauth cm-pass\n"},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm).Build()

	pw, found, err := GetConfigMapPassword(context.Background(), c, "my-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found || pw != "cm-pass" {
		t.Fatalf("expected ('cm-pass', true), got (%q, %v)", pw, found)
	}

	_, found, err = GetConfigMapPassword(context.Background(), c, "missing", "default")
	if err != nil {
		t.Fatalf("unexpected error for missing ConfigMap: %v", err)
	}
	if found {
		t.Fatal("expected found=false for missing ConfigMap")
	}
}

// --- Standalone (single-node, non-clustered) tests ---

func standaloneConfig() *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config-standalone",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           1,
			Mode:               redisv1.ModeStandalone,
			Primaries:          1,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func TestBuildRedisConf_StandaloneDisablesCluster(t *testing.T) {
	conf := buildRedisConf("", "", true, true)
	if !contains(conf, "cluster-enabled no") {
		t.Error("expected 'cluster-enabled no' for standalone mode")
	}
	// None of the cluster-only defaults must be present in standalone mode.
	for _, param := range clusterDefaults {
		if contains(conf, param) {
			t.Errorf("did not expect cluster default %q in standalone redis.conf", param)
		}
	}
}

func TestBuildConfigMap_Standalone(t *testing.T) {
	config := standaloneConfig()
	cm := buildConfigMap("test-cluster", "default", config, "")

	conf := cm.Data["redis.conf"]
	if !contains(conf, "cluster-enabled no") {
		t.Error("expected 'cluster-enabled no' in standalone ConfigMap")
	}
	if contains(conf, "cluster-enabled yes") {
		t.Error("did not expect 'cluster-enabled yes' in standalone ConfigMap")
	}
}

func TestBuildStatefulSet_StandaloneSingleReplica(t *testing.T) {
	config := standaloneConfig()
	owner := testOwner()
	owner.Spec.Mode = redisv1.ModeStandalone
	owner.Spec.Primaries = 1
	owner.Spec.ReplicasPerPrimary = 0

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if *sts.Spec.Replicas != 1 {
		t.Fatalf("expected 1 replica for standalone, got %d", *sts.Spec.Replicas)
	}
}

func TestBuildStatefulSet_StandaloneWithStorage(t *testing.T) {
	config := standaloneConfig()
	config.Spec.Ephemeral = false
	config.Spec.Storage = "5Gi"

	owner := testOwner()
	owner.Spec.Mode = redisv1.ModeStandalone
	owner.Spec.Primaries = 1
	owner.Spec.ReplicasPerPrimary = 0
	owner.Spec.Ephemeral = false
	owner.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if *sts.Spec.Replicas != 1 {
		t.Fatalf("expected 1 replica for standalone, got %d", *sts.Spec.Replicas)
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected 1 VolumeClaimTemplate for standalone with storage, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	expectedStorage := resource.MustParse("5Gi")
	actualStorage := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	if !actualStorage.Equal(expectedStorage) {
		t.Fatalf("expected storage '%s', got '%s'", expectedStorage.String(), actualStorage.String())
	}
}
