// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

const (
	// RedisPort is the default Redis port.
	RedisPort = 6379
	// RedisGossipPort is the Redis cluster gossip port.
	RedisGossipPort = 16379
	// ClusterLabel is the label used to identify resources belonging to a cluster.
	ClusterLabel = "redkey.inditex.dev/cluster"
	// ComponentLabel is the label used to identify the component type.
	ComponentLabel = "redkey.inditex.dev/component"
	// ComponentRedis is the value for the component label for Redis nodes.
	ComponentRedis = "redis"

	// defaultRedisImage is used when no image is specified.
	defaultRedisImage = "redis:8"
)

// EnsureClusterObjects creates or updates all Kubernetes objects required for the Redis cluster.
// It uses the Redkey as the owner reference so objects are garbage-collected on deletion.
func EnsureClusterObjects(
	ctx context.Context,
	c client.Client,
	config *redisv1.RedkeyConfig,
	owner *redisv1.Redkey,
	password string,
) error {
	clusterName := owner.Name
	namespace := owner.Namespace

	// 1. ConfigMap
	if err := ensureConfigMap(ctx, c, clusterName, namespace, config, owner, password); err != nil {
		return fmt.Errorf("ensuring ConfigMap: %w", err)
	}

	// 2. Service
	if err := ensureService(ctx, c, clusterName, namespace, config, owner); err != nil {
		return fmt.Errorf("ensuring Service: %w", err)
	}

	// 3. StatefulSet
	if err := ensureStatefulSet(ctx, c, clusterName, namespace, config, owner); err != nil {
		return fmt.Errorf("ensuring StatefulSet: %w", err)
	}

	// 4. PodDisruptionBudget (optional)
	if err := ReconcilePDB(ctx, c, config, owner); err != nil {
		return err
	}

	return nil
}

// ReconcilePDB creates, updates, or deletes the cluster's PodDisruptionBudget so it
// matches the desired configuration. The PDB is only kept when it is enabled and the
// cluster has more than one primary; otherwise any existing PDB is removed.
func ReconcilePDB(ctx context.Context, c client.Client, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) error {
	clusterName := owner.Name
	namespace := owner.Namespace
	if config.Spec.Pdb.Enabled && config.Spec.Primaries > 1 {
		if err := ensurePDB(ctx, c, clusterName, namespace, config, owner); err != nil {
			return fmt.Errorf("ensuring PDB: %w", err)
		}
		return nil
	}
	if err := DeletePDB(ctx, c, clusterName, namespace); err != nil {
		return fmt.Errorf("deleting PDB: %w", err)
	}
	return nil
}

// SecretPasswordKey is the data key inside the auth Secret that holds the Redis
// password. It is the contract documented in the operator authentication guide
// and is shared by every component that reads the password (reconciler and
// metrics collector) to avoid divergence.
const SecretPasswordKey = "password"

// GetRedisPassword reads the auth secret and returns the password, or empty string if no secret is configured.
func GetRedisPassword(ctx context.Context, c client.Client, secretName, namespace string) (string, error) {
	if secretName == "" {
		return "", nil
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return "", fmt.Errorf("getting auth secret %s: %w", secretName, err)
	}
	if val, ok := secret.Data[SecretPasswordKey]; ok {
		return string(val), nil
	}
	return "", nil
}

// GetConfigMapPassword returns the password currently baked into the cluster's
// ConfigMap (the requirepass directive of redis.conf). This reflects the
// credentials the running Redis nodes were started with and therefore still use,
// which is the source of truth Robin needs to connect to the nodes after the auth
// Secret has been rotated out-of-band but before the new password has been
// applied via CONFIG SET. found is false when the ConfigMap does not exist yet.
func GetConfigMapPassword(ctx context.Context, c client.Client, clusterName, namespace string) (password string, found bool, err error) {
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, cm); err != nil {
		if errors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("getting ConfigMap %s: %w", clusterName, err)
	}
	return parseRequirePass(cm.Data["redis.conf"]), true, nil
}

// parseRequirePass extracts the value of the requirepass directive from a
// redis.conf body. It returns an empty string when no requirepass line is
// present (auth disabled).
func parseRequirePass(redisConf string) string {
	for line := range strings.SplitSeq(redisConf, "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "requirepass "); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

// AllPodsReady checks whether the StatefulSet has all pods in Ready state.
func AllPodsReady(ctx context.Context, c client.Client, clusterName, namespace string, expectedReplicas int32) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return false, err
	}
	return sts.Status.ReadyReplicas >= expectedReplicas, nil
}

// GetStatefulSetReplicas returns the desired replica count currently configured on the
// cluster's StatefulSet. It returns 0 if the StatefulSet has no explicit replica count.
func GetStatefulSetReplicas(ctx context.Context, c client.Client, clusterName, namespace string) (int32, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return 0, err
	}
	if sts.Spec.Replicas == nil {
		return 0, nil
	}
	return *sts.Spec.Replicas, nil
}

// ScaleStatefulSet sets the desired replica count on the cluster's StatefulSet.
// Robin owns the StatefulSet replica count throughout scaling operations, so this is
// the single point where the pod count is changed. It is a no-op when the StatefulSet
// already has the requested replica count.
func ScaleStatefulSet(ctx context.Context, c client.Client, clusterName, namespace string, replicas int32) error {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return fmt.Errorf("getting StatefulSet %s: %w", clusterName, err)
	}
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == replicas {
		return nil
	}
	sts.Spec.Replicas = &replicas
	if err := c.Update(ctx, sts); err != nil {
		return fmt.Errorf("scaling StatefulSet %s to %d replicas: %w", clusterName, replicas, err)
	}
	return nil
}

// StatefulSetExists reports whether the cluster's StatefulSet currently exists.
func StatefulSetExists(ctx context.Context, c client.Client, clusterName, namespace string) (bool, error) {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// DeleteStatefulSet deletes the cluster's StatefulSet. It is used by fast scaling, which
// wipes and recreates the StatefulSet. Missing StatefulSets are treated as success.
func DeleteStatefulSet(ctx context.Context, c client.Client, clusterName, namespace string) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	}
	if err := c.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting StatefulSet %s: %w", clusterName, err)
	}
	return nil
}

// GetPodAddresses returns a map of pod name → IP:port for the cluster's StatefulSet pods.
// This uses pod IPs directly, which works both in-cluster and out-of-cluster (e.g. kind dev).
func GetPodAddresses(ctx context.Context, c client.Client, clusterName, namespace string) (map[string]string, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(clusterLabels(clusterName)),
	); err != nil {
		return nil, fmt.Errorf("listing cluster pods: %w", err)
	}

	addrs := make(map[string]string, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.PodIP == "" {
			continue
		}
		addrs[pod.Name] = fmt.Sprintf("%s:%d", pod.Status.PodIP, RedisPort)
	}
	return addrs, nil
}

// --- ConfigMap ---

func ensureConfigMap(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyConfig, owner *redisv1.Redkey, password string) error {
	desired := buildConfigMap(clusterName, namespace, config, password)
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update if data, labels or annotations drifted. Setting the full desired maps
	// also prunes keys that were removed from spec.labels / spec.annotations.
	changed := false
	if existing.Data["redis.conf"] != desired.Data["redis.conf"] {
		existing.Data = desired.Data
		changed = true
	}
	if !reflect.DeepEqual(existing.Labels, desired.Labels) {
		existing.Labels = desired.Labels
		changed = true
	}
	if !reflect.DeepEqual(existing.Annotations, desired.Annotations) {
		existing.Annotations = desired.Annotations
		changed = true
	}
	if changed {
		return c.Update(ctx, existing)
	}
	return nil
}

func buildConfigMap(clusterName, namespace string, config *redisv1.RedkeyConfig, password string) *corev1.ConfigMap {
	redisConf := buildRedisConf(config.Spec.RedisConfig, password, config.Spec.Ephemeral, config.Spec.IsStandalone())
	specLabels := derefMap(config.Spec.Labels)
	specAnnotations := derefMap(config.Spec.Annotations)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        clusterName,
			Namespace:   namespace,
			Labels:      mergeMeta(specLabels, nil, clusterLabels(clusterName)),
			Annotations: mergeMeta(specAnnotations, nil, nil),
		},
		Data: map[string]string{"redis.conf": redisConf},
	}
}

// clusterDefaults contains the Redis configuration parameters that Redkey applies
// unconditionally to every cluster, regardless of topology. These ensure safe
// operation during slot migrations (upgrades, scaling) and partial-failure scenarios.
//
//   - cluster-enabled yes: required for Redis Cluster mode.
//   - cluster-node-timeout 5000: time (ms) before a node is considered unreachable.
//   - cluster-require-full-coverage no: allows the cluster to continue serving requests
//     even when some hash slots are temporarily uncovered (e.g., during reshard).
//     Without this, any slot migration causes CLUSTERDOWN for the entire cluster.
//   - cluster-allow-reads-when-down yes: permits read operations even when the cluster
//     detects partial failure states. Reduces impact on read-intensive workloads during
//     transient conditions (node restart, network partition recovery).
//   - cluster-allow-replica-migration no: disables Redis' automatic replica migration so
//     the operator/robin retains full, deterministic control over topology. This prevents
//     two unwanted behaviors during slot migrations (scale-down, upgrades): a primary that
//     is drained to zero slots auto-converting into a replica of the node that absorbed its
//     slots, and replicas auto-migrating between primaries. Both generate needless
//     replication/sync traffic and can leave replicas in a cluster configured without them.
var clusterDefaults = []string{
	"cluster-enabled yes",
	"cluster-node-timeout 5000",
	"cluster-require-full-coverage no",
	"cluster-allow-reads-when-down yes",
	"cluster-allow-replica-migration no",
}

// standaloneDefaults contains the Redis configuration parameters Redkey applies to a
// standalone (single-node, non-clustered) deployment. Redis Cluster mode is disabled;
// no cluster topology metadata or slot-coverage settings are needed.
var standaloneDefaults = []string{
	"cluster-enabled no",
}

func buildRedisConf(userConfig, password string, ephemeral, standalone bool) string {
	var lines []string

	// Redis writes nodes.conf (cluster topology metadata) into `dir`. Ephemeral clusters have no
	// mounted data volume, so use /tmp — always writable by the container's runtime UID under
	// restrictive PodSecurity/SCC. Persistent clusters use the PVC mounted at /data.
	dataDir := "/data"
	if ephemeral {
		dataDir = "/tmp"
	}
	lines = append(lines, "dir "+dataDir)

	if standalone {
		lines = append(lines, standaloneDefaults...)
	} else {
		lines = append(lines, clusterDefaults...)
		lines = append(lines, "cluster-config-file "+dataDir+"/nodes.conf")
	}

	if ephemeral {
		lines = append(lines, "appendonly no", "save \"\"")
	} else {
		lines = append(lines, "appendonly yes")
	}

	// Add user config (may override defaults above)
	if userConfig != "" {
		lines = append(lines, userConfig)
	}

	// Add auth
	if password != "" {
		lines = append(lines, fmt.Sprintf("requirepass %s", password))
		lines = append(lines, fmt.Sprintf("masterauth %s", password))
	}

	return strings.Join(lines, "\n") + "\n"
}

// --- Service ---

// ReconcileService creates or updates the cluster's headless Service, applying the
// optional Service override from the configuration. It is safe to call repeatedly:
// an existing Service is only updated when its override-managed fields drift.
func ReconcileService(ctx context.Context, c client.Client, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) error {
	return ensureService(ctx, c, owner.Name, owner.Namespace, config, owner)
}

func ensureService(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) error {
	desired := buildService(clusterName, namespace, config)
	if config.Spec.Override != nil && config.Spec.Override.Service != nil {
		var err error
		desired, err = applyServiceOverride(desired, config.Spec.Override.Service, clusterName, derefMap(config.Spec.Labels), derefMap(config.Spec.Annotations))
		if err != nil {
			return err
		}
	}
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &corev1.Service{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update the existing Service only when an override-managed field drifted.
	// ClusterIP(s) are assigned by the API server and must be preserved.
	if serviceNeedsUpdate(existing, desired) {
		desired.Spec.ClusterIP = existing.Spec.ClusterIP
		desired.Spec.ClusterIPs = existing.Spec.ClusterIPs
		desired.ResourceVersion = existing.ResourceVersion
		return c.Update(ctx, desired)
	}
	return nil
}

// serviceNeedsUpdate reports whether the override-managed fields of the existing
// Service differ from the desired Service.
func serviceNeedsUpdate(existing, desired *corev1.Service) bool {
	return !reflect.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) ||
		existing.Spec.Type != desired.Spec.Type ||
		existing.Spec.PublishNotReadyAddresses != desired.Spec.PublishNotReadyAddresses ||
		!reflect.DeepEqual(existing.Labels, desired.Labels) ||
		!reflect.DeepEqual(existing.Annotations, desired.Annotations)
}

func buildService(clusterName, namespace string, config *redisv1.RedkeyConfig) *corev1.Service {
	specLabels := derefMap(config.Spec.Labels)
	specAnnotations := derefMap(config.Spec.Annotations)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        clusterName,
			Namespace:   namespace,
			Labels:      mergeMeta(specLabels, nil, clusterLabels(clusterName)),
			Annotations: mergeMeta(specAnnotations, nil, nil),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "client",
					Protocol:   corev1.ProtocolTCP,
					Port:       RedisPort,
					TargetPort: intstr.FromInt(RedisPort),
				},
				{
					Name:       "gossip",
					Protocol:   corev1.ProtocolTCP,
					Port:       RedisGossipPort,
					TargetPort: intstr.FromInt(RedisGossipPort),
				},
			},
			Selector:                 clusterLabels(clusterName),
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
		},
	}
}

// --- StatefulSet ---

func ensureStatefulSet(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) error {
	desired := buildStatefulSet(clusterName, namespace, config, owner)
	if config.Spec.Override != nil && config.Spec.Override.StatefulSet != nil {
		var err error
		desired, err = applyStatefulSetOverride(desired, config.Spec.Override.StatefulSet, clusterName, derefMap(config.Spec.Labels), derefMap(config.Spec.Annotations))
		if err != nil {
			return err
		}
	}
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	return err
}

func buildStatefulSet(clusterName, namespace string, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) *appsv1.StatefulSet {
	replicas := config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary)
	image := config.Spec.Image
	if image == "" {
		image = defaultRedisImage
	}

	labels := clusterLabels(clusterName)
	podManagement := appsv1.ParallelPodManagement

	specLabels := derefMap(config.Spec.Labels)
	specAnnotations := derefMap(config.Spec.Annotations)

	// Object and pod metadata: spec.labels / spec.annotations decorate the objects,
	// but the internal cluster labels (also the selector labels) always win. Separate
	// maps are built for the StatefulSet metadata and the pod template so later
	// mutations (e.g. the config-checksum annotation) cannot leak across them.
	stsLabels := mergeMeta(specLabels, nil, clusterLabels(clusterName))
	stsAnnotations := mergeMeta(specAnnotations, nil, nil)
	podLabels := mergeMeta(specLabels, nil, clusterLabels(clusterName))
	podAnnotations := mergeMeta(specAnnotations, nil, nil)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        clusterName,
			Namespace:   namespace,
			Labels:      stsLabels,
			Annotations: stsAnnotations,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			PodManagementPolicy: podManagement,
			ServiceName:         clusterName,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "redis",
							Image: image,
							Ports: []corev1.ContainerPort{
								{Name: "client", ContainerPort: RedisPort},
								{Name: "gossip", ContainerPort: RedisGossipPort},
							},
							Command:        []string{"redis-server", "/conf/redis.conf"},
							LivenessProbe:  createProbe(15, 5),
							ReadinessProbe: createProbe(10, 5),
							VolumeMounts: []corev1.VolumeMount{
								{Name: "config", MountPath: "/conf"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: clusterName},
									Items:                []corev1.KeyToPath{{Key: "redis.conf", Path: "redis.conf"}},
								},
							},
						},
					},
				},
			},
		},
	}

	// Set resources if provided
	if config.Spec.Resources != nil {
		sts.Spec.Template.Spec.Containers[0].Resources = *config.Spec.Resources
	}

	// Add storage for non-ephemeral clusters
	if !config.Spec.Ephemeral {
		addStorage(sts, config, owner)
	}

	return sts
}

func addStorage(sts *appsv1.StatefulSet, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) {
	accessModes := owner.Spec.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(config.Spec.Storage),
				},
			},
		},
	}

	if config.Spec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &config.Spec.StorageClassName
	}

	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	// PVC retention policy. When deletePVC is true, PVCs are deleted both when the StatefulSet is
	// deleted (scale-to-zero / cluster deletion) AND when it is scaled DOWN to a smaller size >0,
	// so the removed ordinals' volumes don't linger with stale, inconsistent slot/node data. When
	// deletePVC is false, PVCs are retained in both cases to preserve data.
	retention := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	if config.Spec.DeletePVC != nil && *config.Spec.DeletePVC {
		retention = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: retention,
		WhenScaled:  retention,
	}

	// Add data volume mount
	sts.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		sts.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: "data", MountPath: "/data"},
	)
}

// --- PodDisruptionBudget ---

func ensurePDB(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) error {
	pdbName := clusterName + "-pdb"
	desired := buildPDB(pdbName, clusterName, namespace, config)
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &policyv1.PodDisruptionBudget{}
	err := c.Get(ctx, types.NamespacedName{Name: pdbName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update the existing PDB if its spec, labels or annotations drifted from the
	// desired state. Setting the full desired maps also prunes removed keys.
	if !reflect.DeepEqual(existing.Spec.MinAvailable, desired.Spec.MinAvailable) ||
		!reflect.DeepEqual(existing.Spec.MaxUnavailable, desired.Spec.MaxUnavailable) ||
		!reflect.DeepEqual(existing.Labels, desired.Labels) ||
		!reflect.DeepEqual(existing.Annotations, desired.Annotations) {
		existing.Spec.MinAvailable = desired.Spec.MinAvailable
		existing.Spec.MaxUnavailable = desired.Spec.MaxUnavailable
		existing.Labels = desired.Labels
		existing.Annotations = desired.Annotations
		return c.Update(ctx, existing)
	}

	return nil
}

func buildPDB(pdbName, clusterName, namespace string, config *redisv1.RedkeyConfig) *policyv1.PodDisruptionBudget {
	specLabels := derefMap(config.Spec.Labels)
	specAnnotations := derefMap(config.Spec.Annotations)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pdbName,
			Namespace:   namespace,
			Labels:      mergeMeta(specLabels, nil, clusterLabels(clusterName)),
			Annotations: mergeMeta(specAnnotations, nil, nil),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: clusterLabels(clusterName),
			},
		},
	}

	if config.Spec.Pdb.PdbSizeAvailable.String() != "0" && config.Spec.Pdb.PdbSizeAvailable.String() != "" {
		pdb.Spec.MinAvailable = &config.Spec.Pdb.PdbSizeAvailable
	} else if config.Spec.Pdb.PdbSizeUnavailable.String() != "0" && config.Spec.Pdb.PdbSizeUnavailable.String() != "" {
		pdb.Spec.MaxUnavailable = &config.Spec.Pdb.PdbSizeUnavailable
	}

	return pdb
}

// --- Helpers ---

func clusterLabels(clusterName string) map[string]string {
	return map[string]string{
		ClusterLabel:   clusterName,
		ComponentLabel: ComponentRedis,
	}
}

// derefMap returns the map pointed to by m, or nil when m is nil. It is a
// convenience for the optional spec.labels / spec.annotations pointer fields.
func derefMap(m *map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	return *m
}

// mergeMeta computes the final labels (or annotations) for a managed object by
// combining three sources with a fixed precedence:
//
//  1. spec: the user-provided spec.labels / spec.annotations from the Redkey.
//  2. override: labels / annotations defined in an override block (Service,
//     StatefulSet, or pod template). When the override defines any entry it fully
//     REPLACES the spec source (block replacement); the spec entries are discarded.
//  3. base: the internal labels / annotations Redkey requires for correct operation
//     (cluster identity / selector labels, ...). These always win and are applied
//     last so user input can never shadow them.
//
// The result is a freshly allocated map (independent per call), or nil when the
// combined result would be empty.
func mergeMeta(spec, override, base map[string]string) map[string]string {
	out := make(map[string]string, len(spec)+len(override)+len(base))
	if len(override) > 0 {
		maps.Copy(out, override)
	} else {
		maps.Copy(out, spec)
	}
	maps.Copy(out, base)
	if len(out) == 0 {
		return nil
	}
	return out
}

func createProbe(initial, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(RedisPort)},
		},
		InitialDelaySeconds: initial,
		PeriodSeconds:       period,
	}
}

// DeleteService deletes the cluster's headless Service. Missing Services are treated as success.
func DeleteService(ctx context.Context, c client.Client, clusterName, namespace string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	}
	if err := c.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting Service %s: %w", clusterName, err)
	}
	return nil
}

// DeleteConfigMap deletes the cluster's ConfigMap. Missing ConfigMaps are treated as success.
func DeleteConfigMap(ctx context.Context, c client.Client, clusterName, namespace string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	}
	if err := c.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting ConfigMap %s: %w", clusterName, err)
	}
	return nil
}

// DeletePDB deletes the cluster's PodDisruptionBudget. Missing PDBs are treated as success.
func DeletePDB(ctx context.Context, c client.Client, clusterName, namespace string) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-pdb", Namespace: namespace},
	}
	if err := c.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting PDB %s-pdb: %w", clusterName, err)
	}
	return nil
}

// DeletePVCs deletes all PersistentVolumeClaims belonging to the cluster.
// It uses the cluster label selector to find matching PVCs.
// Missing PVCs are treated as success.
func DeletePVCs(ctx context.Context, c client.Client, clusterName, namespace string) error {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList,
		client.InNamespace(namespace),
		client.MatchingLabels(clusterLabels(clusterName)),
	); err != nil {
		return fmt.Errorf("listing PVCs for cluster %s: %w", clusterName, err)
	}

	for i := range pvcList.Items {
		if err := c.Delete(ctx, &pvcList.Items[i]); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("deleting PVC %s: %w", pvcList.Items[i].Name, err)
		}
	}
	return nil
}

// --- Upgrade Helpers ---

// ConfigChecksumAnnotation is the annotation key used to store the configuration checksum on pod templates.
// When the checksum changes, Kubernetes recreates the affected pods on the next partition update.
const ConfigChecksumAnnotation = "redkey.inditex.dev/config-checksum"

// UpdateStatefulSetTemplate updates the StatefulSet's pod template to reflect the new configuration:
// image, resources, labels, annotations, and config checksum. It does NOT change the replica count.
// The update strategy is set to OnDelete so that no pods are automatically recreated — the
// reconciler controls which specific pods are recycled via manual deletion, ensuring only drained
// primaries and their replicas are affected (preserving HA for active shards).
func UpdateStatefulSetTemplate(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyConfig, owner *redisv1.Redkey, configChecksum string) error {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return fmt.Errorf("getting StatefulSet %s for template update: %w", clusterName, err)
	}

	// Rebuild the desired pod template from the base builder and re-apply the
	// override. Rebuilding (instead of patching in place) keeps the template in
	// sync with the current configuration and cleanly reverts any fields a
	// previously-set override may have introduced once it is removed.
	base := buildStatefulSet(clusterName, namespace, config, owner)
	if config.Spec.Override != nil && config.Spec.Override.StatefulSet != nil {
		var err error
		base, err = applyStatefulSetOverride(base, config.Spec.Override.StatefulSet, clusterName, derefMap(config.Spec.Labels), derefMap(config.Spec.Annotations))
		if err != nil {
			return fmt.Errorf("applying StatefulSet override for template update: %w", err)
		}
	}

	// Preserve the StatefulSet identity / immutable fields from the existing
	// object. Kubernetes rejects in-place changes to these, and they are owned by
	// the operator's scaling and upgrade logic — never by user overrides:
	//   - spec.replicas               — driven by the primaries/replicas formula
	//   - spec.selector               — must keep matching the managed pods
	//   - spec.serviceName            — must match the headless Service
	//   - spec.podManagementPolicy    — immutable after creation
	//   - spec.volumeClaimTemplates   — immutable after creation
	base.Spec.Replicas = sts.Spec.Replicas
	base.Spec.Selector = sts.Spec.Selector
	base.Spec.ServiceName = sts.Spec.ServiceName
	base.Spec.PodManagementPolicy = sts.Spec.PodManagementPolicy
	base.Spec.VolumeClaimTemplates = sts.Spec.VolumeClaimTemplates

	// Apply the fully-rebuilt desired object so that EVERY override-managed field
	// is reconciled on the existing StatefulSet — its own metadata (labels and
	// annotations) and all mutable spec fields (pod template, minReadySeconds,
	// revisionHistoryLimit, PVC retention policy, ordinals, ...) — not just the
	// pod template. The pod template's labels/annotations already are a merge of
	// the cluster selector labels, spec.labels/spec.annotations and the override
	// template metadata, performed by buildStatefulSet + applyStatefulSetOverride.
	sts.Labels = base.Labels
	sts.Annotations = base.Annotations
	sts.Spec = base.Spec

	// Stamp the config checksum so a change in redis.conf triggers pod recreation.
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	sts.Spec.Template.Annotations[ConfigChecksumAnnotation] = configChecksum

	// Set update strategy to OnDelete so that no pods are automatically recreated.
	// The reconciler will manually delete specific pods (drained primary + its replicas)
	// to trigger recreation with the new template, preserving HA for active shards.
	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
		Type: appsv1.OnDeleteStatefulSetStrategyType,
	}

	if err := c.Update(ctx, sts); err != nil {
		return fmt.Errorf("updating StatefulSet template %s: %w", clusterName, err)
	}
	return nil
}

// SetStatefulSetPartition sets the partition value on the StatefulSet's RollingUpdate strategy.
// Pods with ordinal >= partition will be recreated with the new template. Setting partition to 0
// causes all pods to be updated.
func SetStatefulSetPartition(ctx context.Context, c client.Client, clusterName, namespace string, partition int32) error {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return fmt.Errorf("getting StatefulSet %s for partition update: %w", clusterName, err)
	}

	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
		Type: appsv1.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
			Partition: &partition,
		},
	}

	if err := c.Update(ctx, sts); err != nil {
		return fmt.Errorf("setting StatefulSet %s partition to %d: %w", clusterName, partition, err)
	}
	return nil
}

// IsPodOrdinalReady checks if a specific pod (by ordinal index) in the StatefulSet is Ready.
func IsPodOrdinalReady(ctx context.Context, c client.Client, clusterName, namespace string, ordinal int32) (bool, error) {
	podName := fmt.Sprintf("%s-%d", clusterName, ordinal)
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting pod %s: %w", podName, err)
	}

	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, nil
}

// GetPodImage returns the image of the first container of a specific pod (by ordinal).
func GetPodImage(ctx context.Context, c client.Client, clusterName, namespace string, ordinal int32) (string, error) {
	podName := fmt.Sprintf("%s-%d", clusterName, ordinal)
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod); err != nil {
		return "", fmt.Errorf("getting pod %s for image check: %w", podName, err)
	}
	if len(pod.Spec.Containers) == 0 {
		return "", fmt.Errorf("pod %s has no containers", podName)
	}
	return pod.Spec.Containers[0].Image, nil
}

// PodTemplateHashLabel is the StatefulSet controller-managed label that records the
// ControllerRevision a pod was created from. It changes whenever the pod template changes
// (image, resources, labels, annotations including the config checksum, etc.).
const PodTemplateHashLabel = "controller-revision-hash"

// GetPodControllerRevisionHash returns the controller-revision-hash label of a specific
// pod (by ordinal). This native StatefulSet label identifies the exact pod-template
// revision the pod was created from, so it can be compared against the StatefulSet's
// UpdateRevision to determine whether the pod is running the desired configuration.
// It is deterministic and robust to label/annotation ordering, unlike an ad-hoc checksum.
func GetPodControllerRevisionHash(ctx context.Context, c client.Client, clusterName, namespace string, ordinal int32) (string, error) {
	podName := fmt.Sprintf("%s-%d", clusterName, ordinal)
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod); err != nil {
		return "", fmt.Errorf("getting pod %s for revision check: %w", podName, err)
	}
	return pod.Labels[PodTemplateHashLabel], nil
}

// GetStatefulSetUpdateRevision returns the StatefulSet's current UpdateRevision, i.e. the
// ControllerRevision hash that corresponds to the desired pod template. Pods whose
// controller-revision-hash label differs from this value still run an outdated template
// and must be recycled.
func GetStatefulSetUpdateRevision(ctx context.Context, c client.Client, clusterName, namespace string) (string, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return "", fmt.Errorf("getting StatefulSet %s for update revision: %w", clusterName, err)
	}
	return sts.Status.UpdateRevision, nil
}

// GetStatefulSetUpdateRevisionObserved returns the StatefulSet's UpdateRevision, but only
// after the StatefulSet controller has observed the latest spec change — i.e. once
// Status.ObservedGeneration has caught up with metadata.Generation. Until then it returns
// ("", false, nil) so the caller waits instead of comparing against a stale revision.
//
// This guards against a race when the pod template is updated and the revision is checked
// within the same reconcile pass: right after the spec Update the API server bumps
// Generation, but the controller has not yet recomputed Status.UpdateRevision (it still
// points at the previous template). Comparing then would wrongly conclude the pod already
// runs the desired revision and skip recycling it.
func GetStatefulSetUpdateRevisionObserved(
	ctx context.Context, c client.Client, clusterName, namespace string,
) (string, bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return "", false, fmt.Errorf("getting StatefulSet %s for update revision: %w", clusterName, err)
	}
	if sts.Status.ObservedGeneration < sts.Generation {
		return "", false, nil
	}
	return sts.Status.UpdateRevision, true, nil
}

// DeletePod deletes a specific pod by name. It is used during fast upgrade to force pod recreation.
func DeletePod(ctx context.Context, c client.Client, podName, namespace string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
	}
	if err := c.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting pod %s: %w", podName, err)
	}
	return nil
}

// DeleteAllPods deletes all pods belonging to the cluster. Used by fast upgrade.
func DeleteAllPods(ctx context.Context, c client.Client, clusterName, namespace string) error {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(clusterLabels(clusterName)),
	); err != nil {
		return fmt.Errorf("listing pods for cluster %s: %w", clusterName, err)
	}
	for i := range podList.Items {
		if err := c.Delete(ctx, &podList.Items[i]); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("deleting pod %s: %w", podList.Items[i].Name, err)
		}
	}
	return nil
}

// UpdateConfigMap updates the ConfigMap content for the cluster. This is called during
// an upgrade when redis.conf content changes.
func UpdateConfigMap(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyConfig, password string) error {
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, cm); err != nil {
		return fmt.Errorf("getting ConfigMap %s: %w", clusterName, err)
	}

	conf := buildRedisConf(config.Spec.RedisConfig, password, config.Spec.Ephemeral, config.Spec.IsStandalone())
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data["redis.conf"] = conf

	if err := c.Update(ctx, cm); err != nil {
		return fmt.Errorf("updating ConfigMap %s: %w", clusterName, err)
	}
	return nil
}

// ComputeConfigChecksum computes a SHA-256 checksum of the configuration fields that
// should trigger a pod restart when changed: image, version, and redis config content.
func ComputeConfigChecksum(image, version, redisConfig string) string {
	h := sha256.New()
	h.Write([]byte(image))
	h.Write([]byte("\x00"))
	h.Write([]byte(version))
	h.Write([]byte("\x00"))
	h.Write([]byte(redisConfig))
	return hex.EncodeToString(h.Sum(nil))[:16]
}
