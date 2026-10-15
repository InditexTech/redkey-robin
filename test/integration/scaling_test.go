// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

// scalingOwner creates the Redkey owner used by the scaling reconciler when it
// needs to (re)create cluster objects.
func scalingOwner(primaries, replicas int32, purge *bool) {
	owner := &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: redisv1.RedkeySpec{
			Primaries:            primaries,
			ReplicasPerPrimary:   replicas,
			Ephemeral:            true,
			Image:                "redis:7",
			PurgeKeysOnRebalance: purge,
		},
	}
	Expect(k8sClient.Create(ctx, owner)).To(Succeed())
}

// scalingStatefulSet creates a placeholder StatefulSet at the given size so the scaling
// handlers (which only adjust an existing StatefulSet) have something to act on.
func scalingStatefulSet(replicas int32) {
	labels := map[string]string{
		"redkey.inditex.dev/cluster":   clusterName,
		"redkey.inditex.dev/component": "redis",
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: testNamespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: clusterName,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "redis", Image: "redis:7"},
					},
				},
			},
		},
	}
	Expect(k8sClient.Create(ctx, sts)).To(Succeed())
}

// newScalingConfig builds a Pending config (sequence 2) for the new desired topology.
func newScalingConfig(name string, primaries, replicas int32, purge *bool) {
	cfg := &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{clusterLabel: clusterName},
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:             2,
			Primaries:            primaries,
			ReplicasPerPrimary:   replicas,
			Ephemeral:            true,
			Image:                "redis:7",
			Version:              "7.0",
			PurgeKeysOnRebalance: purge,
		},
	}
	Expect(k8sClient.Create(ctx, cfg)).To(Succeed())
	cfg.Status = redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhasePending,
		Nodes:       map[string]*redisv1.RedisNode{},
	}
	Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
}

func deleteScalingResources() {
	_ = k8sClient.Delete(ctx, &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
	})
	var stsList appsv1.StatefulSetList
	_ = k8sClient.List(ctx, &stsList, client.InNamespace(testNamespace))
	for i := range stsList.Items {
		_ = k8sClient.Delete(ctx, &stsList.Items[i])
	}
	var svcList corev1.ServiceList
	_ = k8sClient.List(ctx, &svcList, client.InNamespace(testNamespace))
	for i := range svcList.Items {
		if svcList.Items[i].Name == "kubernetes" {
			continue
		}
		_ = k8sClient.Delete(ctx, &svcList.Items[i])
	}
	var cmList corev1.ConfigMapList
	_ = k8sClient.List(ctx, &cmList, client.InNamespace(testNamespace))
	for i := range cmList.Items {
		_ = k8sClient.Delete(ctx, &cmList.Items[i])
	}
}

func statefulSetReplicas(g Gomega) int32 {
	var sts appsv1.StatefulSet
	g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
	g.Expect(sts.Spec.Replicas).NotTo(BeNil())
	return *sts.Spec.Replicas
}

// startScalingReconciler runs Robin for the test cluster until the spec finishes.
func startScalingReconciler() {
	rec := newIntegrationReconciler(newTestRuntimeConfig())
	loopCancel, errCh := startReconcilerLoop(rec)
	DeferCleanup(stopReconcilerLoop, loopCancel, errCh)
}

func expectConfigStatus(name, status string) {
	Eventually(func(g Gomega) {
		var fetched redisv1.RedkeyConfig
		key := types.NamespacedName{Name: name, Namespace: testNamespace}
		g.Expect(k8sClient.Get(ctx, key, &fetched)).To(Succeed())
		g.Expect(fetched.Status.Status).To(Equal(status))
	}, timeout, interval).Should(Succeed())
}

func expectStatefulSetReplicas(replicas int32) {
	Eventually(func(g Gomega) {
		g.Expect(statefulSetReplicas(g)).To(Equal(replicas))
	}, timeout, interval).Should(Succeed())
}

var _ = Describe("Cluster Scaling (integration)", func() {
	AfterEach(func() {
		cleanup()
		deleteScalingResources()
	})

	Describe("Scale up", func() {
		It("grows the StatefulSet to the new node count when primaries increase", func() {
			scalingOwner(6, 0, nil)
			scalingStatefulSet(3)
			createAppliedConfig("scale-up-1", 3, 0)
			newScalingConfig("scale-up-2", 6, 0, nil)
			startScalingReconciler()

			expectConfigStatus("scale-up-2", redisv1.ClusterStatusScalingUp)
			// StatefulSet is scaled out to the new total node count.
			expectStatefulSetReplicas(6)
		})

		It("grows the StatefulSet when replicas-per-primary increase", func() {
			scalingOwner(3, 1, nil)
			scalingStatefulSet(3)
			createAppliedConfig("scale-up-rep-1", 3, 0)
			newScalingConfig("scale-up-rep-2", 3, 1, nil)
			startScalingReconciler()

			// 3 primaries + 3*1 replicas = 6 nodes.
			expectStatefulSetReplicas(6)
		})
	})

	Describe("Scale down", func() {
		It("shrinks the StatefulSet to the new node count when primaries decrease", func() {
			scalingOwner(3, 0, nil)
			scalingStatefulSet(6)
			createAppliedConfig("scale-down-1", 6, 0)
			newScalingConfig("scale-down-2", 3, 0, nil)
			startScalingReconciler()

			expectConfigStatus("scale-down-2", redisv1.ClusterStatusScalingDown)
			// With no live Redis pods the handler observes zero current nodes and shrinks
			// the StatefulSet straight to the target size.
			expectStatefulSetReplicas(3)
		})
	})

	Describe("Fast scaling", func() {
		It("recreates the StatefulSet at the new size for eligible clusters", func() {
			purge := true
			scalingOwner(6, 0, &purge)
			scalingStatefulSet(3)
			createAppliedConfig("fast-scale-1", 3, 0)
			newScalingConfig("fast-scale-2", 6, 0, &purge)
			startScalingReconciler()

			// The StatefulSet is deleted and recreated at the new size.
			expectStatefulSetReplicas(6)
		})

		It("does not use fast scaling when transitioning from replicas>0 to replicas=0", func() {
			// Target: 3 primaries, 0 replicas, ephemeral, purge=true — eligible by
			// config, but previous config had replicas so the cluster has replica nodes.
			// Fast scaling must NOT be used (it would destroy data on replicas).
			purge := true
			scalingOwner(3, 0, &purge)
			// Previous topology: 3P + 3R = 6 pods.
			scalingStatefulSet(6)
			createAppliedConfig("fast-norep-1", 3, 1)
			// New config drops replicas to 0 (scale down from 6 to 3).
			newScalingConfig("fast-norep-2", 3, 0, &purge)
			startScalingReconciler()

			// Status should indicate ScalingDown (normal path), NOT fast scaling.
			expectConfigStatus("fast-norep-2", redisv1.ClusterStatusScalingDown)

			// StatefulSet should be shrunk (not deleted+recreated). Since no live Redis
			// pods exist, the handler immediately shrinks to target.
			expectStatefulSetReplicas(3)

			// Verify it still exists (fast scaling would have deleted it).
			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
		})
	})
})
