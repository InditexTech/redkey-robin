// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

const invalidRedisImage = "redis:does-not-exist"

func expectConfigPhase(name, phase string) {
	EventuallyWithOffset(1, func(g Gomega) {
		var fetched redisv1.RedkeyConfig
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, &fetched)).To(Succeed())
		g.Expect(fetched.Status.ConfigPhase).To(Equal(phase))
	}, timeout, interval).Should(Succeed())
}

func expectConfigPhaseStays(name, phase string) {
	ConsistentlyWithOffset(1, func(g Gomega) {
		var fetched redisv1.RedkeyConfig
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, &fetched)).To(Succeed())
		g.Expect(fetched.Status.ConfigPhase).To(Equal(phase))
	}, 500*time.Millisecond, 100*time.Millisecond).Should(Succeed())
}

func expectClusterObjectsGone() {
	objs := []struct {
		name string
		obj  client.Object
	}{
		{clusterName, &appsv1.StatefulSet{}},
		{clusterName, &corev1.Service{}},
		{clusterName, &corev1.ConfigMap{}},
		{clusterName + "-pdb", &policyv1.PodDisruptionBudget{}},
	}
	for _, o := range objs {
		EventuallyWithOffset(1, func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{Name: o.name, Namespace: testNamespace}, o.obj)
			return apierrors.IsNotFound(err)
		}, timeout, interval).Should(BeTrue(), "expected %T %s to be deleted", o.obj, o.name)
	}
}

func createPlaceholderClusterObjects() {
	scalingStatefulSet(6)
	Expect(k8sClient.Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Ports:     []corev1.ServicePort{{Name: "redis", Port: 6379}},
		},
	})).To(Succeed())
	createConfigMap(clusterName)
	Expect(k8sClient.Create(ctx, &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-pdb", Namespace: testNamespace},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"redkey.inditex.dev/cluster": clusterName}},
		},
	})).To(Succeed())
}

func deletePDBs() {
	var pdbList policyv1.PodDisruptionBudgetList
	_ = k8sClient.List(ctx, &pdbList, client.InNamespace(testNamespace))
	for i := range pdbList.Items {
		_ = k8sClient.Delete(ctx, &pdbList.Items[i])
	}
}

var _ = Describe("Scale-to-zero lookahead (integration)", func() {
	AfterEach(func() {
		cleanup()
		deleteScalingResources()
		deletePDBs()
	})

	It("recovers an upgrade blocked by an invalid image by jumping to a pending scale-to-zero", func() {
		scalingOwner(3, 1, nil)
		createPlaceholderClusterObjects()

		createConfigWithStatus("lookahead-upg-1", baselineConfigSpec(1, 3, 1), redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseApplied,
			Status:      redisv1.ClusterStatusReady,
		})
		badImage := baselineConfigSpec(2, 3, 1)
		badImage.Image = invalidRedisImage
		createConfigWithStatus("lookahead-upg-2", badImage, redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})

		startScalingReconciler()

		// The upgrade starts and never completes (pods never become ready).
		expectConfigPhase("lookahead-upg-2", redisv1.ConfigPhaseInProgress)
		expectConfigStatus("lookahead-upg-2", redisv1.ClusterStatusUpgrading)
		expectConfigPhaseStays("lookahead-upg-2", redisv1.ConfigPhaseInProgress)

		// A corrective config that is NOT scale-to-zero stays blocked behind the wedged one.
		fix := baselineConfigSpec(3, 3, 1)
		createConfigWithStatus("lookahead-upg-3", fix, redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})
		expectConfigPhaseStays("lookahead-upg-3", redisv1.ConfigPhasePending)

		// Scaling to zero recovers the cluster.
		zero := baselineConfigSpec(4, 0, 1)
		createConfigWithStatus("lookahead-upg-4", zero, redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})

		expectConfigPhase("lookahead-upg-2", redisv1.ConfigPhaseSuperseded)
		expectConfigPhase("lookahead-upg-3", redisv1.ConfigPhaseSuperseded)
		expectConfigPhase("lookahead-upg-4", redisv1.ConfigPhaseApplied)
		expectConfigStatus("lookahead-upg-4", redisv1.ClusterStatusReady)
		expectClusterObjectsGone()
	})

	It("recovers a first config stuck initializing (no previous Applied config)", func() {
		scalingOwner(3, 0, nil)

		badImage := newConfig("lookahead-new-1", 1, 3, 0)
		badImage.Spec.Image = invalidRedisImage
		createAndSetPhase(badImage, redisv1.ConfigPhasePending)

		startScalingReconciler()

		// Robin creates the objects and waits forever for the pods.
		expectConfigStatus("lookahead-new-1", redisv1.ClusterStatusInitializing)
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &appsv1.StatefulSet{})
		}, timeout, interval).Should(Succeed())

		zero := newConfig("lookahead-new-2", 2, 0, 0)
		createAndSetPhase(zero, redisv1.ConfigPhasePending)

		expectConfigPhase("lookahead-new-1", redisv1.ConfigPhaseSuperseded)
		expectConfigPhase("lookahead-new-2", redisv1.ConfigPhaseApplied)
		expectClusterObjectsGone()
	})

	It("does not abandon an in-progress config on a storage cluster that keeps its PVCs", func() {
		scalingOwner(3, 1, nil)
		createPlaceholderClusterObjects()

		keep := false
		storageSpec := func(seq int, primaries int32) redisv1.RedkeyConfigSpec {
			spec := baselineConfigSpec(seq, primaries, 1)
			spec.Ephemeral = false
			spec.Storage = "1Gi"
			spec.DeletePVC = &keep
			return spec
		}

		wedged := storageSpec(1, 3)
		wedged.Image = invalidRedisImage
		createConfigWithStatus("lookahead-storage-1", wedged, redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseInProgress,
			Status:      redisv1.ClusterStatusInitializing,
		})
		createConfigWithStatus("lookahead-storage-2", storageSpec(2, 0), redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})

		startScalingReconciler()

		expectConfigPhaseStays("lookahead-storage-1", redisv1.ConfigPhaseInProgress)
		expectConfigPhaseStays("lookahead-storage-2", redisv1.ConfigPhasePending)
		stsKey := types.NamespacedName{Name: clusterName, Namespace: testNamespace}
		Expect(k8sClient.Get(ctx, stsKey, &appsv1.StatefulSet{})).To(Succeed())
	})

	It("supersedes a chain of pending configs and applies only the scale-to-zero", func() {
		scalingOwner(3, 1, nil)
		createPlaceholderClusterObjects()

		createConfigWithStatus("lookahead-chain-1", baselineConfigSpec(1, 3, 1), redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseApplied,
			Status:      redisv1.ClusterStatusReady,
		})
		createConfigWithStatus("lookahead-chain-2", baselineConfigSpec(2, 5, 1), redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})
		imageChange := baselineConfigSpec(3, 5, 1)
		imageChange.Image = "redis:8"
		createConfigWithStatus("lookahead-chain-3", imageChange, redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})
		createConfigWithStatus("lookahead-chain-4", baselineConfigSpec(4, 0, 1), redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhasePending,
		})

		startScalingReconciler()

		expectConfigPhase("lookahead-chain-2", redisv1.ConfigPhaseSuperseded)
		expectConfigPhase("lookahead-chain-3", redisv1.ConfigPhaseSuperseded)
		expectConfigPhase("lookahead-chain-4", redisv1.ConfigPhaseApplied)
		expectClusterObjectsGone()
	})
})
