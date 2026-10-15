// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

// standaloneOwner creates a standalone-mode Redkey owner.
func standaloneOwner(primaries int32) {
	owner := &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: redisv1.RedkeySpec{
			Mode:               redisv1.ModeStandalone,
			Primaries:          primaries,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
	Expect(k8sClient.Create(ctx, owner)).To(Succeed())
}

// newStandaloneConfig builds a standalone-mode config with the given sequence and topology.
func newStandaloneConfig(name string, seq int, primaries int32) *redisv1.RedkeyConfig {
	cfg := &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{clusterLabel: clusterName},
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           seq,
			Mode:               redisv1.ModeStandalone,
			Primaries:          primaries,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
		},
	}
	return cfg
}

// createAppliedStandaloneConfig creates a standalone config already in the
// Applied/Ready state, simulating an existing standalone cluster.
func createAppliedStandaloneConfig(name string, seq int, primaries int32) {
	cfg := newStandaloneConfig(name, seq, primaries)
	Expect(k8sClient.Create(ctx, cfg)).To(Succeed())
	cfg.Status = redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhaseApplied,
		Status:      redisv1.ClusterStatusReady,
		Nodes:       map[string]*redisv1.RedisNode{},
	}
	Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
}

var _ = Describe("Standalone Deployment (integration)", func() {
	AfterEach(func() {
		cleanup()
		deleteScalingResources()
	})

	Describe("New standalone cluster", func() {
		It("creates a single-replica StatefulSet with cluster support disabled", func() {
			standaloneOwner(1)

			cfg := newStandaloneConfig("standalone-new-1", 1, 1)
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Reaches Initializing (no kubelet in envtest, so it stays here).
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, timeout, interval).Should(Succeed())

			// Single-replica StatefulSet.
			Eventually(func(g Gomega) {
				var sts appsv1.StatefulSet
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
				g.Expect(sts.Spec.Replicas).NotTo(BeNil())
				g.Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
			}, timeout, interval).Should(Succeed())

			// ConfigMap disables Redis cluster mode.
			Eventually(func(g Gomega) {
				var cm corev1.ConfigMap
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &cm)).To(Succeed())
				conf := cm.Data["redis.conf"]
				g.Expect(strings.Contains(conf, "cluster-enabled no")).To(BeTrue())
				g.Expect(strings.Contains(conf, "cluster-enabled yes")).To(BeFalse())
			}, timeout, interval).Should(Succeed())

			// Headless Service is still created.
			Eventually(func(g Gomega) {
				var svc corev1.Service
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &svc)).To(Succeed())
				g.Expect(svc.Spec.ClusterIP).To(Equal("None"))
			}, timeout, interval).Should(Succeed())
		})

		It("does not create a PodDisruptionBudget for the single node", func() {
			standaloneOwner(1)
			cfg := newStandaloneConfig("standalone-nopdb-1", 1, 1)
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var sts appsv1.StatefulSet
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
			}, timeout, interval).Should(Succeed())

			Consistently(func(g Gomega) {
				var pdbList policyv1.PodDisruptionBudgetList
				g.Expect(k8sClient.List(ctx, &pdbList, client.InNamespace(testNamespace))).To(Succeed())
				g.Expect(pdbList.Items).To(BeEmpty())
			}, "2s", interval).Should(Succeed())
		})
	})

	Describe("Scale to zero and back", func() {
		It("deletes the StatefulSet when scaling a standalone cluster to zero", func() {
			standaloneOwner(0)
			scalingStatefulSet(1)
			createAppliedStandaloneConfig("standalone-stz-1", 1, 1)

			// New config requests zero primaries.
			target := newStandaloneConfig("standalone-stz-2", 2, 0)
			Expect(k8sClient.Create(ctx, target)).To(Succeed())
			target.Status = redisv1.RedkeyConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, target)).To(Succeed())

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// StatefulSet is removed.
			Eventually(func(g Gomega) {
				var sts appsv1.StatefulSet
				err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)
				g.Expect(client.IgnoreNotFound(err)).To(Succeed())
				g.Expect(err).To(HaveOccurred())
			}, timeout, interval).Should(Succeed())
		})

		It("recreates a single-replica StatefulSet when scaling from zero to one", func() {
			standaloneOwner(1)
			createAppliedStandaloneConfig("standalone-up-1", 1, 0)

			target := newStandaloneConfig("standalone-up-2", 2, 1)
			Expect(k8sClient.Create(ctx, target)).To(Succeed())
			target.Status = redisv1.RedkeyConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, target)).To(Succeed())

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var sts appsv1.StatefulSet
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
				g.Expect(sts.Spec.Replicas).NotTo(BeNil())
				g.Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Authenticated standalone cluster", func() {
		It("writes the auth credentials into the standalone redis.conf", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "standalone-auth-secret",
					Namespace: testNamespace,
				},
				Data: map[string][]byte{
					"password": []byte("standalone-secret-123"),
				},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, secret)
			})

			standaloneOwner(1)

			cfg := newStandaloneConfig("standalone-auth-1", 1, 1)
			cfg.Spec.Auth.SecretName = "standalone-auth-secret"
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Reaches Initializing (no kubelet in envtest, so it stays here).
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, timeout, interval).Should(Succeed())

			// ConfigMap carries the auth directives alongside cluster-enabled no.
			Eventually(func(g Gomega) {
				var cm corev1.ConfigMap
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &cm)).To(Succeed())
				conf := cm.Data["redis.conf"]
				g.Expect(strings.Contains(conf, "cluster-enabled no")).To(BeTrue())
				g.Expect(strings.Contains(conf, "requirepass standalone-secret-123")).To(BeTrue())
				g.Expect(strings.Contains(conf, "masterauth standalone-secret-123")).To(BeTrue())
			}, timeout, interval).Should(Succeed())
		})
	})
})
