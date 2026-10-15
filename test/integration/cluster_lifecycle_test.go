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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

var _ = Describe("Cluster Lifecycle (integration)", func() {
	AfterEach(func() {
		cleanup()
		// Clean up Redkey
		var cluster redisv1.Redkey
		_ = k8sClient.Delete(ctx, &redisv1.Redkey{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		})
		_ = cluster // suppress unused
		// Clean up StatefulSets
		var stsList appsv1.StatefulSetList
		_ = k8sClient.List(ctx, &stsList, client.InNamespace(testNamespace))
		for i := range stsList.Items {
			_ = k8sClient.Delete(ctx, &stsList.Items[i])
		}
		// Clean up Services
		var svcList corev1.ServiceList
		_ = k8sClient.List(ctx, &svcList, client.InNamespace(testNamespace))
		for i := range svcList.Items {
			if svcList.Items[i].Name == "kubernetes" {
				continue
			}
			_ = k8sClient.Delete(ctx, &svcList.Items[i])
		}
		// Clean up ConfigMaps
		var cmList corev1.ConfigMapList
		_ = k8sClient.List(ctx, &cmList, client.InNamespace(testNamespace))
		for i := range cmList.Items {
			_ = k8sClient.Delete(ctx, &cmList.Items[i])
		}
	})

	Describe("New cluster creation", func() {
		It("transitions from empty status to Initializing and creates K8s objects", func() {
			// Create the Redkey owner
			owner := &redisv1.Redkey{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: testNamespace,
				},
				Spec: redisv1.RedkeySpec{
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())

			// Create a config with empty status (new cluster)
			cfg := newConfig("lifecycle-new-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			// Start reconciler
			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Config should transition to InProgress
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
			}, timeout, interval).Should(Succeed())

			// Status should reach Initializing (K8s objects created)
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, timeout, interval).Should(Succeed())

			// Verify StatefulSet was created
			Eventually(func(g Gomega) {
				var sts appsv1.StatefulSet
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
				g.Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			}, timeout, interval).Should(Succeed())

			// Verify headless Service was created
			Eventually(func(g Gomega) {
				var svc corev1.Service
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &svc)).To(Succeed())
				g.Expect(svc.Spec.ClusterIP).To(Equal("None"))
			}, timeout, interval).Should(Succeed())

			// Verify ConfigMap was created
			Eventually(func(g Gomega) {
				var cm corev1.ConfigMap
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &cm)).To(Succeed())
				g.Expect(cm.Data).To(HaveKey("redis.conf"))
			}, timeout, interval).Should(Succeed())
		})

		It("creates objects with owner references pointing to Redkey", func() {
			owner := &redisv1.Redkey{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: testNamespace,
				},
				Spec: redisv1.RedkeySpec{
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())

			cfg := newConfig("lifecycle-ownerref-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Wait for Initializing status (objects created)
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, timeout, interval).Should(Succeed())

			// Verify owner reference on StatefulSet
			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
			Expect(sts.OwnerReferences).To(HaveLen(1))
			Expect(sts.OwnerReferences[0].Name).To(Equal(clusterName))
			Expect(sts.OwnerReferences[0].Kind).To(Equal("Redkey"))
		})

		It("stays in Initializing while pods are not ready", func() {
			owner := &redisv1.Redkey{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: testNamespace,
				},
				Spec: redisv1.RedkeySpec{
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())

			cfg := newConfig("lifecycle-wait-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Wait for Initializing
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, timeout, interval).Should(Succeed())

			// It should stay in Initializing (pods not ready in envtest)
			Consistently(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, 500*time.Millisecond, 100*time.Millisecond).Should(Succeed())
		})

		It("handles password-protected cluster (reads secret)", func() {
			// Create auth secret
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-auth",
					Namespace: testNamespace,
				},
				Data: map[string][]byte{
					"password": []byte("secret123"),
				},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, secret)
			})

			owner := &redisv1.Redkey{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: testNamespace,
				},
				Spec: redisv1.RedkeySpec{
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())

			cfg := newConfig("lifecycle-auth-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			cfg.Spec.Auth.SecretName = "redis-auth"
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Should still reach Initializing even with auth
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
			}, timeout, interval).Should(Succeed())

			// Verify ConfigMap contains auth settings
			var cm corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &cm)).To(Succeed())
			Expect(cm.Data["redis.conf"]).To(ContainSubstring("requirepass secret123"))
			Expect(cm.Data["redis.conf"]).To(ContainSubstring("masterauth secret123"))
		})
	})
})
