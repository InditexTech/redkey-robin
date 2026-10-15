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
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/kubernetes"
)

var _ = Describe("Object Overrides (integration)", func() {
	AfterEach(func() {
		cleanup()
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
	})

	createOwner := func() {
		owner := &redisv1.Redkey{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: redisv1.RedkeySpec{
				Primaries:          3,
				ReplicasPerPrimary: 0,
				Ephemeral:          true,
				Image:              "redis:7",
			},
		}
		Expect(k8sClient.Create(ctx, owner)).To(Succeed())
	}

	waitForInitializing := func(cfg *redisv1.RedkeyConfig) {
		Eventually(func(g Gomega) {
			var fetched redisv1.RedkeyConfig
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
			g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
		}, timeout, interval).Should(Succeed())
	}

	Describe("StatefulSet override on cluster creation", func() {
		It("applies pod template scheduling fields while preserving identity", func() {
			createOwner()

			cfg := newConfig("override-sts-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			cfg.Spec.Override = &redisv1.RedkeyOverrideSpec{
				StatefulSet: &redisv1.PartialStatefulSet{
					Metadata: metav1.ObjectMeta{
						Annotations: map[string]string{"backup.io/enabled": "true"},
					},
					Spec: &redisv1.PartialStatefulSetSpec{
						Template: &redisv1.PartialPodTemplateSpec{
							Spec: redisv1.PartialPodSpec{
								NodeSelector: map[string]string{"disktype": "ssd"},
								Tolerations: []corev1.Toleration{
									{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "redis", Effect: corev1.TaintEffectNoSchedule},
								},
							},
						},
					},
				},
			}
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			waitForInitializing(cfg)

			var sts appsv1.StatefulSet
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
				g.Expect(sts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("disktype", "ssd"))
			}, timeout, interval).Should(Succeed())

			Expect(sts.Spec.Template.Spec.Tolerations).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.Tolerations[0].Key).To(Equal("dedicated"))
			Expect(sts.Annotations).To(HaveKeyWithValue("backup.io/enabled", "true"))

			// Identity preserved.
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.ServiceName).To(Equal(clusterName))
			Expect(sts.Spec.Template.Spec.Containers[0].Name).To(Equal("redis"))
		})
	})

	Describe("Service override on cluster creation", func() {
		It("applies extra ports and annotations while staying headless", func() {
			createOwner()

			cfg := newConfig("override-svc-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			cfg.Spec.Override = &redisv1.RedkeyOverrideSpec{
				Service: &redisv1.PartialService{
					Metadata: metav1.ObjectMeta{
						Annotations: map[string]string{"team": "infra"},
					},
					Spec: &redisv1.PartialServiceSpec{
						Ports: []corev1.ServicePort{
							{Name: "metrics", Port: 9121, TargetPort: intstr.FromInt(9121)},
						},
					},
				},
			}
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			waitForInitializing(cfg)

			var svc corev1.Service
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &svc)).To(Succeed())
				g.Expect(svc.Annotations).To(HaveKeyWithValue("team", "infra"))
			}, timeout, interval).Should(Succeed())

			portNames := map[string]bool{}
			for _, p := range svc.Spec.Ports {
				portNames[p.Name] = true
			}
			Expect(portNames).To(HaveKey("metrics"))
			Expect(portNames).To(HaveKey("client"))
			Expect(portNames).To(HaveKey("gossip"))

			// Identity preserved.
			Expect(svc.Spec.ClusterIP).To(Equal("None"))
		})
	})

	Describe("No override (backward compatibility)", func() {
		It("creates default objects with no extra scheduling fields", func() {
			createOwner()

			cfg := newConfig("override-none-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			waitForInitializing(cfg)

			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.NodeSelector).To(BeNil())
			Expect(sts.Spec.Template.Spec.Tolerations).To(BeEmpty())
		})
	})

	Describe("StatefulSet override on upgrade (existing StatefulSet)", func() {
		// This drives UpdateStatefulSetTemplate — the upgrade-path function that
		// reconciles an ALREADY-EXISTING StatefulSet. It is the exact code path that
		// regressed: the creation path (ensureStatefulSet) and the pod-template-only
		// update both worked, so unit/integration/e2e suites that only exercised those
		// missed the bug where top-level StatefulSet metadata and other spec fields
		// were dropped on update.
		It("applies top-level metadata and merges pod template metadata on the existing StatefulSet", func() {
			owner := &redisv1.Redkey{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
				Spec: redisv1.RedkeySpec{
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())

			// Create the initial cluster objects with no override (creation path).
			cfg := newConfig("override-upgrade-1", 1, 3, 0)
			cfg.Spec.Ephemeral = true
			Expect(kubernetes.EnsureClusterObjects(ctx, k8sClient, cfg, owner, "")).To(Succeed())

			stsKey := types.NamespacedName{Name: clusterName, Namespace: testNamespace}
			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, stsKey, &sts)).To(Succeed())
			Expect(sts.Labels).NotTo(HaveKey("tier"))
			Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue(clusterLabel, clusterName))

			// Apply an override that adds StatefulSet-level metadata AND pod template
			// metadata, then drive the exact upgrade-path function on the existing object.
			cfg.Spec.Override = &redisv1.RedkeyOverrideSpec{
				StatefulSet: &redisv1.PartialStatefulSet{
					Metadata: metav1.ObjectMeta{
						Annotations: map[string]string{"backup.io/enabled": "true"},
						Labels:      map[string]string{"tier": "cache"},
					},
					Spec: &redisv1.PartialStatefulSetSpec{
						Template: &redisv1.PartialPodTemplateSpec{
							Metadata: metav1.ObjectMeta{
								Annotations: map[string]string{"sidecar.io/inject": "false"},
								Labels:      map[string]string{"pod-tier": "cache"},
							},
							Spec: redisv1.PartialPodSpec{
								Tolerations: []corev1.Toleration{
									{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "redis", Effect: corev1.TaintEffectNoSchedule},
								},
							},
						},
					},
				},
			}

			checksum := kubernetes.ComputeConfigChecksum(cfg.Spec.Image, cfg.Spec.Version, cfg.Spec.RedisConfig)
			Expect(kubernetes.UpdateStatefulSetTemplate(
				ctx, k8sClient, clusterName, testNamespace, cfg, owner, checksum)).To(Succeed())

			Expect(k8sClient.Get(ctx, stsKey, &sts)).To(Succeed())

			// Top-level StatefulSet metadata is applied (the regression).
			Expect(sts.Annotations).To(HaveKeyWithValue("backup.io/enabled", "true"))
			Expect(sts.Labels).To(HaveKeyWithValue("tier", "cache"))

			// Pod template metadata is MERGED with the existing pod labels, not replaced.
			Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("pod-tier", "cache"))
			Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue(clusterLabel, clusterName))
			Expect(sts.Spec.Template.Annotations).To(HaveKeyWithValue("sidecar.io/inject", "false"))
			Expect(sts.Spec.Template.Annotations).To(HaveKeyWithValue(kubernetes.ConfigChecksumAnnotation, checksum))

			// Pod scheduling override is applied.
			Expect(sts.Spec.Template.Spec.Tolerations).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.Tolerations[0].Key).To(Equal("dedicated"))

			// Identity is preserved.
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.ServiceName).To(Equal(clusterName))
			Expect(sts.Spec.Template.Spec.Containers[0].Name).To(Equal("redis"))
			Expect(sts.Labels).To(HaveKeyWithValue(clusterLabel, clusterName))

			// Removing the override reverts the StatefulSet while the cluster label survives.
			cfg.Spec.Override = nil
			Expect(kubernetes.UpdateStatefulSetTemplate(
				ctx, k8sClient, clusterName, testNamespace, cfg, owner, checksum)).To(Succeed())

			Expect(k8sClient.Get(ctx, stsKey, &sts)).To(Succeed())
			Expect(sts.Annotations).NotTo(HaveKey("backup.io/enabled"))
			Expect(sts.Labels).NotTo(HaveKey("tier"))
			Expect(sts.Labels).To(HaveKeyWithValue(clusterLabel, clusterName))
			Expect(sts.Spec.Template.Labels).NotTo(HaveKey("pod-tier"))
			Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue(clusterLabel, clusterName))
			Expect(sts.Spec.Template.Annotations).NotTo(HaveKey("sidecar.io/inject"))
			Expect(sts.Spec.Template.Spec.Tolerations).To(BeEmpty())
		})
	})
})
