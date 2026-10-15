// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
	"github.com/inditextech/redkey-robin/internal/reconciler"
)

func startReconcilerLoop(rec *reconciler.Reconciler) (context.CancelFunc, <-chan error) {
	loopCtx, loopCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- rec.Start(loopCtx)
	}()
	return loopCancel, errCh
}

func stopReconcilerLoop(loopCancel context.CancelFunc, errCh <-chan error) {
	loopCancel()
	Eventually(errCh, timeout, interval).Should(Receive(BeNil()))
}

func newTestRuntimeConfig() *config.RuntimeConfig {
	return config.NewRuntimeConfigWithReconcilerIntervals(
		50*time.Millisecond,
		50*time.Millisecond,
		50*time.Millisecond,
	)
}

func newIntegrationReconciler(rtConfig *config.RuntimeConfig) *reconciler.Reconciler {
	return reconciler.NewReconciler(k8sClient, clusterName, testNamespace, rtConfig)
}

var _ = Describe("Reconciler (integration)", func() {
	AfterEach(func() {
		cleanup()
	})

	Describe("Start loop processes configs end-to-end", func() {
		It("transitions a Pending config to InProgress", func() {
			cfg1 := newConfig("rec-cycle-1", 1, 3, 1)
			createAndSetPhase(cfg1, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg1), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
			}, timeout, interval).Should(Succeed())
		})

		It("initialises a config with empty phase to Pending then InProgress", func() {
			cfg1 := &redisv1.RedkeyConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rec-init-1",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyConfigSpec{
					Sequence:  1,
					Primaries: 3,
					Ephemeral: true,
					Image:     "redis:7",
					Version:   "7.0",
				},
			}
			Expect(k8sClient.Create(ctx, cfg1)).To(Succeed())

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg1), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
			}, timeout, interval).Should(Succeed())
		})

		It("resumes an InProgress config without changing phase", func() {
			cfg1 := newConfig("rec-resume-1", 1, 3, 1)
			createAndSetPhase(cfg1, redisv1.ConfigPhaseInProgress)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Consistently(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg1), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
			}, 200*time.Millisecond, interval).Should(Succeed())
		})

		It("selects the first non-Applied config when multiple exist", func() {
			cfg1 := newConfig("rec-multi-1", 1, 3, 1)
			createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

			cfg2 := newConfig("rec-multi-2", 2, 5, 1)
			createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

			cfg3 := newConfig("rec-multi-3", 3, 7, 1)
			createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched2 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched2)).To(Succeed())
				g.Expect(fetched2.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))

				var fetched3 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg3), &fetched3)).To(Succeed())
				g.Expect(fetched3.Status.ConfigPhase).To(Equal(redisv1.ConfigPhasePending))
			}, timeout, interval).Should(Succeed())
		})

		It("supersedes intermediate configs and transitions final one to InProgress", func() {
			cfg1 := newConfig("rec-sup-1", 1, 3, 1)
			createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

			cfg2 := newSkippableConfig("rec-sup-2", 2, 5, 1, true)
			createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

			cfg3 := newSkippableConfig("rec-sup-3", 3, 7, 1, true)
			createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

			cfg4 := newConfig("rec-sup-4", 4, 9, 2)
			createAndSetPhase(cfg4, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched2 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched2)).To(Succeed())
				g.Expect(fetched2.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseSuperseded))

				var fetched3 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg3), &fetched3)).To(Succeed())
				g.Expect(fetched3.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseSuperseded))

				var fetched4 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg4), &fetched4)).To(Succeed())
				g.Expect(fetched4.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
			}, timeout, interval).Should(Succeed())
		})

		It("does not supersede when non-topology changes exist", func() {
			cfg1 := newConfig("rec-nontopo-1", 1, 3, 1)
			createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

			cfg2 := newSkippableConfig("rec-nontopo-2", 2, 5, 1, true)
			createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

			cfg3 := newSkippableConfig("rec-nontopo-3", 3, 7, 1, false)
			cfg3.Spec.Image = "redis:8" // non-topology change
			Expect(k8sClient.Create(ctx, cfg3)).To(Succeed())
			cfg3.Status = redisv1.RedkeyConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg3)).To(Succeed())

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched2 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched2)).To(Succeed())
				g.Expect(fetched2.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))

				var fetched3 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg3), &fetched3)).To(Succeed())
				g.Expect(fetched3.Status.ConfigPhase).To(Equal(redisv1.ConfigPhasePending))
			}, timeout, interval).Should(Succeed())
		})

		It("stops cleanly with no configs", func() {
			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Consistently(func(g Gomega) {
				g.Expect(listSorted()).To(BeEmpty())
			}, 200*time.Millisecond, interval).Should(Succeed())
		})

		It("does not supersede an InProgress config even with later pending ones", func() {
			cfg1 := newConfig("rec-ip-1", 1, 3, 1)
			createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

			cfg2 := newSkippableConfig("rec-ip-2", 2, 5, 1, true)
			createAndSetPhase(cfg2, redisv1.ConfigPhaseInProgress)

			cfg3 := newConfig("rec-ip-3", 3, 7, 1)
			createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Consistently(func(g Gomega) {
				var fetched2 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched2)).To(Succeed())
				g.Expect(fetched2.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))

				var fetched3 redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg3), &fetched3)).To(Succeed())
				g.Expect(fetched3.Status.ConfigPhase).To(Equal(redisv1.ConfigPhasePending))
			}, 200*time.Millisecond, interval).Should(Succeed())
		})
	})
})
