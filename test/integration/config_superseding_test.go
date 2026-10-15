// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/reconciler"
)

func newSkippableConfig(name string, seq int, primaries, replicas int32, skip bool) *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{clusterLabel: clusterName},
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           seq,
			SkipIfSuperseded:   skip,
			Primaries:          primaries,
			ReplicasPerPrimary: replicas,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
		},
	}
}

var _ = Describe("Configuration Superseding (integration)", func() {
	AfterEach(func() {
		cleanup()
	})

	It("supersedes intermediate topology-only configs (recently created - No Phase)", func() {
		cfg1 := newSkippableConfig("sup-chain-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-chain-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, "")

		cfg3 := newSkippableConfig("sup-chain-3", 3, 7, 1, false)
		createAndSetPhase(cfg3, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sup-chain-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		Expect(result.Selected.Name).To(Equal("sup-chain-3"))
		Expect(result.Superseded).To(HaveLen(1))
		Expect(result.Superseded[0].Name).To(Equal("sup-chain-2"))

		// Simulate what the reconciler does: write Superseded status.
		for _, cfg := range result.Superseded {
			cfg.Status.ConfigPhase = redisv1.ConfigPhaseSuperseded
			Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
		}

		// Verify persisted status.
		Eventually(func() string {
			var fetched redisv1.RedkeyConfig
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(result.Superseded[0]), &fetched)
			if err != nil {
				return ""
			}
			return fetched.Status.ConfigPhase
		}, 10*time.Second, 250*time.Millisecond).Should(Equal(redisv1.ConfigPhaseSuperseded))
	})

	It("supersedes intermediate topology-only configs", func() {
		cfg1 := newSkippableConfig("sup-chain-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-chain-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

		cfg3 := newSkippableConfig("sup-chain-3", 3, 7, 1, false)
		createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sup-chain-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		Expect(result.Selected.Name).To(Equal("sup-chain-3"))
		Expect(result.Superseded).To(HaveLen(1))
		Expect(result.Superseded[0].Name).To(Equal("sup-chain-2"))

		// Simulate what the reconciler does: write Superseded status.
		for _, cfg := range result.Superseded {
			cfg.Status.ConfigPhase = redisv1.ConfigPhaseSuperseded
			Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
		}

		// Verify persisted status.
		Eventually(func() string {
			var fetched redisv1.RedkeyConfig
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(result.Superseded[0]), &fetched)
			if err != nil {
				return ""
			}
			return fetched.Status.ConfigPhase
		}, 10*time.Second, 250*time.Millisecond).Should(Equal(redisv1.ConfigPhaseSuperseded))
	})

	It("does not supersede when config has non-topology changes (recently created - No Phase)", func() {
		cfg1 := newSkippableConfig("sup-nontopo-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-nontopo-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, "")

		cfg3 := newSkippableConfig("sup-nontopo-3", 3, 7, 1, false)
		cfg3.Spec.Image = "redis:8" // non-topology change
		createAndSetPhase(cfg3, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-nontopo-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		// Chain breaks: cfg-3 has a different image.
		Expect(result.Selected.Name).To(Equal("sup-nontopo-2"))
		Expect(result.Superseded).To(BeEmpty())
	})

	It("does not supersede when config has non-topology changes", func() {
		cfg1 := newSkippableConfig("sup-nontopo-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-nontopo-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

		cfg3 := newSkippableConfig("sup-nontopo-3", 3, 7, 1, false)
		cfg3.Spec.Image = "redis:8" // non-topology change
		createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-nontopo-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		// Chain breaks: cfg-3 has a different image.
		Expect(result.Selected.Name).To(Equal("sup-nontopo-2"))
		Expect(result.Superseded).To(BeEmpty())
	})

	It("supersedes a chain of three topology-only configs (recently created - No Phase)", func() {
		cfg1 := newSkippableConfig("sup-three-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-three-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, "")

		cfg3 := newSkippableConfig("sup-three-3", 3, 7, 1, true)
		createAndSetPhase(cfg3, "")

		cfg4 := newSkippableConfig("sup-three-4", 4, 9, 2, false)
		createAndSetPhase(cfg4, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-three-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		Expect(result.Selected.Name).To(Equal("sup-three-4"))
		Expect(result.Superseded).To(HaveLen(2))
		Expect(result.Superseded[0].Name).To(Equal("sup-three-2"))
		Expect(result.Superseded[1].Name).To(Equal("sup-three-3"))

		// Write superseded status.
		for _, cfg := range result.Superseded {
			cfg.Status.ConfigPhase = redisv1.ConfigPhaseSuperseded
			Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
		}

		// Verify both are superseded in the cluster.
		for _, cfg := range result.Superseded {
			Eventually(func() string {
				var fetched redisv1.RedkeyConfig
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)
				if err != nil {
					return ""
				}
				return fetched.Status.ConfigPhase
			}, 10*time.Second, 250*time.Millisecond).Should(Equal(redisv1.ConfigPhaseSuperseded))
		}
	})

	It("supersedes a chain of three topology-only configs", func() {
		cfg1 := newSkippableConfig("sup-three-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-three-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

		cfg3 := newSkippableConfig("sup-three-3", 3, 7, 1, true)
		createAndSetPhase(cfg3, "")

		cfg4 := newSkippableConfig("sup-three-4", 4, 9, 2, false)
		createAndSetPhase(cfg4, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-three-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		Expect(result.Selected.Name).To(Equal("sup-three-4"))
		Expect(result.Superseded).To(HaveLen(2))
		Expect(result.Superseded[0].Name).To(Equal("sup-three-2"))
		Expect(result.Superseded[1].Name).To(Equal("sup-three-3"))

		// Write superseded status.
		for _, cfg := range result.Superseded {
			cfg.Status.ConfigPhase = redisv1.ConfigPhaseSuperseded
			Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
		}

		// Verify both are superseded in the cluster.
		for _, cfg := range result.Superseded {
			Eventually(func() string {
				var fetched redisv1.RedkeyConfig
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)
				if err != nil {
					return ""
				}
				return fetched.Status.ConfigPhase
			}, 10*time.Second, 250*time.Millisecond).Should(Equal(redisv1.ConfigPhaseSuperseded))
		}
	})

	It("stops chain when middle config lacks skipIfSuperseded", func() {
		cfg1 := newSkippableConfig("sup-mid-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-mid-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

		cfg3 := newSkippableConfig("sup-mid-3", 3, 7, 1, false) // no skip
		createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

		cfg4 := newSkippableConfig("sup-mid-4", 4, 9, 2, true)
		createAndSetPhase(cfg4, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-mid-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		// cfg-3 is selected but chain stops (skipIfSuperseded=false).
		Expect(result.Selected.Name).To(Equal("sup-mid-3"))
		Expect(result.Superseded).To(HaveLen(1))
		Expect(result.Superseded[0].Name).To(Equal("sup-mid-2"))
	})

	It("does not supersede InProgress config (recently created - No Phase)", func() {
		cfg1 := newSkippableConfig("sup-ip-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-ip-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, redisv1.ConfigPhaseInProgress)

		cfg3 := newSkippableConfig("sup-ip-3", 3, 7, 1, false)
		createAndSetPhase(cfg3, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-ip-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		Expect(result.Selected.Name).To(Equal("sup-ip-2"))
		Expect(result.Superseded).To(BeEmpty())
	})

	It("does not supersede InProgress config", func() {
		cfg1 := newSkippableConfig("sup-ip-1", 1, 3, 1, false)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newSkippableConfig("sup-ip-2", 2, 5, 1, true)
		createAndSetPhase(cfg2, redisv1.ConfigPhaseInProgress)

		cfg3 := newSkippableConfig("sup-ip-3", 3, 7, 1, false)
		createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected.Name).To(Equal("sup-ip-2"))

		result := reconciler.ApplySuperseding(configs, selected)
		Expect(result.Selected.Name).To(Equal("sup-ip-2"))
		Expect(result.Superseded).To(BeEmpty())
	})
})
