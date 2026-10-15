// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/reconciler"
)

const (
	testNamespace = "default"
	clusterName   = "test-cluster"
	clusterLabel  = "redkey.inditex.dev/cluster"
	timeout       = 10 * time.Second
	interval      = 250 * time.Millisecond
)

func newConfig(name string, seq int, primaries, replicas int32) *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{clusterLabel: clusterName},
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           seq,
			Primaries:          primaries,
			ReplicasPerPrimary: replicas,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
		},
	}
}

// createAndSetPhase creates the CR and then sets its status ConfigPhase.
func createAndSetPhase(cfg *redisv1.RedkeyConfig, phase string) {
	Expect(k8sClient.Create(ctx, cfg)).To(Succeed())

	// Update status subresource.
	cfg.Status = redisv1.RedkeyConfigStatus{
		ConfigPhase: phase,
		Nodes:       map[string]*redisv1.RedisNode{},
	}
	Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
}

func listSorted() []redisv1.RedkeyConfig {
	var list redisv1.RedkeyConfigList
	ExpectWithOffset(1, k8sClient.List(ctx, &list,
		client.InNamespace(testNamespace),
		client.MatchingLabels{clusterLabel: clusterName},
	)).To(Succeed())
	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].Spec.Sequence < list.Items[j].Spec.Sequence
	})
	return list.Items
}

func cleanup() {
	var list redisv1.RedkeyConfigList
	_ = k8sClient.List(ctx, &list, client.InNamespace(testNamespace))
	for i := range list.Items {
		_ = k8sClient.Delete(ctx, &list.Items[i])
	}
	// Wait until all configs are gone.
	Eventually(func() int {
		var l redisv1.RedkeyConfigList
		_ = k8sClient.List(ctx, &l, client.InNamespace(testNamespace))
		return len(l.Items)
	}, timeout, interval).Should(Equal(0))

	// Clean up ConfigMaps created for auth integration tests.
	var cms corev1.ConfigMapList
	_ = k8sClient.List(ctx, &cms, client.InNamespace(testNamespace))
	for i := range cms.Items {
		_ = k8sClient.Delete(ctx, &cms.Items[i])
	}
}

func createConfigMap(name string) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Data: map[string]string{"redis.conf": ""},
	}
	Expect(k8sClient.Create(ctx, cm)).To(Succeed())
}

var _ = Describe("Configuration Selection (integration)", func() {
	AfterEach(func() {
		cleanup()
	})

	It("selects the single config when only one exists (recently created - No Phase)", func() {
		cfg := newConfig("sel-single-1", 1, 3, 1)
		createAndSetPhase(cfg, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sel-single-1"))
	})

	It("selects the single config when only one exists", func() {
		cfg := newConfig("sel-single-1", 1, 3, 1)
		createAndSetPhase(cfg, redisv1.ConfigPhasePending)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sel-single-1"))
	})

	It("selects the first non-Applied config (recently created - No Phase)", func() {
		cfg1 := newConfig("sel-first-1", 1, 3, 1)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newConfig("sel-first-2", 2, 5, 1)
		createAndSetPhase(cfg2, "")

		cfg3 := newConfig("sel-first-3", 3, 7, 1)
		createAndSetPhase(cfg3, "")

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sel-first-2"))
	})

	It("selects the first non-Applied config", func() {
		cfg1 := newConfig("sel-first-1", 1, 3, 1)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newConfig("sel-first-2", 2, 5, 1)
		createAndSetPhase(cfg2, redisv1.ConfigPhasePending)

		cfg3 := newConfig("sel-first-3", 3, 7, 1)
		createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sel-first-2"))
	})

	It("selects the highest-sequence config when all are Applied", func() {
		cfg1 := newConfig("sel-all-1", 1, 3, 1)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newConfig("sel-all-2", 2, 5, 1)
		createAndSetPhase(cfg2, redisv1.ConfigPhaseApplied)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sel-all-2"))
	})

	It("selects the InProgress config over later Pending ones", func() {
		cfg1 := newConfig("sel-ip-1", 1, 3, 1)
		createAndSetPhase(cfg1, redisv1.ConfigPhaseApplied)

		cfg2 := newConfig("sel-ip-2", 2, 5, 1)
		createAndSetPhase(cfg2, redisv1.ConfigPhaseInProgress)

		cfg3 := newConfig("sel-ip-3", 3, 7, 1)
		createAndSetPhase(cfg3, redisv1.ConfigPhasePending)

		configs := listSorted()
		_, selected := reconciler.SelectConfig(configs)
		Expect(selected).NotTo(BeNil())
		Expect(selected.Name).To(Equal("sel-ip-2"))
	})
})
