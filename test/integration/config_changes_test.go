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
)

// baselineConfigSpec returns the spec of an existing cluster that the change-detection cases mutate.
func baselineConfigSpec(seq int, primaries, replicas int32) redisv1.RedkeyConfigSpec {
	return redisv1.RedkeyConfigSpec{
		Sequence:           seq,
		Primaries:          primaries,
		ReplicasPerPrimary: replicas,
		Ephemeral:          true,
		Image:              "redis:7",
		Version:            "7.0",
		RedisConfig:        "maxmemory 100mb",
		RobinConfig: &redisv1.RobinConfig{
			Reconciler: &redisv1.RobinConfigReconciler{
				IntervalSeconds: new(30),
			},
			Metrics: &redisv1.RobinConfigMetrics{
				CollectionIntervalSeconds: new(60),
			},
		},
	}
}

// createConfigWithStatus creates a RedkeyConfig for the test cluster and sets its status.
func createConfigWithStatus(
	name string, spec redisv1.RedkeyConfigSpec, status redisv1.RedkeyConfigStatus,
) *redisv1.RedkeyConfig {
	cfg := &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{clusterLabel: clusterName},
		},
		Spec: spec,
	}
	Expect(k8sClient.Create(ctx, cfg)).To(Succeed())

	status.Nodes = map[string]*redisv1.RedisNode{}
	cfg.Status = status
	Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
	return cfg
}

// createAppliedConfig creates a config in Applied phase with Ready status,
// simulating a fully applied configuration for an existing cluster.
func createAppliedConfig(name string, primaries, replicas int32) {
	createConfigWithStatus(name, baselineConfigSpec(1, primaries, replicas), redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhaseApplied,
		Status:      redisv1.ClusterStatusReady,
	})
}

// configChange describes an Applied baseline config followed by a Pending config
// and the phase/status Robin is expected to set on the latter.
type configChange struct {
	prev      func(*redisv1.RedkeyConfigSpec)
	next      func(*redisv1.RedkeyConfigSpec)
	configMap bool
	phase     string
	status    string
}

// runConfigChange creates both configs, starts Robin and waits for the expected
// phase/status on the pending config, which is returned.
func runConfigChange(prefix string, c configChange) *redisv1.RedkeyConfig {
	if c.configMap {
		createConfigMap(clusterName)
	}

	prevSpec := baselineConfigSpec(1, 3, 0)
	if c.prev != nil {
		c.prev(&prevSpec)
	}
	createConfigWithStatus(prefix+"-1", prevSpec, redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhaseApplied,
		Status:      redisv1.ClusterStatusReady,
	})

	nextSpec := baselineConfigSpec(2, 3, 0)
	if c.next != nil {
		c.next(&nextSpec)
	}
	cfg := createConfigWithStatus(prefix+"-2", nextSpec, redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhasePending,
	})

	rec := newIntegrationReconciler(newTestRuntimeConfig())
	loopCancel, errCh := startReconcilerLoop(rec)
	DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

	Eventually(func(g Gomega) {
		var fetched redisv1.RedkeyConfig
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
		g.Expect(fetched.Status.ConfigPhase).To(Equal(c.phase))
		g.Expect(fetched.Status.Status).To(Equal(c.status))
	}, timeout, interval).Should(Succeed())
	return cfg
}

var _ = Describe("Config Changes Detection (integration)", func() {
	AfterEach(func() {
		cleanup()
	})

	Describe("Change detection and status transitions", func() {
		DescribeTable("sets the expected phase and status on the pending config",
			func(prefix string, c configChange) {
				runConfigChange(prefix, c)
			},
			Entry("marks config as Applied immediately when only Robin config changes", "changes-robin", configChange{
				next: func(s *redisv1.RedkeyConfigSpec) {
					s.RobinConfig.Reconciler.IntervalSeconds = new(60)
					s.RobinConfig.Metrics.CollectionIntervalSeconds = new(120)
				},
				phase:  redisv1.ConfigPhaseApplied,
				status: redisv1.ClusterStatusReady,
			}),
			Entry("transitions to ScalingUp when primaries increase", "changes-scaleup", configChange{
				next:   func(s *redisv1.RedkeyConfigSpec) { s.Primaries = 6 },
				phase:  redisv1.ConfigPhaseInProgress,
				status: redisv1.ClusterStatusScalingUp,
			}),
			Entry("transitions to ScalingDown when primaries decrease", "changes-scaledown", configChange{
				prev:   func(s *redisv1.RedkeyConfigSpec) { s.Primaries = 6 },
				phase:  redisv1.ConfigPhaseInProgress,
				status: redisv1.ClusterStatusScalingDown,
			}),
			Entry("transitions to ScalingUp when replicas increase (primaries same)", "changes-replicas-up", configChange{
				next:   func(s *redisv1.RedkeyConfigSpec) { s.ReplicasPerPrimary = 2 },
				phase:  redisv1.ConfigPhaseInProgress,
				status: redisv1.ClusterStatusScalingUp,
			}),
			Entry("transitions to Upgrading when only Kubernetes/Redis config changes", "changes-upgrade", configChange{
				next: func(s *redisv1.RedkeyConfigSpec) {
					s.Image = "redis:9-bookworm"
					s.Version = "9.0"
					s.RedisConfig = "maxmemory 200mb"
				},
				phase:  redisv1.ConfigPhaseInProgress,
				status: redisv1.ClusterStatusUpgrading,
			}),
			Entry("no-change config (only control fields differ) is marked as Applied", "changes-noop", configChange{
				next:   func(s *redisv1.RedkeyConfigSpec) { s.SkipIfSuperseded = true },
				phase:  redisv1.ConfigPhaseApplied,
				status: redisv1.ClusterStatusReady,
			}),
			Entry("auth-only change is marked Applied without cluster operation", "changes-auth", configChange{
				next:      func(s *redisv1.RedkeyConfigSpec) { s.Auth = redisv1.RedisAuth{SecretName: "my-new-secret"} },
				configMap: true,
				phase:     redisv1.ConfigPhaseApplied,
				status:    redisv1.ClusterStatusReady,
			}),
			Entry("auth removal-only is marked Applied without cluster operation", "changes-auth-remove", configChange{
				prev:      func(s *redisv1.RedkeyConfigSpec) { s.Auth = redisv1.RedisAuth{SecretName: "existing-secret"} },
				configMap: true,
				phase:     redisv1.ConfigPhaseApplied,
				status:    redisv1.ClusterStatusReady,
			}),
			Entry("auth + image change still transitions to Upgrading", "changes-authimg", configChange{
				next: func(s *redisv1.RedkeyConfigSpec) {
					s.Image = "redis:9-bookworm"
					s.Auth = redisv1.RedisAuth{SecretName: "new-secret"}
				},
				configMap: true,
				phase:     redisv1.ConfigPhaseInProgress,
				status:    redisv1.ClusterStatusUpgrading,
			}),
		)

		It("scaling takes priority over Kubernetes/Redis changes", func() {
			cfg2 := runConfigChange("changes-combined", configChange{
				next: func(s *redisv1.RedkeyConfigSpec) {
					s.Primaries = 6
					s.Image = "redis:9-bookworm"
					s.Version = "9.0"
					s.RedisConfig = "maxmemory 200mb"
					s.RobinConfig.Reconciler.IntervalSeconds = new(60)
				},
				phase:  redisv1.ConfigPhaseInProgress,
				status: redisv1.ClusterStatusScalingUp,
			})

			// Status should remain ScalingUp (operation not implemented yet)
			Consistently(func(g Gomega) {
				var fetched redisv1.RedkeyConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusScalingUp))
			}, 200*time.Millisecond, 50*time.Millisecond).Should(Succeed())
		})
	})
})
