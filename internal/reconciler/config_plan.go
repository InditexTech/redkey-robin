// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"sort"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// selectConfig lists all RedkeyConfig CRs for this cluster, initialises
// any that lack a status phase, applies superseding logic, and returns the
// configuration that should be applied next (targetConfig) together with the
// most recent previously-applied configuration (previousConfig), if one exists.
// previousConfig is the config immediately before targetConfig in sequence order
// and can be used later to detect what changed between the two.
func (r *Reconciler) selectConfig(ctx context.Context) (previousConfig *redisv1.RedkeyConfig, targetConfig *redisv1.RedkeyConfig, err error) {
	// Get the list of configs for this cluster, sorted by sequence ascending.
	configs, err := r.listConfigs(ctx, true)
	if err != nil {
		r.logger.Error("Failed to list RedkeyConfigs", "error", err)
		return nil, nil, err
	}

	if len(configs) == 0 {
		r.logger.Info("No RedkeyConfig resources found")
		return nil, nil, nil
	}

	// Log current state
	for _, cfg := range configs {
		r.logger.Info("Found RedkeyConfig",
			"name", cfg.Name,
			"sequence", cfg.Spec.Sequence,
			"configPhase", cfg.Status.ConfigPhase,
			"status", cfg.Status.Status,
		)
	}

	// Initialise status for configs that were just created by the Operator and
	// therefore have no status set yet (ConfigPhase is empty). Robin owns the
	// status subresource, so it is responsible for writing the initial phase.
	for i := range configs {
		if configs[i].Status.ConfigPhase == "" {
			if err := r.initConfigPhase(ctx, &configs[i]); err != nil {
				r.logger.Warn("Failed to initialise ConfigPhase, we'll retry on next cycle",
					"name", configs[i].Name, "error", err)
				// Continue with the rest; we'll retry on the next cycle.
			}
		}
	}

	// Select the next configuration to process.
	previous, selected := SelectConfig(configs)
	if selected == nil {
		r.logger.Info("No actionable RedkeyConfig found")
		return nil, nil, nil
	}

	// Attempt superseding to skip intermediate configs.
	target := selected
	supersedingResult := ApplySuperseding(configs, selected)
	if len(supersedingResult.Superseded) > 0 {
		target = r.applySupersedingStatus(ctx, supersedingResult)
	}

	return previous, target, nil
}

// listConfigs lists all RedkeyConfig CRs for this cluster. If sorted is true, the returned slice is sorted by sequence ascending.
func (r *Reconciler) listConfigs(ctx context.Context, sorted bool) ([]redisv1.RedkeyConfig, error) {
	var configList redisv1.RedkeyConfigList
	if err := r.client.List(ctx, &configList,
		client.InNamespace(r.namespace),
		client.MatchingLabels{ClusterLabel: r.clusterName},
	); err != nil {
		return nil, err
	}
	if sorted {
		// Sort by sequence ascending
		sort.Slice(configList.Items, func(i, j int) bool {
			return configList.Items[i].Spec.Sequence < configList.Items[j].Spec.Sequence
		})
	}
	return configList.Items, nil
}

// initConfigPhase writes the initial status for a config that was just created
// by the Operator and therefore has no status yet. Robin owns the status
// subresource, so it must set ConfigPhase to Pending on first contact.
// All required (non-omitempty) fields are initialised to their zero values so
// that the API server accepts the update.
func (r *Reconciler) initConfigPhase(ctx context.Context, cfg *redisv1.RedkeyConfig) error {
	r.logger.Info("Initialising ConfigPhase to Pending", "name", cfg.Name, "sequence", cfg.Spec.Sequence)
	cfg.Status = redisv1.RedkeyConfigStatus{
		ConfigPhase: redisv1.ConfigPhasePending,
		Status:      "",
		Substatus:   redisv1.RedkeySubstatus{},
		Nodes:       map[string]*redisv1.RedisNode{},
	}
	return r.client.Status().Update(ctx, cfg)
}

// applySupersedingStatus writes ConfigPhase=Superseded for each config in
// supersedingResult.Superseded, in order. If a write fails, it stops and
// returns the config that failed (which is still Pending and becomes the
// active config). On full success it returns supersedingResult.Selected.
func (r *Reconciler) applySupersedingStatus(ctx context.Context, sr SupersedingResult) *redisv1.RedkeyConfig {
	for _, cfg := range sr.Superseded {
		r.logger.Info("Superseding configuration",
			"name", cfg.Name, "sequence", cfg.Spec.Sequence)
		cfg.Status.ConfigPhase = redisv1.ConfigPhaseSuperseded
		if err := r.client.Status().Update(ctx, cfg); err != nil {
			r.logger.Error("Failed to mark config as Superseded, falling back",
				"name", cfg.Name, "error", err)
			// This config could not be superseded — use it as the active config.
			return cfg
		}
	}
	return sr.Selected
}

// setConfigPhaseInProgress transitions a config to InProgress.
func (r *Reconciler) setConfigPhaseInProgress(ctx context.Context, cfg *redisv1.RedkeyConfig) error {
	cfg.Status.ConfigPhase = redisv1.ConfigPhaseInProgress
	return r.client.Status().Update(ctx, cfg)
}

// SelectConfig chooses the next RedkeyConfig to process from a list
// that must already be sorted by Spec.Sequence ascending.
//
// It returns two values:
//   - previousConfig: the last Applied config before the selected one in
//     sequence order (skipping Superseded configs), or nil if none exists.
//   - targetConfig: the config to apply next.
//
// Selection rules:
//   - If the list is empty, returns (nil, nil).
//   - Skips configs whose ConfigPhase is Applied or Superseded.
//   - Returns the first config whose ConfigPhase is Pending, InProgress, or
//     empty (not yet initialised).
//   - If all configs are Applied (or Superseded), returns the last Applied one
//     (highest sequence).
func SelectConfig(configs []redisv1.RedkeyConfig) (previousConfig *redisv1.RedkeyConfig, targetConfig *redisv1.RedkeyConfig) {
	if len(configs) == 0 {
		return nil, nil
	}

	for i := range configs {
		phase := configs[i].Status.ConfigPhase
		if phase != redisv1.ConfigPhaseApplied && phase != redisv1.ConfigPhaseSuperseded {
			return lastAppliedBefore(configs, i), &configs[i]
		}
	}

	// All configs are Applied/Superseded — return the last Applied one.
	last := lastApplied(configs)
	if last == nil {
		// Edge case: all are Superseded with none Applied — return the last one.
		return nil, &configs[len(configs)-1]
	}
	prev := lastAppliedBefore(configs, indexOfConfig(configs, last))
	return prev, last
}

// lastAppliedBefore returns the last config with ConfigPhase==Applied that
// appears before index i, or nil if none exists.
func lastAppliedBefore(configs []redisv1.RedkeyConfig, i int) *redisv1.RedkeyConfig {
	for j := i - 1; j >= 0; j-- {
		if configs[j].Status.ConfigPhase == redisv1.ConfigPhaseApplied {
			return &configs[j]
		}
	}
	return nil
}

// lastApplied returns the last config with ConfigPhase==Applied, or nil.
func lastApplied(configs []redisv1.RedkeyConfig) *redisv1.RedkeyConfig {
	for i := len(configs) - 1; i >= 0; i-- {
		if configs[i].Status.ConfigPhase == redisv1.ConfigPhaseApplied {
			return &configs[i]
		}
	}
	return nil
}

// validConfigPhase returns true if phase is one of the known ConfigPhase values.
// excludeSuperseded controls whether ConfigPhaseSuperseded is considered valid or not, depending on the context of the check.
func validConfigPhase(phase string, excludeSuperseded bool) bool {
	if excludeSuperseded {
		return phase == redisv1.ConfigPhasePending ||
			phase == redisv1.ConfigPhaseInProgress ||
			phase == redisv1.ConfigPhaseApplied
	}
	return phase == redisv1.ConfigPhasePending ||
		phase == redisv1.ConfigPhaseInProgress ||
		phase == redisv1.ConfigPhaseApplied ||
		phase == redisv1.ConfigPhaseSuperseded
}
